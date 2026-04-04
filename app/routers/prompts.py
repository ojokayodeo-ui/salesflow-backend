"""
Prompt Management Router
Handles all CRUD operations, search, versioning, AI enhancement, and export for prompts.
"""

import csv
import io
import json
import logging
import os
from typing import Optional

import anthropic
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.services import prompt_db

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Pydantic Schemas ─────────────────────────────────────────────────────────

class PromptCreate(BaseModel):
    title: str
    description: Optional[str] = None
    content: str
    tags: list[str] = []
    category: Optional[str] = None
    is_favorite: bool = False
    variables: list[dict] = []


class PromptUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None
    tags: Optional[list[str]] = None
    category: Optional[str] = None
    is_favorite: Optional[bool] = None
    variables: Optional[list[dict]] = None
    change_note: Optional[str] = None


class CategoryCreate(BaseModel):
    name: str
    color: str = "#6366f1"
    icon: Optional[str] = None


class AIEnhanceRequest(BaseModel):
    content: str
    mode: str = "improve"  # improve | rewrite | structure | generate
    context: Optional[str] = None


# ── Prompts CRUD ─────────────────────────────────────────────────────────────

@router.get("")
async def list_prompts(
    search: Optional[str] = Query(None),
    tags: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    sort: str = Query("recent", enum=["recent", "most_used", "az", "favorites"]),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    prompts = await prompt_db.list_prompts(
        search=search,
        tags=tag_list,
        category=category,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    total = await prompt_db.count_prompts(search=search, tags=tag_list, category=category)
    return {"prompts": prompts, "total": total, "limit": limit, "offset": offset}


@router.post("", status_code=201)
async def create_prompt(body: PromptCreate):
    prompt = await prompt_db.create_prompt(
        title=body.title,
        description=body.description,
        content=body.content,
        tags=body.tags,
        category=body.category,
        is_favorite=body.is_favorite,
        variables=body.variables,
    )
    return prompt


@router.get("/export")
async def export_prompts(
    fmt: str = Query("json", enum=["json", "csv"]),
    category: Optional[str] = Query(None),
    tags: Optional[str] = Query(None),
):
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    prompts = await prompt_db.list_prompts(
        search=None, tags=tag_list, category=category, sort="recent", limit=10000, offset=0
    )

    if fmt == "json":
        content = json.dumps(prompts, indent=2, default=str)
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=prompts.json"},
        )

    # CSV export
    if not prompts:
        raise HTTPException(status_code=404, detail="No prompts to export")
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["id", "title", "description", "content", "tags", "category",
                    "usage_count", "is_favorite", "created_at"],
    )
    writer.writeheader()
    for p in prompts:
        writer.writerow({
            "id": p["id"],
            "title": p["title"],
            "description": p.get("description", ""),
            "content": p["content"],
            "tags": "|".join(p.get("tags", [])),
            "category": p.get("category", ""),
            "usage_count": p.get("usage_count", 0),
            "is_favorite": p.get("is_favorite", False),
            "created_at": p.get("created_at", ""),
        })
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=prompts.csv"},
    )


@router.get("/categories")
async def list_categories():
    cats = await prompt_db.list_categories()
    return {"categories": cats}


@router.post("/categories", status_code=201)
async def create_category(body: CategoryCreate):
    cat = await prompt_db.create_category(name=body.name, color=body.color, icon=body.icon)
    return cat


@router.delete("/categories/{category_id}")
async def delete_category(category_id: str):
    await prompt_db.delete_category(category_id)
    return {"deleted": True}


@router.post("/ai/enhance")
async def ai_enhance(body: AIEnhanceRequest):
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    mode_instructions = {
        "improve": (
            "You are a prompt engineering expert. Improve the following prompt to be clearer, "
            "more specific, and more effective. Keep the same intent and tone but enhance clarity, "
            "add missing context, and make variables/placeholders explicit using {{variable_name}} syntax."
        ),
        "rewrite": (
            "You are a prompt engineering expert. Completely rewrite the following prompt to be "
            "professional, structured, and highly effective. Use best practices: clear role, "
            "specific task, context, constraints, and output format. Use {{variable_name}} for customizable parts."
        ),
        "structure": (
            "You are a prompt engineering expert. Restructure the following prompt into a "
            "well-organized format with clear sections: Role, Task, Context, Constraints, Output Format. "
            "Do not change the core intent — only improve the structure. Use {{variable_name}} for placeholders."
        ),
        "generate": (
            "You are a prompt engineering expert. Generate a complete, professional prompt based on "
            "the following brief description. The prompt should be ready to use and include "
            "{{variable_name}} placeholders for customizable parts."
        ),
    }

    system = mode_instructions.get(body.mode, mode_instructions["improve"])
    user_msg = body.content
    if body.context:
        user_msg = f"Context: {body.context}\n\nPrompt:\n{body.content}"

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    enhanced = message.content[0].text

    # Extract variables from the enhanced prompt
    import re
    variables = list(set(re.findall(r'\{\{(\w+)\}\}', enhanced)))

    return {
        "original": body.content,
        "enhanced": enhanced,
        "variables": variables,
        "mode": body.mode,
        "tokens_used": message.usage.input_tokens + message.usage.output_tokens,
    }


@router.get("/{prompt_id}")
async def get_prompt(prompt_id: str):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return prompt


@router.put("/{prompt_id}")
async def update_prompt(prompt_id: str, body: PromptUpdate):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    updated = await prompt_db.update_prompt(
        prompt_id=prompt_id,
        title=body.title,
        description=body.description,
        content=body.content,
        tags=body.tags,
        category=body.category,
        is_favorite=body.is_favorite,
        variables=body.variables,
        change_note=body.change_note,
        old_content=prompt["content"],
    )
    return updated


@router.delete("/{prompt_id}")
async def delete_prompt(prompt_id: str):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    await prompt_db.delete_prompt(prompt_id)
    return {"deleted": True}


@router.post("/{prompt_id}/use")
async def track_usage(prompt_id: str):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    await prompt_db.increment_usage(prompt_id)
    return {"usage_count": prompt["usage_count"] + 1}


@router.post("/{prompt_id}/favorite")
async def toggle_favorite(prompt_id: str):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    new_state = not prompt["is_favorite"]
    await prompt_db.set_favorite(prompt_id, new_state)
    return {"is_favorite": new_state}


@router.get("/{prompt_id}/versions")
async def get_versions(prompt_id: str):
    prompt = await prompt_db.get_prompt(prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")
    versions = await prompt_db.get_versions(prompt_id)
    return {"versions": versions}
