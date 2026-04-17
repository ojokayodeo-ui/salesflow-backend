"""
Nurture feature — warm lead management, swipe file library, AI email composer.
"""

import logging
import httpx
from fastapi import APIRouter, HTTPException
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL   = "claude-sonnet-4-20250514"


# ── Swipe Files ──────────────────────────────────────────────────────────────

@router.get("/swipe-files")
async def list_swipe_files():
    files = await db.list_swipe_files()
    return {"swipe_files": files}


@router.post("/swipe-files")
async def create_swipe_file(body: dict):
    required = ("title", "category", "content")
    for field in required:
        if not body.get(field):
            raise HTTPException(status_code=422, detail=f"Missing field: {field}")
    file = await db.create_swipe_file(
        title    = body["title"].strip(),
        category = body["category"].strip(),
        content  = body["content"].strip(),
        source   = body.get("source", "").strip(),
        tags     = body.get("tags", []),
    )
    return {"swipe_file": file}


@router.patch("/swipe-files/{file_id}")
async def update_swipe_file(file_id: str, body: dict):
    existing = await db.get_swipe_file(file_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Swipe file not found")
    file = await db.update_swipe_file(
        file_id  = file_id,
        title    = body.get("title", existing["title"]).strip(),
        category = body.get("category", existing["category"]).strip(),
        content  = body.get("content", existing["content"]).strip(),
        source   = body.get("source", existing.get("source", "")).strip(),
        tags     = body.get("tags", existing.get("tags", [])),
    )
    return {"swipe_file": file}


@router.delete("/swipe-files/{file_id}")
async def delete_swipe_file(file_id: str):
    existing = await db.get_swipe_file(file_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Swipe file not found")
    await db.delete_swipe_file(file_id)
    return {"deleted": True, "id": file_id}


# ── AI Email Composer ─────────────────────────────────────────────────────────

@router.post("/compose")
async def compose_nurture_email(body: dict):
    """
    Generate a nurture email using Claude.
    Body: { deal_id, tone, angle, focus, swipe_file_ids, custom_context }
    Returns: { subject, body }
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    deal_id        = body.get("deal_id", "")
    tone           = body.get("tone", "professional")
    angle          = body.get("angle", "value")
    focus          = body.get("focus", "")
    swipe_file_ids = body.get("swipe_file_ids", [])
    custom_context = body.get("custom_context", "").strip()

    # Load lead / deal context
    lead_context = ""
    if deal_id:
        deal = await db.get_deal(deal_id)
        if deal:
            parts = [
                f"Prospect name: {deal.get('name', 'Unknown')}",
                f"Company: {deal.get('company', '')}",
                f"Job title: {deal.get('job_title', '')}",
                f"Industry: {deal.get('industry', '')}",
                f"Location: {deal.get('location', '')}",
                f"Company size: {deal.get('headcount', '')}",
            ]
            if deal.get("reply_body"):
                parts.append(f"Their original reply: \"{deal['reply_body'][:400]}\"")
            if deal.get("company_desc"):
                parts.append(f"Company description: {deal['company_desc'][:300]}")
            lead_context = "\n".join(p for p in parts if not p.endswith(": "))

    # Load selected swipe files for creative inspiration
    swipe_content = ""
    if swipe_file_ids:
        files = []
        for fid in swipe_file_ids[:5]:
            sf = await db.get_swipe_file(fid)
            if sf:
                files.append(f"--- {sf['title']} ({sf['category']}) ---\n{sf['content'][:800]}")
        if files:
            swipe_content = "\n\n".join(files)

    # Build the prompt
    prompt_parts = [
        "You are an expert B2B sales copywriter specialising in warm lead nurture emails.",
        "Write a short, personalised nurture email that feels human and avoids corporate clichés.",
        "",
        f"Tone: {tone}",
        f"Angle / hook: {angle}",
    ]
    if focus:
        prompt_parts.append(f"Focus / theme: {focus}")
    if lead_context:
        prompt_parts += ["", "LEAD CONTEXT:", lead_context]
    if custom_context:
        prompt_parts += ["", "ADDITIONAL CONTEXT:", custom_context]
    if swipe_content:
        prompt_parts += [
            "",
            "SWIPE FILE INSPIRATION (use these as creative reference, not copy-paste):",
            swipe_content,
        ]
    prompt_parts += [
        "",
        "Instructions:",
        "- Subject line: punchy, curiosity-driven, max 60 chars",
        "- Body: 3-5 short paragraphs, conversational, ends with a soft CTA",
        "- Do NOT use salesy buzzwords (synergy, leverage, game-changer, etc.)",
        "- Personalise using the lead context where relevant",
        "",
        'Return ONLY valid JSON in this exact format (no markdown, no extra text):',
        '{"subject": "...", "body": "..."}',
        "Use \\n for newlines inside the body string.",
    ]

    prompt = "\n".join(prompt_parts)

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                CLAUDE_API_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      CLAUDE_MODEL,
                    "max_tokens": 1024,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw_text = data["content"][0]["text"].strip()

        import json as _json
        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        result = _json.loads(raw_text)
        subject = result.get("subject", "")
        body    = result.get("body", "")

        if not subject or not body:
            raise ValueError("Claude returned empty subject or body")

        logger.info("Nurture email composed for deal %s", deal_id or "unknown")
        return {"subject": subject, "body": body}

    except httpx.HTTPStatusError as exc:
        logger.error("Claude API error %s: %s", exc.response.status_code, exc.response.text[:300])
        raise HTTPException(status_code=502, detail=f"Claude API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Nurture compose failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
