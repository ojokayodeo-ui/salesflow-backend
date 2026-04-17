"""
PALM AI Agent — intelligent marketing specialist.
Uses Claude with live CRM context to provide strategic sales advice.
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


async def _build_crm_context() -> str:
    """Load live CRM data and format as context for the agent."""
    try:
        all_deals = await db.list_deals()
    except Exception:
        return "CRM data unavailable."

    stage_counts: dict[str, int] = {}
    for d in all_deals:
        s = d.get("stage", "unknown")
        stage_counts[s] = stage_counts.get(s, 0) + 1

    recent = sorted(all_deals, key=lambda x: x.get("created_at", ""), reverse=True)[:15]

    deal_lines = []
    for d in recent:
        parts = [
            f"• {d.get('name','?')} @ {d.get('company','?')}",
            f"stage={d.get('stage','?')}",
        ]
        if d.get("job_title"):
            parts.append(f"title={d['job_title']}")
        if d.get("industry"):
            parts.append(f"industry={d['industry']}")
        if d.get("sentiment"):
            parts.append(f"sentiment={d['sentiment']}")
        if d.get("reply_body"):
            preview = d["reply_body"][:100].replace("\n", " ")
            parts.append(f'reply="{preview}..."')
        deal_lines.append(" | ".join(parts))

    pipeline_summary = ", ".join(f"{k}: {v}" for k, v in sorted(stage_counts.items()))
    return (
        f"LIVE PIPELINE — {len(all_deals)} total deals | {pipeline_summary}\n\n"
        f"RECENT DEALS:\n" + ("\n".join(deal_lines) if deal_lines else "None yet.")
    )


@router.post("/chat")
async def agent_chat(body: dict):
    """
    Multi-turn marketing agent chat.
    Body: { messages: [{role: "user"|"assistant", content: "..."}] }
    Returns: { response: "..." }
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    messages = body.get("messages", [])
    if not messages:
        raise HTTPException(status_code=422, detail="messages array required")

    crm_context = await _build_crm_context()

    system_prompt = f"""You are PALM AI — an elite B2B marketing specialist and revenue advisor embedded in the Pipeline Activation Lead Magnet (PALM) app.

YOUR EXPERTISE:
- Cold outreach strategy and email copywriting
- ICP (Ideal Customer Profile) targeting and segmentation
- Warm lead nurturing and conversion tactics
- Sequence strategy and follow-up cadences
- Pipeline health analysis and deal prioritisation
- B2B sales psychology and objection handling

LIVE CRM DATA (use this to give specific, personalised advice):
{crm_context}

BEHAVIOUR:
- Be direct, specific and actionable — no fluff, no filler
- Reference actual deals and leads by name when relevant
- When asked to draft emails, write complete, ready-to-send copy
- Suggest concrete next steps with urgency
- Think like a high-performance sales specialist whose goal is booked calls and closed deals
- Keep responses tight unless depth is needed
- Use the CRM data to identify patterns, opportunities and risks the user might have missed"""

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                CLAUDE_API_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      CLAUDE_MODEL,
                    "max_tokens": 2048,
                    "system":     system_prompt,
                    "messages":   messages,
                },
            )
            resp.raise_for_status()
        data = resp.json()
        response_text = data["content"][0]["text"]
        logger.info("Agent responded (%d chars)", len(response_text))
        return {"response": response_text}

    except httpx.HTTPStatusError as exc:
        logger.error("Claude API error %s: %s", exc.response.status_code, exc.response.text[:200])
        raise HTTPException(status_code=502, detail=f"Claude API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Agent chat failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
