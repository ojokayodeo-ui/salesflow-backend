"""
PALM AI Agent — intelligent marketing specialist.
Knows your CRM pipeline, swipe file knowledge base, and stage-by-stage playbook.
Advises on next steps, call preparation, and winning each deal.
"""

import asyncio
import logging
import httpx
from fastapi import APIRouter, HTTPException
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL   = "claude-sonnet-4-6"

STAGE_PLAYBOOK = {
    "new":            "Fresh lead. Goal: qualify fast, understand their situation, book a discovery call within 48h.",
    "pending_review": "Awaiting internal review. Verify ICP fit, research the company, prepare personalised outreach.",
    "sequence":       "In email sequence. Monitor opens/replies, warm with value (case study, insight, quick win). Aim for reply.",
    "seq_enrolled":   "Enrolled in sequence. Ensure emails are going out. Watch for any reply signal.",
    "delivered":      "Leads delivered. Follow up to confirm receipt, ask for feedback on quality, keep relationship warm.",
    "won":            "Deal closed. Focus on onboarding, success metrics, referral potential, and upsell opportunities.",
    "lost":           "Cold/lost. Re-engage after 30+ days with a new angle or proof point. Don't chase — offer value.",
}


async def _build_crm_context() -> str:
    try:
        all_deals = await db.list_deals()
    except Exception:
        return "CRM data unavailable."

    stage_counts: dict[str, int] = {}
    for d in all_deals:
        s = d.get("stage", "unknown")
        stage_counts[s] = stage_counts.get(s, 0) + 1

    recent = sorted(all_deals, key=lambda x: x.get("created_at", ""), reverse=True)[:20]

    deal_lines = []
    for d in recent:
        parts = [f"• {d.get('name','?')} @ {d.get('company','?')} | stage={d.get('stage','?')}"]
        if d.get("job_title"):   parts.append(f"title={d['job_title']}")
        if d.get("industry"):    parts.append(f"industry={d['industry']}")
        if d.get("sentiment"):   parts.append(f"sentiment={d['sentiment']}")
        if d.get("reply_body"):
            preview = d["reply_body"][:120].replace("\n", " ")
            parts.append(f'reply="{preview}"')
        deal_lines.append(" | ".join(parts))

    pipeline_summary = ", ".join(f"{k}: {v}" for k, v in sorted(stage_counts.items()))
    return (
        f"LIVE PIPELINE — {len(all_deals)} total deals | {pipeline_summary}\n\n"
        f"RECENT DEALS:\n" + ("\n".join(deal_lines) if deal_lines else "None yet.")
    )


async def _build_swipe_context(limit: int = 8) -> str:
    try:
        files = await db.list_swipe_files()
    except Exception:
        return ""
    if not files:
        return ""
    selected = files[:limit]
    parts = []
    for f in selected:
        snippet = (f.get("content") or "")[:600]
        parts.append(f"[{f['category'].upper()}] {f['title']}\n{snippet}")
    return "KNOWLEDGE BASE (swipe files you've trained me on):\n\n" + "\n\n---\n\n".join(parts)


async def _build_mail_context(email: str, deal_id: str = "") -> str:
    """
    Fetch email history for a prospect:
    - Tracked send/open metrics from our DB
    - Sent + received messages from Outlook via Graph API
    Returns a formatted string ready to inject into the agent prompt.
    """
    parts: list[str] = []

    # ── Tracked email metrics from our DB ────────────────────────────────────
    if deal_id:
        try:
            metrics = await db.get_email_metrics(deal_id)
            sent_c   = metrics.get("sent_count", 0)
            recv_c   = metrics.get("received_count", 0)
            opens    = metrics.get("total_opens", 0)
            if sent_c or recv_c:
                parts.append(
                    f"TRACKED EMAIL STATS: {sent_c} emails sent · "
                    f"{opens} total opens · {recv_c} replies received"
                )
                events = (metrics.get("events") or [])[:6]
                for e in events:
                    direction = e.get("direction", "?")
                    subj      = e.get("subject") or "(no subject)"
                    date      = str(e.get("sent_at") or "")[:10]
                    oc        = e.get("open_count") or 0
                    if direction == "sent":
                        parts.append(f"  [{date}] SENT: {subj}" +
                                     (f" — opened {oc}×" if oc else " — not opened yet"))
                    else:
                        parts.append(f"  [{date}] RECEIVED: {subj}")
        except Exception as exc:
            logger.warning("Could not load email metrics for deal %s: %s", deal_id, exc)

    # ── Outlook inbox + sentItems thread ─────────────────────────────────────
    if email and settings.ms_sender_email:
        try:
            from app.services.outlook import get_access_token, GRAPH_BASE
            token   = await get_access_token()
            headers = {"Authorization": f"Bearer {token}"}

            async with httpx.AsyncClient(timeout=20) as client:
                inbox_resp, sent_resp = await asyncio.gather(
                    client.get(
                        f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages",
                        headers=headers,
                        params={
                            "$search": f'"from:{email}"',
                            "$select": "subject,receivedDateTime,bodyPreview",
                            "$top": 8,
                        },
                    ),
                    client.get(
                        f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sentItems",
                        headers=headers,
                        params={
                            "$filter": f"toRecipients/any(r:r/emailAddress/address eq '{email}')",
                            "$select": "subject,sentDateTime,bodyPreview",
                            "$top": 8,
                        },
                    ),
                )

            received = inbox_resp.json().get("value", []) if inbox_resp.status_code == 200 else []
            sent     = sent_resp.json().get("value",  []) if sent_resp.status_code  == 200 else []

            if sent or received:
                thread = []
                for m in sent:
                    thread.append({
                        "dir":     "SENT",
                        "date":    (m.get("sentDateTime")     or "")[:10],
                        "subject": m.get("subject")           or "(no subject)",
                        "preview": (m.get("bodyPreview")      or "")[:250],
                    })
                for m in received:
                    thread.append({
                        "dir":     "RECEIVED",
                        "date":    (m.get("receivedDateTime") or "")[:10],
                        "subject": m.get("subject")           or "(no subject)",
                        "preview": (m.get("bodyPreview")      or "")[:250],
                    })
                thread.sort(key=lambda x: x["date"], reverse=True)

                parts.append("")
                parts.append("OUTLOOK EMAIL THREAD (most recent first):")
                for t in thread[:10]:
                    parts.append(f"  [{t['dir']}] {t['date']} | {t['subject']}")
                    if t["preview"]:
                        parts.append(f"    \"{t['preview']}\"")
        except Exception as exc:
            logger.warning("Could not fetch Outlook thread for %s: %s", email, exc)

    if not parts:
        return ""
    return "EMAIL HISTORY WITH THIS PROSPECT:\n" + "\n".join(parts)


def _build_system_prompt(crm_context: str, swipe_context: str, mail_context: str = "") -> str:
    playbook_text = "\n".join(f"  {stage}: {tip}" for stage, tip in STAGE_PLAYBOOK.items())
    mail_section  = f"\n\n{mail_context}" if mail_context else ""
    return f"""You are PALM AI — an elite B2B marketing specialist and revenue advisor embedded in the Pipeline Activation Lead Magnet (PALM) app.

YOUR EXPERTISE:
- Cold outreach strategy and email copywriting
- ICP (Ideal Customer Profile) targeting and segmentation
- Warm lead nurturing and conversion tactics — booking calls at every stage
- Sequence strategy and follow-up cadences
- Pipeline health analysis and deal prioritisation
- B2B sales psychology and objection handling
- Call preparation: what to say, what to ask, how to handle objections

STAGE-BY-STAGE PLAYBOOK (your default action map):
{playbook_text}

{crm_context}

{swipe_context}{mail_section}

BEHAVIOUR:
- Be direct, specific and actionable — no fluff
- Reference actual deals and leads by name when relevant
- When asked to draft emails or scripts, write complete ready-to-use copy
- For "what next" questions: give ONE clear priority action, then 2-3 supporting steps
- For call prep: give an opening line, 3 key questions to ask, and how to propose booking
- Think like a high-performance sales specialist whose only goal is booked calls and closed deals
- Use swipe file knowledge to inform your copy and strategy recommendations
- Reference actual email subjects and prospect replies when they are available in context"""


@router.post("/chat")
async def agent_chat(body: dict):
    """
    Multi-turn marketing agent chat.
    Body: { messages: [{role, content}], deal_id?: str }
    When deal_id is provided the agent has full email thread context for that deal.
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    messages = body.get("messages", [])
    if not messages:
        raise HTTPException(status_code=422, detail="messages array required")

    deal_id = body.get("deal_id", "")

    # Build context (run in parallel where possible)
    crm_task   = asyncio.create_task(_build_crm_context())
    swipe_task = asyncio.create_task(_build_swipe_context())

    mail_context = ""
    if deal_id:
        deal = await db.get_deal(deal_id)
        if deal:
            mail_context = await _build_mail_context(deal.get("email", ""), deal_id)

    crm_context, swipe_context = await crm_task, await swipe_task
    system_prompt = _build_system_prompt(crm_context, swipe_context, mail_context)

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
        logger.exception("Agent chat failed")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/deal-advice")
async def deal_advice(body: dict):
    """
    Get specific next-step advice for a single deal, including full email history.
    Body: { deal_id: str, question?: str }
    Returns: { advice: str }
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    deal_id  = body.get("deal_id", "")
    question = body.get("question", "What should I do next to move this deal forward and book a call?")

    if not deal_id:
        raise HTTPException(status_code=422, detail="deal_id required")

    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    stage        = deal.get("stage", "new")
    playbook_tip = STAGE_PLAYBOOK.get(stage, "Focus on booking a call.")

    deal_ctx = f"""DEAL:
Name: {deal.get('name', 'Unknown')}
Company: {deal.get('company', '')}
Email: {deal.get('email', '')}
Job title: {deal.get('job_title', '')}
Industry: {deal.get('industry', '')}
Location: {deal.get('location', '')}
Company size: {deal.get('headcount', '')}
Current stage: {stage}
Stage goal: {playbook_tip}
Sentiment: {deal.get('sentiment', 'unknown')}
Last reply: {(deal.get('reply_body') or '')[:300]}
Company description: {(deal.get('company_desc') or '')[:200]}"""

    # Fetch email history in parallel with swipe context
    mail_task  = asyncio.create_task(_build_mail_context(deal.get("email", ""), deal_id))
    swipe_task = asyncio.create_task(_build_swipe_context(limit=5))
    mail_context, swipe_context = await mail_task, await swipe_task

    if mail_context:
        deal_ctx += f"\n\n{mail_context}"

    system = f"""You are PALM AI — a B2B marketing specialist focused on booking calls and closing deals.
{swipe_context}
Give specific, actionable advice. Be direct. Lead with the single most important action.
Reference specific email subjects or prospect replies from the email history where relevant."""

    user_msg = f"{deal_ctx}\n\nQUESTION: {question}"

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
                    "system":     system,
                    "messages":   [{"role": "user", "content": user_msg}],
                },
            )
            resp.raise_for_status()
        advice = resp.json()["content"][0]["text"]
        logger.info("Deal advice for %s (%s)", deal.get("name"), stage)
        return {"advice": advice, "deal_name": deal.get("name"), "stage": stage}

    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Claude API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Deal advice failed")
        raise HTTPException(status_code=500, detail=str(exc))
