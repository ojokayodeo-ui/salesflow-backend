"""
PALM AI Agent — elite B2B marketing strategist.
Combines live CRM data, full email thread analysis, swipe file knowledge,
and deep sales psychology to give razor-sharp, deal-winning advice.
"""

import asyncio
import logging
import re
from html.parser import HTMLParser

import httpx
from fastapi import APIRouter, HTTPException
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL   = "claude-sonnet-4-6"

STAGE_PLAYBOOK = {
    "new":            "Fresh lead. Qualify fast, understand pain, book discovery call within 48h.",
    "pending_review": "Awaiting internal review. Verify ICP fit, research company, prepare personalised outreach.",
    "sequence":       "In email sequence. Monitor opens/replies. Warm with genuine value. Goal: get a reply.",
    "seq_enrolled":   "Enrolled in sequence. Ensure emails are going out. Watch for any engagement signal.",
    "delivered":      "Leads delivered. Follow up to confirm receipt, get feedback, keep relationship warm.",
    "won":            "Deal closed. Onboard smoothly, document success metrics, nurture for referrals and upsell.",
    "lost":           "Cold/lost. Wait 30+ days then re-engage with a completely new angle or compelling proof point.",
}


# ── HTML stripping ─────────────────────────────────────────────────────────────

class _HTMLStripper(HTMLParser):
    SKIP = {"script", "style", "head", "nav", "footer", "noscript"}

    def __init__(self):
        super().__init__()
        self._buf: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in {"p", "div", "br", "tr", "li", "h1", "h2", "h3"}:
            self._buf.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._buf.append(data)

    def get_text(self) -> str:
        raw = "".join(self._buf)
        return re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t]+", " ", raw)).strip()


def _strip_html(html: str) -> str:
    p = _HTMLStripper()
    p.feed(html)
    return p.get_text()


# ── Context builders ───────────────────────────────────────────────────────────

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
        if d.get("job_title"):  parts.append(f"title={d['job_title']}")
        if d.get("industry"):   parts.append(f"industry={d['industry']}")
        if d.get("sentiment"):  parts.append(f"sentiment={d['sentiment']}")
        if d.get("reply_body"):
            parts.append(f'reply="{d["reply_body"][:120].replace(chr(10)," ")}"')
        deal_lines.append(" | ".join(parts))

    summary = ", ".join(f"{k}: {v}" for k, v in sorted(stage_counts.items()))
    return (
        f"LIVE PIPELINE — {len(all_deals)} total deals | {summary}\n\n"
        "RECENT DEALS:\n" + ("\n".join(deal_lines) if deal_lines else "None yet.")
    )


async def _build_swipe_context(limit: int = 12) -> str:
    """Load swipe files as a full tactical arsenal — more content, more files."""
    try:
        files = await db.list_swipe_files()
    except Exception:
        return ""
    if not files:
        return ""
    parts = []
    for f in files[:limit]:
        snippet = (f.get("content") or "")[:1200]
        parts.append(
            f"[{f['category'].upper()}] {f['title']}\n{snippet}"
            + ("…" if len(f.get("content") or "") > 1200 else "")
        )
    return (
        "YOUR TACTICAL ARSENAL — SWIPE FILES (apply these techniques to the live situation):\n\n"
        + "\n\n---\n\n".join(parts)
    )


async def _build_mail_context(email: str, deal_id: str = "") -> str:
    """
    Fetch the FULL email thread with a prospect:
    - Tracked open/send metrics from DB
    - Full body content of sent + received messages from Outlook
    Returns a richly formatted string for deep analysis.
    """
    parts: list[str] = []

    # ── Tracked metrics ───────────────────────────────────────────────────────
    if deal_id:
        try:
            metrics  = await db.get_email_metrics(deal_id)
            sent_c   = metrics.get("sent_count", 0)
            recv_c   = metrics.get("received_count", 0)
            opens    = metrics.get("total_opens", 0)
            if sent_c or recv_c:
                parts.append(
                    f"TRACKED STATS: {sent_c} emails sent · "
                    f"{opens} total opens · {recv_c} inbound replies tracked"
                )
                for e in (metrics.get("events") or [])[:8]:
                    direction = e.get("direction", "?")
                    subj = e.get("subject") or "(no subject)"
                    date = str(e.get("sent_at") or "")[:10]
                    oc   = e.get("open_count") or 0
                    if direction == "sent":
                        parts.append(
                            f"  [SENT {date}] {subj}"
                            + (f" — opened {oc}×" if oc else " — NOT opened")
                        )
                    else:
                        parts.append(f"  [RECEIVED {date}] {subj}")
        except Exception as exc:
            logger.warning("Email metrics unavailable for deal %s: %s", deal_id, exc)

    # ── Full Outlook thread ───────────────────────────────────────────────────
    if email and settings.ms_sender_email:
        try:
            from app.services.outlook import get_access_token, GRAPH_BASE
            token   = await get_access_token()
            headers = {"Authorization": f"Bearer {token}"}

            async with httpx.AsyncClient(timeout=25) as client:
                inbox_resp, sent_resp = await asyncio.gather(
                    client.get(
                        f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages",
                        headers=headers,
                        params={
                            "$search": f'"from:{email}"',
                            "$select": "subject,receivedDateTime,body,from",
                            "$top": 6,
                        },
                    ),
                    client.get(
                        f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sentItems",
                        headers=headers,
                        params={
                            "$filter": f"toRecipients/any(r:r/emailAddress/address eq '{email}')",
                            "$select": "subject,sentDateTime,body",
                            "$top": 6,
                        },
                    ),
                )

            received = inbox_resp.json().get("value", []) if inbox_resp.status_code == 200 else []
            sent     = sent_resp.json().get("value",  []) if sent_resp.status_code  == 200 else []

            if sent or received:
                thread = []
                for m in sent:
                    body_raw = (m.get("body") or {}).get("content", "")
                    body_type = (m.get("body") or {}).get("contentType", "text")
                    body_text = _strip_html(body_raw) if body_type == "html" else body_raw
                    thread.append({
                        "dir":     "YOU SENT",
                        "date":    (m.get("sentDateTime") or "")[:10],
                        "subject": m.get("subject") or "(no subject)",
                        "body":    body_text[:2000],
                    })
                for m in received:
                    body_raw  = (m.get("body") or {}).get("content", "")
                    body_type = (m.get("body") or {}).get("contentType", "text")
                    body_text = _strip_html(body_raw) if body_type == "html" else body_raw
                    thread.append({
                        "dir":     "PROSPECT REPLIED",
                        "date":    (m.get("receivedDateTime") or "")[:10],
                        "subject": m.get("subject") or "(no subject)",
                        "body":    body_text[:2000],
                    })

                thread.sort(key=lambda x: x["date"])  # chronological

                parts.append("")
                parts.append("FULL EMAIL THREAD (chronological — read carefully):")
                parts.append("=" * 60)
                for t in thread:
                    parts.append(f"\n[{t['dir']}] {t['date']} | Subject: {t['subject']}")
                    parts.append("-" * 40)
                    parts.append(t["body"] if t["body"] else "(body unavailable)")
                    parts.append("")
                parts.append("=" * 60)
        except Exception as exc:
            logger.warning("Outlook thread unavailable for %s: %s", email, exc)

    if not parts:
        return ""
    return "EMAIL INTELLIGENCE:\n" + "\n".join(parts)


# ── System prompt ──────────────────────────────────────────────────────────────

def _build_system_prompt(crm_context: str, swipe_context: str, mail_context: str = "") -> str:
    playbook_text = "\n".join(f"  {stage}: {tip}" for stage, tip in STAGE_PLAYBOOK.items())
    mail_section  = f"\n\n{mail_context}" if mail_context else ""

    return f"""You are PALM AI — an elite B2B revenue strategist, sales psychologist, and copywriter embedded in the Pipeline Activation Lead Magnet (PALM) app. You combine deep sales strategy with human psychology to help close deals and book calls.

═══ YOUR EXPERTISE ═══
• Cold outreach, warm nurture, and conversion strategy
• Human psychology and buyer behaviour — you understand what drives decisions below the surface
• Email copywriting that feels human, not salesy
• ICP targeting, pipeline analysis, deal prioritisation
• Call preparation: opening lines, discovery questions, objection handling scripts
• Sequence strategy and follow-up cadences that actually get replies

═══ PSYCHOLOGICAL FRAMEWORKS YOU APPLY ═══
CIALDINI'S PRINCIPLES (use the right one for each situation):
  • Reciprocity — give genuine value first, no strings; creates psychological obligation
  • Commitment & Consistency — get small yeses that lead naturally to bigger ones
  • Social Proof — others like them are already doing it (specifics beat vague claims)
  • Authority — position as the credible expert, not the eager salesperson
  • Liking — find genuine common ground, mirror their language and style
  • Scarcity — limited time, limited slots, real urgency (never fake it)

DEEPER LEVERS:
  • Loss Aversion — people feel losses 2× more than gains; frame what they lose by NOT acting
  • Identity & Status — connect to who they see themselves as, or who they want to become
  • Pattern Interrupt — break the "salesperson alarm" with unexpected, disarming approaches
  • The Columbo Close — soft, curious, non-threatening; let them lower their guard
  • Reciprocity Loop — give value → earn attention → earn trust → earn the meeting
  • Objection Reframing — an objection is a question in disguise; answer the real question
  • Strategic Withdrawal — sometimes pulling back creates more pull than pushing forward
  • Specificity Bias — vague claims trigger scepticism; hyper-specific details build trust
  • Future Pacing — paint a vivid picture of life after the solution; make it real in their mind

═══ WHEN AN EMAIL THREAD IS PROVIDED ═══
Read every message carefully, then diagnose before prescribing:

1. PSYCHOLOGICAL STATE — what is this prospect actually feeling?
   (Curious / Engaged / Warm / Resistant / Sceptical / Ghosting / Price-sensitive / Burned before)

2. CORE BARRIER — what is the REAL objection or fear underneath their behaviour?
   (Fear of change / ROI uncertainty / Authority — needs boss approval / Timing / Trust deficit / Competition)

3. COMMUNICATION SIGNALS — analyse their language:
   • Word choice (formal vs casual → match their register)
   • Length of replies (brief = low engagement or busy; long = interested but not convinced)
   • Questions asked (reveals what matters to them)
   • What they DIDN'T say (silence and avoidance are data)
   • Positive signals they've dropped (even small ones)

4. TACTICAL PRESCRIPTION — match the lever to the diagnosis:
   • Ghosting → Pattern interrupt + strategic withdrawal ("I'll assume the timing isn't right — good luck")
   • Price objection → ROI reframe + loss aversion + social proof from similar companies
   • Warm but stalling → Commitment escalation + reduce friction + make next step trivially easy
   • Engaged but not booking → Create gentle urgency + Columbo close + direct ask
   • Sceptical → Authority + hyper-specific proof + remove risk (guarantee, trial, free value)
   • Cold reply → Find the one thing they cared about and go deeper on that

5. SWIPE FILE APPLICATION — look through the swipe files and identify which specific technique, hook, or copy style fits this situation best. Reference it explicitly.

═══ STAGE-BY-STAGE PLAYBOOK ═══
{playbook_text}

═══ LIVE CRM DATA ═══
{crm_context}

{swipe_context}{mail_section}

═══ HOW YOU RESPOND ═══
• Lead with the DIAGNOSIS (1-2 sentences on where this prospect is psychologically)
• Then give the PRESCRIPTION: one clear priority action with the psychological rationale
• Then give READY-TO-USE COPY when relevant (full email draft, not a template — personalised)
• Be direct and specific — vague advice is worthless; name the tactic and explain why it works here
• Reference actual email subjects, phrases the prospect used, or open/reply data when it's in context
• When drafting emails: match the prospect's register, use the right psychological lever, keep it human
• Never use corporate buzzwords. Never be generic. Every answer should feel like it was made for THIS deal."""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/chat")
async def agent_chat(body: dict):
    """
    Multi-turn marketing agent chat.
    Body: { messages: [{role, content}], deal_id?: str }
    When deal_id is provided the agent has full email thread + psychological analysis context.
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    messages = body.get("messages", [])
    if not messages:
        raise HTTPException(status_code=422, detail="messages array required")

    deal_id = body.get("deal_id", "")

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
    Deep strategic advice for a single deal — reads full email thread,
    diagnoses psychological state, prescribes specific tactics and copy.
    Body: { deal_id: str, question?: str }
    """
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    deal_id  = body.get("deal_id", "")
    question = body.get("question", "Analyse the full email thread and tell me exactly what to do next to move this deal forward.")

    if not deal_id:
        raise HTTPException(status_code=422, detail="deal_id required")

    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    stage        = deal.get("stage", "new")
    playbook_tip = STAGE_PLAYBOOK.get(stage, "Focus on booking a call.")

    deal_ctx = f"""DEAL PROFILE:
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
Last reply snippet: {(deal.get('reply_body') or '')[:400]}
Company description: {(deal.get('company_desc') or '')[:300]}"""

    mail_task  = asyncio.create_task(_build_mail_context(deal.get("email", ""), deal_id))
    swipe_task = asyncio.create_task(_build_swipe_context(limit=10))
    mail_context, swipe_context = await mail_task, await swipe_task

    if mail_context:
        deal_ctx += f"\n\n{mail_context}"

    system = f"""You are PALM AI — an elite B2B sales strategist and psychologist.

Your job: read the full email thread, diagnose the prospect's psychological state, and prescribe the exact tactic that breaks their resistance and moves them toward a booked call.

{swipe_context}

RESPONSE FORMAT:
1. DIAGNOSIS — prospect's psychological state and real barrier (2-3 sentences)
2. THE LEVER — which psychological principle to apply and exactly why it fits here
3. NEXT ACTION — one clear, specific move (with rationale)
4. READY-TO-SEND COPY — the actual email or message, personalised, human, using the right lever
5. WATCH FOR — what signals in their reply will tell you how it landed

Be razor-sharp. Reference specific things from their emails. Never be generic."""

    user_msg = f"{deal_ctx}\n\nQUESTION: {question}"

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
