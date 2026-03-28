"""
Email Composer Service
Generates personalised delivery email bodies using Claude,
with fallback templates for each style.
"""

import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData, ProspectData

logger = logging.getLogger(__name__)

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"


def template_warm(prospect: ProspectData, icp: ICPData, from_name: str) -> str:
    titles = ", ".join(icp.target_titles[:2]) if icp.target_titles else "senior decision-makers"
    return f"""Hi {prospect.name.split()[0]},

As promised — here are your 100 targeted leads.

I've built this list specifically around companies that match your profile: {icp.sub_niche} firms with {icp.company_size}, based in {icp.hq_country}.

Every contact has been filtered for the decision-maker titles most likely to say yes to what you do — {titles}.

The CSV is attached. A few things worth knowing:
- All emails are verified
- Sorted by growth signal (highest intent first)
- Includes company name and city for quick prioritisation

Let me know if you'd like a tighter niche, a different geography, or more senior contacts — I can turn a revised list around quickly.

Looking forward to getting you booked meetings.

{from_name}"""


def template_direct(prospect: ProspectData, icp: ICPData, from_name: str) -> str:
    titles = ", ".join(icp.target_titles) if icp.target_titles else "decision makers"
    return f"""Hi {prospect.name.split()[0]},

Your 100 leads are attached.

Filters applied:
- Industry: {icp.industry}
- Sub-niche: {icp.sub_niche}
- Company size: {icp.company_size}
- Location: {icp.hq_country}
- Titles: {titles}

All contacts are email-verified. Reply if you need adjustments.

{from_name}"""


async def template_ai(
    prospect: ProspectData,
    icp: ICPData,
    from_name: str,
    sender_email: str,
) -> str:
    """Generate a fully personalised email body via Claude."""
    prompt = f"""You are writing a lead delivery email on behalf of a cold email agency owner named {from_name}.

Sender: {from_name} <{sender_email}>
Recipient: {prospect.name} at {prospect.company}

ICP context used to build their lead list:
- Industry: {icp.industry} / {icp.sub_niche}
- Company size: {icp.company_size}
- Location: {icp.hq_country}
- Target titles: {', '.join(icp.target_titles)}
- Core pain point: {icp.pain_point}
- Buying signal used: {icp.buying_signal}

Write a warm, professional email body (no subject line). 3–4 short paragraphs.
- Open by referencing their specific niche and the pain point they mentioned
- Explain that the list was built around their ICP (briefly)
- Give 2–3 bullet points about what's in the CSV
- End with a soft next step (not pushy)
Tone: direct, peer-to-peer, no corporate fluff."""

    headers = {
        "x-api-key":         settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type":      "application/json",
    }
    payload = {
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": 600,
        "messages":   [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=25) as client:
            resp = await client.post(ANTHROPIC_MESSAGES_URL, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]
    except Exception as exc:
        logger.exception("AI email composition failed, using warm template: %s", exc)
        return template_warm(prospect, icp, from_name)


async def compose_email_body(
    prospect:    ProspectData,
    icp:         ICPData,
    from_name:   str,
    sender_email: str,
    template:    str = "warm",  # "warm" | "direct" | "ai"
) -> str:
    if template == "direct":
        return template_direct(prospect, icp, from_name)
    if template == "ai":
        return await template_ai(prospect, icp, from_name, sender_email)
    return template_warm(prospect, icp, from_name)
