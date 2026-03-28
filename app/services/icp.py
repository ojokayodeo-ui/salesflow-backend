"""
ICP Generation Service
Uses the Anthropic API to analyse prospect data and produce a structured
Ideal Customer Profile that maps directly to Apollo.io filter fields.
"""

import json
import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData, ProspectData

logger = logging.getLogger(__name__)

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"


async def generate_icp(prospect: ProspectData, reply_body: str = "") -> ICPData:
    """
    Call Claude to build a structured ICP from prospect data.
    Falls back to a safe default if the API call fails.
    """
    prompt = f"""You are an expert B2B ICP analyst for a cold email lead generation agency.

Analyse this prospect and return a structured Ideal Customer Profile.

Company: {prospect.company}
Domain: {prospect.domain or "unknown"}
Contact: {prospect.name} <{prospect.email}>
Reply: "{reply_body}"
Region context: UK B2B market

Return ONLY valid JSON — no markdown fences, no explanation:
{{
  "industry": "...",
  "sub_niche": "...",
  "company_size": "10–50 employees",
  "hq_country": "United Kingdom",
  "target_titles": ["Managing Director", "Founder"],
  "pain_point": "...",
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "apollo_employee_min": 10,
  "apollo_employee_max": 50,
  "company_age_years": "2–6",
  "buying_signal": "..."
}}"""

    headers = {
        "x-api-key":         settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type":      "application/json",
    }
    payload = {
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": 800,
        "messages":   [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(ANTHROPIC_MESSAGES_URL, json=payload, headers=headers)
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"]
            data = json.loads(raw.replace("```json", "").replace("```", "").strip())
            return ICPData(**data)
    except Exception as exc:
        logger.exception("ICP generation failed, using fallback: %s", exc)
        return ICPData(
            industry="Management Consulting",
            sub_niche="Strategy & Operations",
            company_size="10–50 employees",
            hq_country="United Kingdom",
            target_titles=["Managing Director", "Founder", "Head of Growth"],
            pain_point="No predictable pipeline for booking qualified meetings",
            keywords=["management consulting", "strategy", "business transformation"],
            apollo_employee_min=10,
            apollo_employee_max=50,
            company_age_years="2–6",
            buying_signal="Actively hiring in sales or business development",
        )


def icp_to_apollo_filters(icp: ICPData) -> dict:
    """Map ICP fields to Apollo.io People Search filter parameters."""
    return {
        "person_titles":                      ", ".join(icp.target_titles),
        "organization_industry_tag_ids":       icp.industry,
        "organization_num_employees_ranges":   f"{icp.apollo_employee_min},{icp.apollo_employee_max}",
        "person_locations":                    icp.hq_country,
        "q_organization_keyword_tags":         ", ".join(icp.keywords),
        "contact_email_status":               "verified",
        "limit":                              100,
        "sort_by":                            "recommendations",
    }
