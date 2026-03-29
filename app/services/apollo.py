"""
Apollo.io Lead Search Service
Uses the People Search API to find verified contacts matching the ICP.
API reference: https://docs.apollo.io/reference/people-api-search
"""

import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData

logger = logging.getLogger(__name__)

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"


async def search_leads(icp: ICPData, limit: int = 100) -> list[dict]:
    """
    Search Apollo.io for verified contacts matching the ICP.
    Returns a list of lead dicts or empty list on failure.
    """
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — skipping lead search")
        return []

    # Apollo api_search uses flat query params, not nested objects
    payload = {
        "per_page": min(limit, 100),
        "page": 1,
        "person_titles[]": icp.target_titles[:5],      # max 5 titles
        "person_locations[]": [icp.hq_country],
        "organization_num_employees_ranges[]": [
            f"{icp.apollo_employee_min},{icp.apollo_employee_max}"
        ],
        "contact_email_status[]": ["verified"],
        "prospected_by_current_team[]": ["no"],
    }

    # Add keywords if present
    if icp.keywords:
        payload["q_keywords"] = " ".join(icp.keywords[:3])

    logger.info(
        "Searching Apollo — %s / %s in %s (%d-%d employees)",
        icp.industry, icp.sub_niche, icp.hq_country,
        icp.apollo_employee_min, icp.apollo_employee_max,
    )

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": settings.apollo_api_key,
        "Cache-Control": "no-cache",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                APOLLO_SEARCH_URL,
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()

        people = data.get("people", [])
        leads = []

        for p in people:
            email = (p.get("email") or "").strip()
            if not email:
                continue

            leads.append({
                "first_name":   p.get("first_name", ""),
                "last_name":    p.get("last_name", ""),
                "full_name":    p.get("name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "title":        p.get("title", ""),
                "email":        email,
                "company":      (p.get("organization") or {}).get("name", ""),
                "city":         p.get("city", ""),
                "country":      p.get("country", ""),
                "linkedin_url": p.get("linkedin_url", ""),
            })

        logger.info("Apollo returned %d verified leads", len(leads))
        return leads[:limit]

    except httpx.HTTPStatusError as exc:
        logger.error(
            "Apollo API HTTP error %s: %s",
            exc.response.status_code, exc.response.text[:300],
        )
        return []
    except Exception as exc:
        logger.exception("Apollo search failed: %s", exc)
        return []


def leads_to_csv(leads: list[dict]) -> str:
    """Convert lead list to a CSV string ready for email attachment."""
    if not leads:
        return "First Name,Last Name,Title,Company,Email,City,Country,LinkedIn\n"

    rows = ["First Name,Last Name,Title,Company,Email,City,Country,LinkedIn"]
    for lead in leads:
        def esc(v: str) -> str:
            v = str(v or "").replace('"', '""')
            return f'"{v}"' if "," in v or '"' in v else v

        rows.append(",".join([
            esc(lead.get("first_name", "")),
            esc(lead.get("last_name", "")),
            esc(lead.get("title", "")),
            esc(lead.get("company", "")),
            esc(lead.get("email", "")),
            esc(lead.get("city", "")),
            esc(lead.get("country", "")),
            esc(lead.get("linkedin_url", "")),
        ]))

    return "\n".join(rows)
