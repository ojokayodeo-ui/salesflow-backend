"""
Apollo.io Lead Search Service
Calls the Apollo.io People Search API with ICP-derived filters
and returns up to 100 verified contacts as structured lead records.

API docs: https://apolloio.github.io/apollo-api-docs/?shell#people-search
"""

import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData

logger = logging.getLogger(__name__)

APOLLO_BASE = "https://api.apollo.io/v1"


async def search_leads(icp: ICPData, limit: int = 100) -> list[dict]:
    """
    Search Apollo.io for verified contacts matching the ICP.

    Returns a list of lead dicts:
    {
        "first_name": str,
        "last_name": str,
        "full_name": str,
        "title": str,
        "email": str,
        "company": str,
        "city": str,
        "country": str,
        "linkedin_url": str,
    }

    Falls back to an empty list if the API key is missing or the call fails.
    """
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — skipping lead search")
        return []

    # Build the request payload from ICP fields
    payload = {
        "api_key": settings.apollo_api_key,
        "per_page": min(limit, 100),
        "page": 1,
        "person_titles": icp.target_titles,
        "organization_locations": [icp.hq_country],
        "organization_num_employees_ranges": [
            f"{icp.apollo_employee_min},{icp.apollo_employee_max}"
        ],
        "contact_email_status": ["verified"],   # verified emails only
        "q_organization_keyword_tags": icp.keywords,
        "prospected_by_current_team": ["no"],   # exclude existing contacts
    }

    # Add industry if present
    if icp.industry:
        payload["organization_industry_tag_ids"] = [icp.industry]

    logger.info(
        "Searching Apollo for %d leads — %s / %s in %s",
        limit, icp.industry, icp.sub_niche, icp.hq_country,
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{APOLLO_BASE}/mixed_people/search",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

        people = data.get("people", [])
        leads = []

        for p in people:
            email = (p.get("email") or "").strip()
            if not email:
                continue  # skip contacts without a verified email

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
        logger.error("Apollo API HTTP error %s: %s", exc.response.status_code, exc.response.text)
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
