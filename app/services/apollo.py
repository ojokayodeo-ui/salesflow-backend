"""
Apollo.io Lead Search Service
API confirmed working - uses verified broad search parameters.
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
    Uses broad parameters confirmed working via debug endpoint.
    """
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — skipping lead search")
        return []

    # Use top 3 titles from ICP, fall back to common titles
    titles = (icp.target_titles or [])[:3]
    if not titles:
        titles = ["Managing Director", "CEO", "Founder"]

    # Country from ICP
    country = icp.hq_country or "United Kingdom"

    # Build payload — confirmed working format
    payload = {
        "per_page": min(limit, 100),
        "page": 1,
        "person_titles[]": titles,
        "person_locations[]": [country],
        "contact_email_status[]": ["verified"],
    }

    # Only add employee range if it's reasonable
    emp_min = icp.apollo_employee_min or 0
    emp_max = icp.apollo_employee_max or 0
    if emp_min > 0 and emp_max > 0 and emp_max <= 10000:
        payload["organization_num_employees_ranges[]"] = [f"{emp_min},{emp_max}"]

    logger.info(
        "Apollo search — titles: %s | country: %s | employees: %s",
        titles, country,
        f"{emp_min}-{emp_max}" if emp_min and emp_max else "any",
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
        total = data.get("pagination", {}).get("total_entries", 0)
        logger.info("Apollo: %d people returned (total available: %s)", len(people), total)

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

        logger.info("Apollo: %d leads with verified emails", len(leads))

        # If still 0 results, try without employee range
        if not leads and emp_min and emp_max:
            logger.info("Retrying without employee range filter...")
            payload.pop("organization_num_employees_ranges[]", None)
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(APOLLO_SEARCH_URL, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
            people = data.get("people", [])
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
            logger.info("Apollo retry: %d leads", len(leads))

        return leads[:limit]

    except httpx.HTTPStatusError as exc:
        logger.error("Apollo HTTP error %s: %s", exc.response.status_code, exc.response.text[:300])
        return []
    except Exception as exc:
        logger.exception("Apollo search failed: %s", exc)
        return []


def leads_to_csv(leads: list[dict]) -> str:
    """Convert lead list to CSV string."""
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
