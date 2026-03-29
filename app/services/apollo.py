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
    Uses broad parameters to maximise results.
    """
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — skipping lead search")
        return []

    # Use only the most reliable filters to avoid empty results
    # Apollo is very sensitive to filter combinations — fewer filters = more results
    payload = {
        "per_page": min(limit, 100),
        "page": 1,
        "contact_email_status[]": ["verified"],
    }

    # Add job titles if present (limit to top 3 to avoid over-filtering)
    titles = (icp.target_titles or [])[:3]
    if titles:
        payload["person_titles[]"] = titles

    # Add country — use broad location
    if icp.hq_country:
        payload["person_locations[]"] = [icp.hq_country]

    # Add employee range — only if reasonable
    emp_min = icp.apollo_employee_min or 1
    emp_max = icp.apollo_employee_max or 200
    payload["organization_num_employees_ranges[]"] = [f"{emp_min},{emp_max}"]

    # Keywords — single string, max 2 words to avoid over-filtering
    keywords = icp.keywords or []
    if keywords:
        payload["q_keywords"] = keywords[0]  # just the most important keyword

    logger.info(
        "Apollo search — titles: %s | country: %s | employees: %d-%d | keyword: %s",
        titles, icp.hq_country, emp_min, emp_max,
        keywords[0] if keywords else "none",
    )
    # Log the full payload so we can see exactly what is being sent
    logger.info("Apollo FULL PAYLOAD: %s", payload)

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

            if resp.status_code != 200:
                logger.error(
                    "Apollo API error %s: %s",
                    resp.status_code, resp.text[:500],
                )
                # Try with even fewer filters as fallback
                return await _search_minimal(icp, limit, headers)

            data = resp.json()

        people = data.get("people", [])
        # Log the full response structure to diagnose 0 results
        logger.info("Apollo raw response: %d people | total: %s | pagination: %s | keys: %s",
                    len(people),
                    data.get("pagination", {}).get("total_entries", "?"),
                    data.get("pagination", {}),
                    list(data.keys()),
                )
        if not people:
            logger.info("Apollo full response (first 800 chars): %s", str(data)[:800])

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

        # If 0 results, try minimal fallback
        if not leads:
            logger.info("0 results — trying minimal search fallback")
            return await _search_minimal(icp, limit, headers)

        return leads[:limit]

    except httpx.HTTPStatusError as exc:
        logger.error("Apollo HTTP error %s: %s", exc.response.status_code, exc.response.text[:300])
        return []
    except Exception as exc:
        logger.exception("Apollo search failed: %s", exc)
        return []


async def _search_minimal(icp: ICPData, limit: int, headers: dict) -> list[dict]:
    """
    Fallback: search with only job titles and country — no employee range or keywords.
    Maximises chance of getting results.
    """
    titles = (icp.target_titles or ["Managing Director", "CEO", "Founder"])[:3]
    country = icp.hq_country or "United Kingdom"

    payload = {
        "per_page": min(limit, 100),
        "page": 1,
        "person_titles[]": titles,
        "person_locations[]": [country],
        "contact_email_status[]": ["verified"],
    }

    logger.info("Apollo minimal fallback — titles: %s | country: %s", titles, country)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(APOLLO_SEARCH_URL, json=payload, headers=headers)
            if resp.status_code != 200:
                logger.error("Apollo minimal fallback failed: %s %s", resp.status_code, resp.text[:200])
                return []
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

        logger.info("Apollo minimal fallback returned %d leads", len(leads))
        return leads[:limit]

    except Exception as exc:
        logger.exception("Apollo minimal fallback failed: %s", exc)
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
