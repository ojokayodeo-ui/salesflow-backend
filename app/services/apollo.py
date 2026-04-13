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
    """
    Convert lead list to CSV string.
    If leads were uploaded by the user, the original columns are preserved
    exactly via the raw_json field — so the outgoing CSV matches the upload.
    Falls back to the standard 8-column format for Apollo-sourced leads.
    """
    import csv as _csv, io as _io, json as _json

    if not leads:
        return "First Name,Last Name,Title,Company,Email,City,Country,LinkedIn\n"

    # Attempt to reconstruct from raw_json (user-uploaded CSVs)
    raw_rows = []
    for lead in leads:
        raw = lead.get("raw_json")
        if raw:
            try:
                raw_rows.append(_json.loads(raw) if isinstance(raw, str) else raw)
            except Exception:
                raw_rows.append(None)
        else:
            raw_rows.append(None)

    valid_raws = [r for r in raw_rows if r]
    if valid_raws:
        # Use the column order from the first row that has raw data
        fieldnames = list(valid_raws[0].keys())
        out = _io.StringIO()
        writer = _csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for i, lead in enumerate(leads):
            if raw_rows[i]:
                writer.writerow(raw_rows[i])
            else:
                # Lead has no raw data — map what we have into the known columns
                fallback = {k: "" for k in fieldnames}
                for key, val in {
                    "first_name": lead.get("first_name", ""),
                    "last_name":  lead.get("last_name", ""),
                    "title":      lead.get("title", ""),
                    "company":    lead.get("company", ""),
                    "email":      lead.get("email", ""),
                    "city":       lead.get("city", ""),
                    "country":    lead.get("country", ""),
                    "linkedin_url": lead.get("linkedin_url", ""),
                }.items():
                    # Case-insensitive match against fieldnames
                    for fn in fieldnames:
                        if fn.lower().replace(" ", "_") == key:
                            fallback[fn] = val
                            break
                writer.writerow(fallback)
        return out.getvalue()

    # Fallback: standard fixed columns (Apollo-sourced leads)
    out = _io.StringIO()
    writer = _csv.DictWriter(
        out,
        fieldnames=["First Name", "Last Name", "Title", "Company", "Email", "City", "Country", "LinkedIn"],
        lineterminator="\n",
    )
    writer.writeheader()
    for lead in leads:
        writer.writerow({
            "First Name": lead.get("first_name", ""),
            "Last Name":  lead.get("last_name", ""),
            "Title":      lead.get("title", ""),
            "Company":    lead.get("company", ""),
            "Email":      lead.get("email", ""),
            "City":       lead.get("city", ""),
            "Country":    lead.get("country", ""),
            "LinkedIn":   lead.get("linkedin_url", ""),
        })
    return out.getvalue()
