"""
Apollo.io Lead Search Service

Two-step flow:
  1. Search  — POST /api/v1/mixed_people/api_search  → returns people (often no email)
  2. Reveal  — POST /api/v1/people/match per person   → reveals email using export credits

Apollo's search endpoint only returns emails for contacts already exported from your
account. For new contacts the email field is null. The reveal step consumes 1 export
credit per contact and returns the actual email address.
"""

import asyncio
import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData

logger = logging.getLogger(__name__)

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_MATCH_URL  = "https://api.apollo.io/api/v1/people/match"


def _make_headers() -> dict:
    return {
        "Content-Type":  "application/json",
        "X-Api-Key":     settings.apollo_api_key,
        "Cache-Control": "no-cache",
    }


def _build_lead(p: dict) -> dict | None:
    """Convert raw Apollo person dict to our lead dict. Returns None if no email."""
    email = (p.get("email") or "").strip()
    if not email:
        return None
    es = (p.get("email_status") or "").lower()
    if es not in ("verified", ""):
        return None

    # Apollo returns org data under "organization" (search) or "employment_history"
    # The reveal response may also put current org data directly on the person.
    org = p.get("organization") or {}

    # If org is sparse, try to supplement from account/company fields on person
    if not org.get("website_url"):
        org = dict(org)
        org["website_url"]             = p.get("company_website_url") or p.get("website_url") or ""
        org["linkedin_url"]            = org.get("linkedin_url") or p.get("company_linkedin_url") or ""
        org["industry"]                = org.get("industry") or p.get("industry") or ""
        org["estimated_num_employees"] = (org.get("estimated_num_employees")
                                          or p.get("num_employees")
                                          or p.get("company_size"))
        org["founded_year"]            = org.get("founded_year") or p.get("founded_year") or ""

    phones = p.get("phone_numbers") or []
    # phone_numbers is a list of dicts; also check sanitized_number and raw_number
    phone = ""
    for ph in phones:
        phone = ph.get("sanitized_number") or ph.get("raw_number") or ""
        if phone:
            break
    # Fallback: some reveal responses put the number directly on person
    if not phone:
        phone = p.get("phone_number") or p.get("direct_phone") or ""

    return {
        "first_name":        p.get("first_name", ""),
        "last_name":         p.get("last_name", ""),
        "full_name":         p.get("name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
        "title":             p.get("title", ""),
        "seniority":         p.get("seniority", ""),
        "departments":       ", ".join(p.get("departments") or []),
        "email":             email,
        "email_status":      p.get("email_status", ""),
        "phone":             phone,
        "linkedin_url":      p.get("linkedin_url", ""),
        "twitter_url":       p.get("twitter_url", ""),
        "city":              p.get("city", ""),
        "state":             p.get("state", ""),
        "country":           p.get("country", ""),
        "company":           org.get("name", "") or p.get("company_name", ""),
        "company_website":   org.get("website_url", ""),
        "company_linkedin":  org.get("linkedin_url", ""),
        "company_industry":  org.get("industry", ""),
        "company_employees": str(org.get("estimated_num_employees") or org.get("num_employees") or ""),
        "company_founded":   str(org.get("founded_year") or ""),
        "apollo_id":         p.get("id", ""),
    }


async def _reveal_email(person: dict, headers: dict) -> dict | None:
    """
    Call Apollo's people/match endpoint to reveal a single contact's email.
    Uses 1 export credit. Returns enriched person dict with email + all available
    fields from the reveal response merged in (org data, phone, linkedin, etc.).
    """
    pid = person.get("id") or person.get("person_id")
    linkedin = person.get("linkedin_url", "")

    if not pid and not linkedin:
        return None

    payload: dict = {"reveal_personal_emails": True, "reveal_phone_number": True}
    if pid:
        payload["id"] = pid
    elif linkedin:
        payload["linkedin_url"] = linkedin

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(APOLLO_MATCH_URL, json=payload, headers=headers)
            if resp.status_code not in (200, 201):
                return None
            data = resp.json()
        matched = data.get("person") or data
        email = (matched.get("email") or "").strip()
        if not email:
            return None

        # Start from the original search person, then overlay all non-empty
        # fields from the reveal response — the reveal returns richer data
        enriched = dict(person)
        for key, val in matched.items():
            if val is not None and val != "" and val != [] and val != {}:
                enriched[key] = val

        # Always trust the reveal's email and status
        enriched["email"]        = email
        enriched["email_status"] = matched.get("email_status", "")
        return enriched
    except Exception as exc:
        logger.debug("Email reveal failed for %s: %s", pid or linkedin, exc)
        return None


async def _reveal_emails_batch(
    people: list[dict],
    headers: dict,
    max_reveal: int = 30,
) -> list[dict]:
    """
    For each person in `people` that has no email, call _reveal_email to get it.
    Runs reveals concurrently (up to max_reveal contacts) to stay fast.
    Returns updated list — contacts still missing emails are dropped.
    """
    need_reveal = [p for p in people if not (p.get("email") or "").strip()][:max_reveal]
    already_have = [p for p in people if (p.get("email") or "").strip()]

    if not need_reveal:
        return already_have

    logger.info("Apollo: revealing emails for %d contacts (using export credits)", len(need_reveal))

    tasks = [_reveal_email(p, headers) for p in need_reveal]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    revealed = []
    for r in results:
        if isinstance(r, dict) and (r.get("email") or "").strip():
            revealed.append(r)

    logger.info(
        "Apollo email reveal: %d/%d contacts got emails",
        len(revealed), len(need_reveal),
    )
    return already_have + revealed


async def _apollo_search(
    payload: dict,
    headers: dict,
    max_reveal: int = 30,
) -> tuple[list[dict], int]:
    """
    Execute one Apollo search and reveal emails for contacts that don't have them.
    Returns (people_with_emails, total_available).
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(APOLLO_SEARCH_URL, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    people = data.get("people", [])
    total  = data.get("pagination", {}).get("total_entries", 0)
    logger.info("Apollo search: %d people returned (total available: %s)", len(people), total)

    if not people:
        return [], total

    # Log what fields the first person actually has so we can debug field mapping
    if people:
        sample = people[0]
        org_keys = list((sample.get("organization") or {}).keys())
        logger.debug(
            "Apollo sample person keys: %s | org keys: %s",
            list(sample.keys()), org_keys,
        )

    # Reveal emails for contacts that don't have one yet
    people = await _reveal_emails_batch(people, headers, max_reveal=max_reveal)

    leads = [l for l in (_build_lead(p) for p in people) if l]
    return leads, total


async def search_leads(icp: ICPData, limit: int = 100) -> list[dict]:
    """
    Search Apollo.io for contacts matching the ICP and reveal their emails.
    """
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — skipping lead search")
        return []

    titles  = (icp.target_titles or [])[:3] or ["Managing Director", "CEO", "Founder"]
    country = icp.hq_country or "United Kingdom"
    emp_min = icp.apollo_employee_min or 0
    emp_max = icp.apollo_employee_max or 0

    payload = {
        "per_page":               min(limit, 50),
        "page":                   1,
        "person_titles[]":        titles,
        "person_locations[]":     [country],
        "contact_email_status[]": ["verified"],
    }
    if emp_min > 0 and emp_max > 0 and emp_max <= 10000:
        payload["organization_num_employees_ranges[]"] = [f"{emp_min},{emp_max}"]

    headers = _make_headers()
    logger.info("Apollo search — titles: %s | country: %s | employees: %s-%s",
                titles, country, emp_min or "any", emp_max or "any")

    try:
        leads, _ = await _apollo_search(payload, headers, max_reveal=min(limit, 30))

        # Retry without employee range if still nothing
        if not leads and (emp_min or emp_max):
            logger.info("Apollo: retrying without employee range")
            payload.pop("organization_num_employees_ranges[]", None)
            leads, _ = await _apollo_search(payload, headers, max_reveal=min(limit, 30))

        logger.info("Apollo: %d leads with emails", len(leads))
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
        return "First Name,Last Name,Full Name,Title,Seniority,Departments,Email,Email Status,Phone,LinkedIn,Twitter,City,State,Country,Company,Company Website,Company LinkedIn,Company Industry,Company Employees,Company Founded,Apollo ID\n"

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
        fieldnames = list(valid_raws[0].keys())
        out = _io.StringIO()
        writer = _csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for i, lead in enumerate(leads):
            if raw_rows[i]:
                writer.writerow(raw_rows[i])
            else:
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
                    for fn in fieldnames:
                        if fn.lower().replace(" ", "_") == key:
                            fallback[fn] = val
                            break
                writer.writerow(fallback)
        return out.getvalue()

    # Fallback: standard fixed columns (Apollo-sourced leads)
    fieldnames = [
        "First Name", "Last Name", "Full Name", "Title", "Seniority", "Departments",
        "Email", "Email Status", "Phone", "LinkedIn", "Twitter",
        "City", "State", "Country",
        "Company", "Company Website", "Company LinkedIn", "Company Industry",
        "Company Employees", "Company Founded", "Apollo ID",
    ]
    field_map = {
        "first_name":        "First Name",
        "last_name":         "Last Name",
        "full_name":         "Full Name",
        "title":             "Title",
        "seniority":         "Seniority",
        "departments":       "Departments",
        "email":             "Email",
        "email_status":      "Email Status",
        "phone":             "Phone",
        "linkedin_url":      "LinkedIn",
        "twitter_url":       "Twitter",
        "city":              "City",
        "state":             "State",
        "country":           "Country",
        "company":           "Company",
        "company_website":   "Company Website",
        "company_linkedin":  "Company LinkedIn",
        "company_industry":  "Company Industry",
        "company_employees": "Company Employees",
        "company_founded":   "Company Founded",
        "apollo_id":         "Apollo ID",
    }
    out = _io.StringIO()
    writer = _csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for lead in leads:
        row = {col: "" for col in fieldnames}
        for key, col in field_map.items():
            row[col] = lead.get(key, "")
        writer.writerow(row)
    return out.getvalue()
