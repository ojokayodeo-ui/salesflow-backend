"""
Instantly.ai API Service

Fetches full lead/prospect data from Instantly when a webhook fires.
The webhook only sends email + reply text — this enriches it with
the full prospect profile: name, company, job title, location, etc.
"""

import logging
import httpx
from app.config import settings

logger = logging.getLogger(__name__)

INSTANTLY_API_BASE = "https://api.instantly.ai/api/v2"


async def get_lead_by_email(email: str) -> dict:
    """
    Fetch full lead data from Instantly API by email address.
    Tries multiple known Instantly v2 API formats to maximise hit rate.
    Returns a dict with all available prospect fields, or {} if not found.
    """
    if not settings.instantly_api_key:
        logger.warning("INSTANTLY_API_KEY not set — skipping lead enrichment")
        return {}

    auth_headers = {
        "Authorization": f"Bearer {settings.instantly_api_key}",
        "Content-Type": "application/json",
    }

    def _find_in_items(items: list) -> dict | None:
        """Return the first item whose email matches exactly."""
        for item in items:
            if (item.get("email") or "").lower().strip() == email.lower().strip():
                return item
        return None

    def _parse_items(data) -> list:
        """Handle multiple Instantly response envelope formats."""
        if isinstance(data, list):
            return data
        for key in ("items", "data", "leads", "results", "contacts"):
            if isinstance(data.get(key), list):
                return data[key]
        return []

    # ── Attempt 1: GET /leads?email=xxx ─────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{INSTANTLY_API_BASE}/leads",
                params={"email": email, "limit": 25},
                headers=auth_headers,
            )
            resp.raise_for_status()
            items = _parse_items(resp.json())
            lead = _find_in_items(items)
            if lead:
                logger.info("Instantly lead found (GET /leads?email): %s at %s",
                            lead.get("first_name",""), lead.get("company_name",""))
                return lead
            if items:
                logger.info("GET /leads?email returned %d items but no exact match for %s", len(items), email)
    except httpx.HTTPStatusError as exc:
        logger.info("GET /leads?email → %s for %s", exc.response.status_code, email)
    except Exception as exc:
        logger.info("GET /leads?email failed for %s: %s", email, exc)

    # ── Attempts 2–4: POST /leads/list with various filter formats ───────────
    post_bodies = [
        {"email": email, "limit": 25},
        {"search": email, "limit": 25},
        {"filter": email, "limit": 25},
    ]

    for body in post_bodies:
        fmt = list(body.keys())[0]
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{INSTANTLY_API_BASE}/leads/list",
                    json=body,
                    headers=auth_headers,
                )
                resp.raise_for_status()
                items = _parse_items(resp.json())
                lead = _find_in_items(items)
                if lead:
                    logger.info("Instantly lead found (POST /leads/list fmt=%s): %s at %s",
                                fmt, lead.get("first_name",""), lead.get("company_name",""))
                    return lead
                if items:
                    logger.info("POST /leads/list fmt=%s returned %d items but no exact match for %s",
                                fmt, len(items), email)
        except httpx.HTTPStatusError as exc:
            logger.info("POST /leads/list fmt=%s → %s for %s", fmt, exc.response.status_code, email)
        except Exception as exc:
            logger.info("POST /leads/list fmt=%s failed for %s: %s", fmt, email, exc)

    logger.warning("No Instantly lead found for %s after all attempts", email)
    return {}


def extract_prospect_data(lead: dict, payload) -> dict:
    """
    Merge Instantly API lead data with webhook payload.
    Returns a clean dict of all prospect fields.
    Priority: Instantly API data > webhook payload fields.

    Instantly v2 stores profile fields in a nested 'personalization' object.
    We flatten it into the lead dict first so all lookups work the same way.
    """
    # ── Flatten Instantly v2 'personalization' object ────────────────────────
    # Instantly v2 /leads/list returns profile data nested under 'personalization'
    # e.g. {"email": "gary@...", "personalization": {"first_name": "Gary", "job_title": "MD", ...}}
    # Merge it into a flat dict so all field lookups below work identically.
    flat = dict(lead)
    personalization = lead.get("personalization") or {}
    if isinstance(personalization, dict):
        for k, v in personalization.items():
            if v and not flat.get(k):   # personalization fills gaps only; top-level wins
                flat[k] = v
    # Also check 'payload' nested object inside personalization (some Instantly versions)
    inner_payload = personalization.get("payload") or {}
    if isinstance(inner_payload, dict):
        for k, v in inner_payload.items():
            if v and not flat.get(k):
                flat[k] = v

    def p(attr):
        """Safely get attribute from webhook payload."""
        return getattr(payload, attr, None) or ""

    # Names
    first_name   = flat.get("first_name") or flat.get("firstName") or p("first_name") or p("firstName")
    last_name    = flat.get("last_name")  or flat.get("lastName")  or p("last_name")  or p("lastName")

    # Company
    company_name  = flat.get("company_name")   or flat.get("companyName")   or p("company_name")   or p("companyName")
    company_domain= flat.get("company_domain") or flat.get("companyDomain") or p("company_domain") or p("companyDomain")
    website       = flat.get("website") or flat.get("company_website") or flat.get("companyWebsite") or p("company_website") or p("companyWebsite") or p("website")

    # Job
    job_title = flat.get("job_title") or flat.get("personTitle") or flat.get("jobTitle") or p("job_title") or p("jobTitle")
    job_level = flat.get("job_level") or flat.get("jobLevel") or flat.get("seniority") or p("job_level") or p("jobLevel")
    department= flat.get("department") or p("department")

    # Social
    linkedin  = flat.get("linkedin_url") or flat.get("linkedInUrl") or flat.get("linkedIn") or flat.get("linkedin") or p("linkedin_url") or p("linkedIn")

    # Location — build from parts if flat field missing
    location  = flat.get("location") or p("location") or ""
    if not location:
        city    = flat.get("city")    or p("city")    or ""
        state   = flat.get("state")   or p("state")   or ""
        country = flat.get("country") or p("country") or ""
        parts   = [x for x in [city, state, country] if x]
        location = ", ".join(parts)
    elif flat.get("country") and flat["country"] not in location:
        location = f"{location}, {flat['country']}"

    # Company size
    headcount = str(
        flat.get("employee_count") or flat.get("employeeCount") or flat.get("companyHeadCount") or
        flat.get("number_of_employees") or p("employee_count") or p("companyHeadCount") or ""
    )

    # Industry
    industry     = flat.get("industry")     or p("industry")     or ""
    sub_industry = flat.get("sub_industry") or flat.get("subIndustry") or p("sub_industry") or p("subIndustry") or ""
    description  = flat.get("company_description") or flat.get("companyDescription") or flat.get("summary") or p("company_description") or p("companyDescription") or ""
    headline     = flat.get("headline") or flat.get("bio") or p("headline") or ""

    # Build full name
    full_name = f"{first_name} {last_name}".strip()
    if not full_name:
        prospect_email = payload.get_prospect_email()
        full_name = prospect_email.split("@")[0].replace(".", " ").title() if prospect_email else "Unknown"

    # Clean website
    if website and not website.startswith("http"):
        website = "https://" + website

    # Clean domain
    if website and not company_domain:
        company_domain = website.replace("https://","").replace("http://","").replace("www.","").rstrip("/")

    # Also check custom_variables from both lead/flat and payload
    cv = flat.get("custom_variables") or flat.get("customVariables") or {}
    if hasattr(payload, "model_extra") and payload.model_extra:
        cv.update(payload.model_extra)

    def cv_get(*keys):
        for k in keys:
            v = cv.get(k) or cv.get(k.lower()) or cv.get(k.replace("_", " "))
            if v: return str(v)
        return ""

    job_title    = job_title    or cv_get("job_title","jobtitle","title","position","role")
    job_level    = job_level    or cv_get("job_level","seniority","level") or ""
    company_name = company_name or cv_get("company","company_name","organisation","organization")
    location     = location     or cv_get("location","city","country","region")
    industry     = industry     or cv_get("industry","sector","vertical")
    linkedin     = linkedin     or cv_get("linkedin","linkedin_url","linkedin_profile")
    headline     = headline     or cv_get("headline","bio","about","summary")
    department   = department   or getattr(payload,"department",None) or cv_get("department","team","function") or ""

    return {
        "name":         full_name,
        "first_name":   first_name,
        "last_name":    last_name,
        "company":      company_name,
        "domain":       company_domain,
        "website":      website,
        "job_title":    job_title,
        "linkedin":     linkedin,
        "location":     location,
        "headcount":    headcount,
        "industry":     industry,
        "sub_industry": sub_industry,
        "description":  description,
        "headline":     headline,
        "job_level":    job_level,
        "department":   department,
        "reply_subject":getattr(payload,"reply_subject",None) or "",
    }
