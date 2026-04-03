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
    Handles both snake_case (v2) and camelCase (v1) field names from both sources.
    """
    def p(attr):
        """Safely get attribute from payload, trying snake_case and camelCase variants."""
        return getattr(payload, attr, None) or ""

    # Names
    first_name   = lead.get("first_name") or lead.get("firstName") or p("first_name") or p("firstName")
    last_name    = lead.get("last_name")  or lead.get("lastName")  or p("last_name")  or p("lastName")

    # Company
    company_name  = lead.get("company_name")   or lead.get("companyName")   or p("company_name")   or p("companyName")
    company_domain= lead.get("company_domain") or lead.get("companyDomain") or p("company_domain") or p("companyDomain")
    website       = lead.get("website") or lead.get("company_website") or lead.get("companyWebsite") or p("company_website") or p("companyWebsite") or p("website")

    # Job
    job_title = lead.get("job_title") or lead.get("personTitle") or lead.get("jobTitle") or p("job_title") or p("jobTitle")
    job_level = lead.get("job_level") or lead.get("jobLevel") or p("job_level") or p("jobLevel")
    department= lead.get("department") or p("department")

    # Social
    linkedin  = lead.get("linkedin_url") or lead.get("linkedInUrl") or lead.get("linkedIn") or p("linkedin_url") or p("linkedIn")

    # Location — build from parts if flat field missing
    location  = lead.get("location") or p("location") or ""
    if not location:
        city    = lead.get("city")    or p("city")    or ""
        state   = lead.get("state")   or p("state")   or ""
        country = lead.get("country") or p("country") or ""
        parts   = [x for x in [city, state, country] if x]
        location = ", ".join(parts)
    elif lead.get("country") and lead["country"] not in location:
        location = f"{location}, {lead['country']}"

    # Company size
    headcount = str(
        lead.get("employee_count") or lead.get("employeeCount") or
        lead.get("companyHeadCount") or p("employee_count") or p("companyHeadCount") or ""
    )

    # Industry
    industry     = lead.get("industry")     or p("industry")     or ""
    sub_industry = lead.get("sub_industry") or lead.get("subIndustry") or p("sub_industry") or p("subIndustry") or ""
    description  = lead.get("company_description") or lead.get("companyDescription") or p("company_description") or p("companyDescription") or ""
    headline     = lead.get("headline") or p("headline") or ""

    # Build full name
    full_name = f"{first_name} {last_name}".strip()
    if not full_name:
        email = payload.get_prospect_email()
        full_name = email.split("@")[0].replace(".", " ").title() if email else "Unknown"

    # Clean website
    if website and not website.startswith("http"):
        website = "https://" + website

    # Clean domain
    if website and not company_domain:
        company_domain = website.replace("https://","").replace("http://","").replace("www.","").rstrip("/")

    # Also check custom_variables from both lead and payload
    cv = lead.get("custom_variables") or lead.get("customVariables") or {}
    if hasattr(payload, "model_extra") and payload.model_extra:
        cv.update(payload.model_extra)
    
    # Try to get missing fields from custom_variables
    def cv_get(*keys):
        for k in keys:
            v = cv.get(k) or cv.get(k.lower()) or cv.get(k.replace("_"," "))
            if v: return str(v)
        return ""

    job_title    = job_title    or cv_get("job_title","jobtitle","title","position","role")
    job_level    = lead.get("jobLevel") or getattr(payload,"job_level",None) or getattr(payload,"jobLevel",None) or cv_get("job_level","seniority","level") or ""
    company_name = company_name or cv_get("company","company_name","organisation","organization")
    location     = location     or cv_get("location","city","country","region")
    industry     = industry     or cv_get("industry","sector","vertical")
    linkedin     = linkedin     or cv_get("linkedin","linkedin_url","linkedin_profile")
    headline     = headline     or cv_get("headline","bio","about","summary")
    department   = lead.get("department") or getattr(payload,"department",None) or cv_get("department","team","function") or ""

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
