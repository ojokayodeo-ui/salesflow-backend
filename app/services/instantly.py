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
    Returns a dict with all available prospect fields.
    Falls back to empty dict if API call fails.

    Instantly v2 /leads/list accepts `email` as the search key.
    We raise the limit so the strict email-match loop has room to find the lead
    even if the API returns results in an unexpected order.
    """
    if not settings.instantly_api_key:
        logger.warning("INSTANTLY_API_KEY not set — skipping lead enrichment")
        return {}

    # Try multiple request formats; Instantly v2 uses `email` as the filter key.
    # We attempt the two most likely formats and fall back gracefully.
    request_bodies = [
        {"email": email, "limit": 25},           # v2 preferred format
        {"search": email, "limit": 25},           # alternative v2 format
        {"filter": email, "limit": 25},           # legacy format (kept as last resort)
    ]

    for body in request_bodies:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{INSTANTLY_API_BASE}/leads/list",
                    json=body,
                    headers={
                        "Authorization": f"Bearer {settings.instantly_api_key}",
                        "Content-Type": "application/json",
                    },
                )
                resp.raise_for_status()
                data = resp.json()

            items = data.get("items", [])
            if not items:
                continue  # try next format

            # Strict email match
            lead = None
            for item in items:
                item_email = (item.get("email") or "").lower().strip()
                if item_email == email.lower().strip():
                    lead = item
                    break

            if lead:
                logger.info(
                    "Enriched lead from Instantly (format=%s): %s at %s",
                    list(body.keys())[0],
                    lead.get("first_name", "") or lead.get("firstName", ""),
                    lead.get("company_name", "") or lead.get("companyName", ""),
                )
                return lead

            logger.info(
                "Instantly returned %d lead(s) for format=%s but none matched %s exactly",
                len(items), list(body.keys())[0], email,
            )

        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Instantly API error %s (format=%s) for %s: %s",
                exc.response.status_code, list(body.keys())[0], email, exc.response.text[:200],
            )
        except Exception as exc:
            logger.warning(
                "Instantly lead fetch failed (format=%s) for %s: %s",
                list(body.keys())[0], email, exc,
            )

    logger.info("No Instantly lead found for %s after trying all search formats", email)
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
