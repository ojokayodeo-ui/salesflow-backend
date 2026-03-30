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
    """
    if not settings.instantly_api_key:
        logger.warning("INSTANTLY_API_KEY not set — skipping lead enrichment")
        return {}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Instantly v2 uses POST /leads/list — filter is a plain string (the email)
            resp = await client.post(
                f"{INSTANTLY_API_BASE}/leads/list",
                json={"filter": email, "limit": 1},
                headers={
                    "Authorization": f"Bearer {settings.instantly_api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # Instantly v2 /leads/list returns {"items": [...]}
        items = data.get("items", [])
        if not items:
            logger.info("No lead found in Instantly for %s", email)
            return {}

        # Strict email match — verify the returned lead matches the searched email
        lead = None
        for item in items:
            item_email = (item.get("email") or "").lower().strip()
            if item_email == email.lower().strip():
                lead = item
                break

        if not lead:
            logger.info("Instantly returned leads but none matched email %s exactly", email)
            return {}

        logger.info("Enriched lead from Instantly: %s at %s",
                   lead.get("first_name","") or lead.get("firstName",""),
                   lead.get("company_name","") or lead.get("companyName",""))
        return lead

    except httpx.HTTPStatusError as exc:
        logger.warning("Instantly API error %s for %s: %s", 
                      exc.response.status_code, email, exc.response.text[:200])
        return {}
    except Exception as exc:
        logger.warning("Instantly lead fetch failed for %s: %s", email, exc)
        return {}


def extract_prospect_data(lead: dict, payload) -> dict:
    """
    Merge Instantly API lead data with webhook payload.
    Returns a clean dict of all prospect fields.
    Priority: Instantly API data > webhook payload fields.
    """
    # From Instantly API — handle both camelCase (v1) and snake_case (v2) field names
    first_name    = lead.get("first_name") or lead.get("firstName") or payload.firstName or ""
    last_name     = lead.get("last_name") or lead.get("lastName") or payload.lastName or ""
    company_name  = lead.get("company_name") or lead.get("companyName") or payload.companyName or ""
    company_domain= lead.get("company_domain") or lead.get("companyDomain") or payload.companyDomain or ""
    website       = lead.get("website") or payload.companyWebsite or ""
    job_title     = lead.get("job_title") or lead.get("personTitle") or lead.get("jobTitle") or payload.jobTitle or ""
    linkedin      = lead.get("linkedin_url") or lead.get("linkedInUrl") or lead.get("linkedIn") or payload.linkedIn or ""
    location      = lead.get("city") or lead.get("location") or payload.location or ""
    country       = lead.get("country") or ""
    if country and location:
        location = f"{location}, {country}"
    elif country:
        location = country
    headcount     = str(lead.get("employee_count") or lead.get("employeeCount") or lead.get("companyHeadCount") or payload.companyHeadCount or "")
    industry      = lead.get("industry") or payload.industry or ""
    sub_industry  = lead.get("sub_industry") or lead.get("subIndustry") or payload.subIndustry or ""
    description   = lead.get("company_description") or lead.get("companyDescription") or payload.companyDescription or ""
    headline      = lead.get("headline") or payload.headline or ""

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
        "job_level":    lead.get("jobLevel") or payload.jobLevel or "",
        "department":   lead.get("department") or payload.department or "",
        "reply_subject":payload.reply_subject or "",
    }
