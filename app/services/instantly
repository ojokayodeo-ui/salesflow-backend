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
            resp = await client.get(
                f"{INSTANTLY_API_BASE}/leads",
                params={"email": email, "limit": 1},
                headers={
                    "Authorization": f"Bearer {settings.instantly_api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # Instantly v2 returns {"items": [...]}
        items = data.get("items", [])
        if not items:
            logger.info("No lead found in Instantly for %s", email)
            return {}

        lead = items[0]
        logger.info("Enriched lead from Instantly: %s at %s", 
                   lead.get("firstName",""), lead.get("companyName",""))
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
    # From Instantly API
    first_name    = lead.get("firstName") or payload.firstName or ""
    last_name     = lead.get("lastName") or payload.lastName or ""
    company_name  = lead.get("companyName") or payload.companyName or ""
    company_domain= lead.get("companyDomain") or payload.companyDomain or ""
    website       = lead.get("website") or payload.companyWebsite or ""
    job_title     = lead.get("personTitle") or lead.get("jobTitle") or payload.jobTitle or ""
    linkedin      = lead.get("linkedInUrl") or lead.get("linkedIn") or payload.linkedIn or ""
    location      = lead.get("city") or lead.get("location") or payload.location or ""
    if lead.get("country") and location:
        location = f"{location}, {lead.get('country')}"
    elif lead.get("country"):
        location = lead.get("country", "")
    headcount     = str(lead.get("employeeCount") or lead.get("companyHeadCount") or payload.companyHeadCount or "")
    industry      = lead.get("industry") or payload.industry or ""
    sub_industry  = lead.get("subIndustry") or payload.subIndustry or ""
    description   = lead.get("companyDescription") or payload.companyDescription or ""
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
