"""
ICP Generation Service

Uses Claude to run a full 6-part B2B prospecting analysis on the prospect's
company, incorporating their website content, LinkedIn, Instantly data, and
reply text to produce a deeply targeted Apollo.io filter set.
"""

import json
import logging
import httpx
from app.config import settings
from app.models.schemas import ICPData, ProspectData

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


async def scrape_website(url: str) -> str:
    """
    Fetch and extract readable text from the prospect's website.
    Returns empty string if the site can't be reached.
    """
    if not url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            # Strip HTML tags simply
            text = resp.text
            import re
            text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
            text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            # Return first 3000 chars — enough for Claude to understand the business
            return text[:3000]
    except Exception as exc:
        logger.warning("Website scrape failed for %s: %s", url, exc)
        return ""


async def generate_icp(prospect: ProspectData, reply_body: str = "") -> ICPData:
    """
    Run the full 6-part ICP analysis using Claude.
    Scrapes the prospect's website first to enrich the analysis.
    Falls back to a safe default if the API call fails.
    """
    # Scrape website for richer context
    website_content = await scrape_website(prospect.website or "")
    if website_content:
        logger.info("Website scraped for %s (%d chars)", prospect.company, len(website_content))
    else:
        logger.info("No website content for %s — using Instantly data only", prospect.company)

    # Build rich context block from all available prospect data
    prospect_context = f"""
PROSPECT DETAILS (from Instantly.ai):
- Name: {prospect.name}
- Email: {prospect.email}
- Job Title: {prospect.job_title or 'Unknown'}
- Company: {prospect.company}
- Website: {prospect.website or prospect.domain or 'Unknown'}
- LinkedIn: {prospect.linkedin or 'Not provided'}
- Location: {prospect.location or 'Unknown'}
- Company Headcount: {prospect.headcount or 'Unknown'}
- Industry (Instantly): {prospect.industry or 'Unknown'}
- Sub-industry: {prospect.sub_industry or 'Unknown'}
- Company Description: {prospect.description[:500] if prospect.description else 'Not provided'}
- Headline: {prospect.headline or 'Not provided'}

PROSPECT'S REPLY:
"{reply_body or 'No reply text captured'}"

WEBSITE CONTENT (scraped):
{website_content if website_content else 'Could not scrape website — use company description and Instantly data above.'}
""".strip()

    prompt = f"""You are a B2B prospecting expert and outbound marketing strategist who specialises in building highly targeted lead lists using Apollo.io.

Your task is to analyse the following business and generate the best Apollo.io prospecting settings to find their ideal customers.

{prospect_context}

Use ALL the information above — the website content, the company description, the prospect's job title, their reply, and strategic inference — to determine the best targeting criteria.

Provide outputs that can be directly used inside Apollo.io filters.

Return ONLY valid JSON — no markdown, no explanation, no fences. Use this exact structure:

{{
  "industry": "Primary industry e.g. Management Consulting",
  "sub_niche": "Specific niche e.g. NHS / Public Sector Strategy",
  "company_size": "e.g. 10-50 employees",
  "hq_country": "e.g. United Kingdom",
  "revenue_range": "e.g. £500k-£5M",
  "target_titles": ["Primary title 1", "Primary title 2", "Primary title 3"],
  "secondary_titles": ["Secondary title 1", "Secondary title 2", "Alternative title 1"],
  "pain_point": "The single biggest pain point this company's ideal customer faces",
  "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "apollo_employee_min": 10,
  "apollo_employee_max": 50,
  "company_age_years": "2-6",
  "buying_signal": "The strongest signal that a company needs this service right now",
  "technologies": ["tech1", "tech2"],
  "growth_signals": ["signal1", "signal2"],
  "cold_email_hook": "One sentence hook that would resonate strongly with this audience",
  "value_proposition": "One sentence value prop tailored to this ICP",
  "ideal_list_size": 100,
  "segmentation": ["Segment 1: e.g. By industry vertical", "Segment 2: e.g. By company size", "Segment 3: e.g. By seniority"]
}}

PART 1 thinking: What industry/niche does this prospect serve? Who are THEIR ideal customers?
PART 2 thinking: What Apollo company filters would find those customers?
PART 3 thinking: What job titles hold the buying power for this type of service?
PART 4 thinking: What signals indicate a company is ready to buy right now?
PART 5 thinking: How should the lead list be sized and segmented?
PART 6 thinking: What cold email angle would cut through for this specific audience?"""

    headers = {
        "x-api-key":         settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type":      "application/json",
    }
    payload = {
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "messages":   [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(ANTHROPIC_URL, json=payload, headers=headers)
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"]
            # Strip any accidental markdown fences
            raw = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
            icp = ICPData(**data)
            logger.info(
                "ICP generated for %s: %s / %s | Hook: %s",
                prospect.company, icp.industry, icp.sub_niche,
                (icp.cold_email_hook or "")[:60],
            )
            return icp

    except Exception as exc:
        logger.exception("ICP generation failed, using fallback: %s", exc)
        return ICPData(
            industry           = "Management Consulting",
            sub_niche          = "Strategy & Operations",
            company_size       = "10-50 employees",
            hq_country         = "United Kingdom",
            revenue_range      = "£500k-£5M",
            target_titles      = ["Managing Director", "Founder", "CEO"],
            secondary_titles   = ["Head of Strategy", "Director", "Partner"],
            pain_point         = "No predictable pipeline for booking qualified meetings",
            keywords           = ["management consulting", "strategy", "business growth", "B2B", "consulting firm"],
            apollo_employee_min= 10,
            apollo_employee_max= 50,
            company_age_years  = "2-6",
            buying_signal      = "Actively hiring in sales or business development",
            technologies       = [],
            growth_signals     = ["Hiring for sales roles", "Recently funded"],
            cold_email_hook    = "Most consulting firms rely on referrals — we build you a predictable outbound system",
            value_proposition  = "We deliver 100 verified leads and book qualified appointments on a pay-per-meeting basis",
            ideal_list_size    = 100,
            segmentation       = ["By industry vertical", "By company size", "By seniority level"],
        )


def icp_to_apollo_filters(icp: ICPData) -> dict:
    """Map ICP fields to Apollo.io People Search filter parameters."""
    all_titles = icp.target_titles + (icp.secondary_titles or [])
    return {
        "person_titles":                    all_titles,
        "organization_industry_tag_ids":    icp.industry,
        "organization_num_employees_ranges":f"{icp.apollo_employee_min},{icp.apollo_employee_max}",
        "person_locations":                 [icp.hq_country],
        "q_organization_keyword_tags":      icp.keywords,
        "contact_email_status":             ["verified"],
        "limit":                            icp.ideal_list_size or 100,
        "sort_by":                          "recommendations",
    }
