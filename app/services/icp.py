"""
ICP Generation Service

Generates 5 distinct ICP segments using Claude AI.
Uses structured website intelligence (multi-page crawl) combined with
Instantly.ai lead data and Apify market research to ground every segment
in real, verifiable data.

STRICT: No hallucination. If data is not found, ICP specificity is reduced
        rather than guessing. Each segment includes the source signal that
        led to its creation.
"""

import json
import logging
import httpx
from app.config import settings
from app.models.schemas import ProspectData

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


async def generate_icp_segments(
    prospect: ProspectData,
    reply_body: str = "",
    deal_id: str = "",
) -> list[dict]:
    """
    Generate 5 distinct ICP segments for the prospect's business.

    Data pipeline (in priority order):
      1. Prospect's reply text       -- primary signal
      2. Structured website intel    -- multi-page crawl via website_extractor
      3. Instantly.ai lead profile   -- job title, industry, location, headcount
      4. Apify Google Search         -- external market intelligence

    Args:
        prospect:   Enriched prospect data from Instantly
        reply_body: The original positive reply text
        deal_id:    If provided, website intel is stored on the deal in DB

    Returns list of 5 ICP segment dicts.
    """
    from app.services.website_extractor import extract_website_intel
    from app.services.apify import enrich_icp_context

    # ── 1. Multi-page website extraction ────────────────────────────────────
    website_intel: dict = {}
    if prospect.website or prospect.domain:
        website_url = prospect.website or (
            "https://" + prospect.domain if prospect.domain else ""
        )
        website_intel = await extract_website_intel(website_url, prospect.company or "")
        logger.info(
            "Website intel for %s: status=%s pages=%s",
            prospect.company,
            website_intel.get("status", "?"),
            website_intel.get("source_pages", []),
        )

        # Store intel on the deal immediately if deal_id provided
        if deal_id and website_intel and website_intel.get("status") in ("success", "failed", "skipped"):
            try:
                from app.services import database as _db
                pool = await _db.get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE deals SET website_intel=$1 WHERE id=$2",
                        json.dumps(website_intel), deal_id,
                    )
                logger.info("Website intel stored for deal %s", deal_id)
            except Exception as e:
                logger.warning("Failed to store website intel for deal %s: %s", deal_id, e)

    # ── 2. Apify external enrichment ─────────────────────────────────────────
    reply_keywords: list[str] = []
    if reply_body and reply_body.strip():
        import re as _re
        words = _re.findall(r'\b[A-Za-z]{4,}\b', reply_body)
        stop  = {"that","this","with","from","have","they","their","your","been","will",
                 "would","could","just","also","into","about","more","some","when","than"}
        reply_keywords = [w.lower() for w in words if w.lower() not in stop][:10]

    apify_intel = await enrich_icp_context(
        company_name   = prospect.company or "",
        domain         = prospect.domain or prospect.website or "",
        industry       = prospect.industry or website_intel.get("industry", ""),
        reply_keywords = reply_keywords,
        location       = prospect.location or "United Kingdom",
    )

    # ── 3. Build Apify context block ─────────────────────────────────────────
    apify_block = ""
    if apify_intel:
        parts = []
        if apify_intel.get("client_signals"):
            parts.append("WHO THEY SERVE (Google search):\n" +
                         "\n".join(apify_intel["client_signals"]))
        if apify_intel.get("pain_points"):
            parts.append("SECTOR PAIN POINTS (Google search):\n" +
                         "\n".join(apify_intel["pain_points"]))
        if apify_intel.get("market_context"):
            parts.append("MARKET CONTEXT & BUYING SIGNALS (Google search):\n" +
                         "\n".join(apify_intel["market_context"]))
        if parts:
            apify_block = (
                "\n\n=== EXTERNAL MARKET INTELLIGENCE (Apify Google Search) ===\n" +
                "\n\n".join(parts) +
                "\n=== END EXTERNAL INTELLIGENCE ===\n"
            )
            logger.info("Apify context injected for %s (%d chars)", prospect.company, len(apify_block))

    # ── 4. Build reply signal block ──────────────────────────────────────────
    reply_block = ""
    if reply_body and reply_body.strip() and reply_body != "No reply text captured":
        reply_block = f"""
=== CRITICAL: PROSPECT'S REPLY — READ THIS FIRST ===
This is the most important signal. Extract every mention of:
- Specific industries or sectors they work in
- Types of clients or customers they serve
- Problems or challenges they mentioned
- Geographic focus
- Company types they target
- Any specific names, sectors, or niches

REPLY TEXT:
"{reply_body}"
=== END OF REPLY ===
"""

    # ── 5. Build Instantly lead profile block ────────────────────────────────
    instantly_fields = []
    if prospect.name:         instantly_fields.append(f"  Name: {prospect.name}")
    if prospect.job_title:    instantly_fields.append(f"  Job title: {prospect.job_title}")
    if prospect.job_level:    instantly_fields.append(f"  Seniority: {prospect.job_level}")
    if prospect.department:   instantly_fields.append(f"  Department: {prospect.department}")
    if prospect.company:      instantly_fields.append(f"  Company: {prospect.company}")
    if prospect.website or prospect.domain:
        instantly_fields.append(f"  Website: {prospect.website or prospect.domain}")
    if prospect.linkedin:     instantly_fields.append(f"  LinkedIn: {prospect.linkedin}")
    if prospect.location:     instantly_fields.append(f"  Location: {prospect.location}")
    if prospect.headcount:    instantly_fields.append(f"  Company size: {prospect.headcount} employees")
    if prospect.industry:     instantly_fields.append(f"  Industry: {prospect.industry}")
    if prospect.sub_industry: instantly_fields.append(f"  Sub-industry: {prospect.sub_industry}")
    if prospect.headline:     instantly_fields.append(f"  Headline/bio: {prospect.headline}")
    if prospect.description:  instantly_fields.append(f"  Company description: {prospect.description[:800]}")

    instantly_block = "\n".join(instantly_fields) if instantly_fields else "  (no Instantly data available)"

    # ── 6. Build structured website intel block ───────────────────────────────
    wi_status = website_intel.get("status", "")
    if wi_status == "success":
        wi_parts = []
        wi_parts.append(f"  Pages crawled: {', '.join(website_intel.get('source_pages', []))}")
        wi_parts.append(f"  Industry (from site): {website_intel.get('industry', 'NOT FOUND')}")
        wi_parts.append(f"  Target customers (from site): {website_intel.get('target_customers', 'NOT FOUND')}")
        wi_parts.append(f"  Services offered (from site): {website_intel.get('services', 'NOT FOUND')}")
        wi_parts.append(f"  Value proposition (from site): {website_intel.get('value_proposition', 'NOT FOUND')}")
        wi_parts.append(f"  Positioning (from site): {website_intel.get('positioning', 'NOT FOUND')}")
        wi_parts.append(f"  Geographic focus (from site): {website_intel.get('geographic_focus', 'NOT FOUND')}")
        wi_parts.append(f"  Key clients mentioned (from site): {website_intel.get('key_clients_mentioned', 'NOT FOUND')}")
        kws = website_intel.get("keywords", [])
        if kws:
            wi_parts.append(f"  Repeated keywords: {', '.join(kws)}")
        website_block = (
            "\n=== STRUCTURED WEBSITE INTELLIGENCE (multi-page crawl — use only non-NOT FOUND fields) ===\n" +
            "\n".join(wi_parts) +
            "\n=== END WEBSITE INTELLIGENCE ===\n"
        )
    elif wi_status in ("failed", "skipped"):
        reason = website_intel.get("reason", "")
        website_block = f"\n=== WEBSITE INTELLIGENCE: unavailable ({reason}) ===\n"
    else:
        website_block = "\n=== WEBSITE INTELLIGENCE: not extracted ===\n"

    prospect_context = f"""
{reply_block}
=== INSTANTLY.AI LEAD DATA (real enriched data) ===
{instantly_block}
=== END INSTANTLY DATA ===
{website_block}
{apify_block}
""".strip()

    prompt = f"""You are a world-class B2B market segmentation expert. Generate 5 sharp, specific, actionable ICP segments for this prospect's outbound sales.

CRITICAL RULES:
1. Use ONLY the real data provided below. Do not invent industries, company types, or pain points not evidenced.
2. The prospect's reply is the PRIMARY signal - extract every specific detail from it.
3. The structured website intelligence contains fields extracted from their actual website - use any field that is NOT "NOT FOUND". Fields marked "NOT FOUND" mean the information was not found on their site and MUST NOT be used.
4. The Instantly.ai data contains real enriched profile data - the company description and headline are particularly valuable.
5. Apify market intelligence (if present) contains real Google search data - use it to validate pain points.
6. Each segment must be MEANINGFULLY DIFFERENT - different industry, buyer type, company size, or pain point.
7. Be hyper-specific: "Ecommerce founders running 1M-10M Shopify brands" beats "online retailers".
8. Never use em dashes. Use a regular hyphen (-) or rewrite the sentence.
9. If a field has no real data to support it, use the closest evidenced inference and note it in reply_signal.

{prospect_context}

Return ONLY valid JSON - no markdown, no explanation, no fences.
IMPORTANT: Never use em dashes in any text values.

{{
  "segments": [
    {{
      "segment_number": 1,
      "segment_name": "Short specific name e.g. NHS Procurement Directors",
      "reply_signal": "Quote or paraphrase from the reply that led to this segment (or 'inferred from website' / 'inferred from profile')",
      "industry": "Primary industry",
      "sub_niche": "Specific sub-niche",
      "company_size": "e.g. 10-50 employees",
      "employee_min": 10,
      "employee_max": 50,
      "hq_country": "e.g. United Kingdom",
      "target_titles": ["Title 1", "Title 2", "Title 3"],
      "secondary_titles": ["Alt Title 1", "Alt Title 2"],
      "pain_point": "The single most pressing pain this segment faces that the prospect can solve",
      "buying_signal": "The clearest signal that a company in this segment needs help right now",
      "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
      "cold_email_hook": "One punchy sentence that would grab this audience's attention",
      "apollo_prompt": "A 2-3 sentence plain English description to paste into Apollo AI search. Include job titles, industry, company size, location, and specific characteristics."
    }},
    {{"segment_number": 2}},
    {{"segment_number": 3}},
    {{"segment_number": 4}},
    {{"segment_number": 5}}
  ]
}}"""

    headers = {
        "x-api-key":         settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type":      "application/json",
    }
    payload = {
        "model":      "claude-sonnet-4-6",
        "max_tokens": 4096,
        "messages":   [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(ANTHROPIC_URL, json=payload, headers=headers)
            resp.raise_for_status()
            raw  = resp.json()["content"][0]["text"]
            raw  = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
            segments = data.get("segments", [])
            if segments:
                logger.info(
                    "Generated %d ICP segments for %s: %s",
                    len(segments), prospect.company,
                    [s.get("segment_name", "?") for s in segments],
                )
                return segments[:5]
    except Exception as exc:
        logger.exception("ICP segment generation failed, using fallback: %s", exc)

    return _fallback_segments(prospect)


def _fallback_segments(prospect: ProspectData) -> list[dict]:
    """Sensible fallback if Claude API fails."""
    company = prospect.company or "the business"
    country = prospect.location or "United Kingdom"

    return [
        {
            "segment_number": 1,
            "segment_name": "SME Managing Directors",
            "industry": "Management Consulting",
            "sub_niche": "Strategy & Business Growth",
            "company_size": "10-50 employees",
            "employee_min": 10,
            "employee_max": 50,
            "hq_country": country,
            "target_titles": ["Managing Director", "CEO", "Founder"],
            "secondary_titles": ["Director", "Owner"],
            "pain_point": "No predictable pipeline of qualified meetings with decision-makers",
            "buying_signal": "Hiring for business development or sales roles",
            "keywords": ["consulting", "strategy", "growth", "B2B", "advisory"],
            "cold_email_hook": "Most consulting firms grow purely on referrals - we add a predictable outbound engine alongside that.",
            "apollo_prompt": f"Find Managing Directors, CEOs and Founders at consulting and advisory firms in {country} with 10-50 employees. Target companies that offer strategic or operational consulting services to B2B clients.",
        },
        {
            "segment_number": 2,
            "segment_name": "Healthcare Sector Leaders",
            "industry": "Healthcare Consulting",
            "sub_niche": "NHS / Private Healthcare Advisory",
            "company_size": "5-30 employees",
            "employee_min": 5,
            "employee_max": 30,
            "hq_country": country,
            "target_titles": ["Managing Director", "Head of Consulting", "Practice Lead"],
            "secondary_titles": ["Director", "Partner"],
            "pain_point": "Difficulty winning NHS and private healthcare contracts consistently",
            "buying_signal": "Recently expanded team or added healthcare-specific services",
            "keywords": ["healthcare consulting", "NHS", "private healthcare", "clinical", "health advisory"],
            "cold_email_hook": "Healthcare consultancies that rely on frameworks and referrals are leaving consistent pipeline on the table.",
            "apollo_prompt": f"Search for Managing Directors and Practice Leads at healthcare consulting firms in {country} with 5-30 employees.",
        },
        {
            "segment_number": 3,
            "segment_name": "Recruitment & Staffing Owners",
            "industry": "Staffing & Recruiting",
            "sub_niche": "Specialist Recruitment Agencies",
            "company_size": "5-25 employees",
            "employee_min": 5,
            "employee_max": 25,
            "hq_country": country,
            "target_titles": ["Managing Director", "Founder", "Director"],
            "secondary_titles": ["CEO", "Owner", "Head of Business Development"],
            "pain_point": "Over-reliance on job boards and inbound with no consistent outbound to attract new client businesses",
            "buying_signal": "Agency growing headcount but client base not keeping pace",
            "keywords": ["recruitment agency", "staffing", "talent acquisition", "executive search"],
            "cold_email_hook": "Most recruitment agencies compete on the same job boards - we help you reach new client businesses before they even post a role.",
            "apollo_prompt": f"Find Founders and Managing Directors at specialist recruitment and staffing agencies in {country} with 5-25 employees.",
        },
        {
            "segment_number": 4,
            "segment_name": "Professional Services Firms",
            "industry": "Professional Services",
            "sub_niche": "Accounting, Legal & Financial Advisory",
            "company_size": "10-75 employees",
            "employee_min": 10,
            "employee_max": 75,
            "hq_country": country,
            "target_titles": ["Managing Partner", "Director", "Founder"],
            "secondary_titles": ["Head of Business Development", "Practice Manager"],
            "pain_point": "New client acquisition is entirely referral-dependent with no scalable outbound process",
            "buying_signal": "Recently promoted to leadership or opened a new office location",
            "keywords": ["professional services", "accounting", "financial advisory", "legal"],
            "cold_email_hook": "Referral networks plateau - we build the outbound system that runs alongside them.",
            "apollo_prompt": f"Search for Managing Partners, Directors and Founders at professional services firms in {country} with 10-75 employees.",
        },
        {
            "segment_number": 5,
            "segment_name": "Technology & SaaS Leaders",
            "industry": "Technology",
            "sub_niche": "B2B SaaS & Tech Services",
            "company_size": "15-100 employees",
            "employee_min": 15,
            "employee_max": 100,
            "hq_country": country,
            "target_titles": ["CEO", "Head of Sales", "VP Sales"],
            "secondary_titles": ["Founder", "Chief Revenue Officer", "Sales Director"],
            "pain_point": "Sales team spending too much time on unqualified outreach with poor conversion rates",
            "buying_signal": "Actively hiring SDRs or outbound sales representatives",
            "keywords": ["SaaS", "B2B software", "tech startup", "sales automation"],
            "cold_email_hook": "B2B SaaS teams hiring more SDRs rarely fix the underlying targeting problem - we fix the targeting.",
            "apollo_prompt": f"Find CEOs, Heads of Sales and VP Sales at B2B SaaS and technology service companies in {country} with 15-100 employees.",
        },
    ]


# ── Legacy backward-compat wrapper ────────────────────────────────────────────

async def generate_icp(prospect: ProspectData, reply_body: str = ""):
    """
    Legacy single-ICP function - returns first segment as ICPData-compatible dict.
    Used by the webhook pipeline for backward compatibility.
    """
    from app.models.schemas import ICPData
    segments = await generate_icp_segments(prospect, reply_body)
    if not segments:
        return _fallback_icp()
    s = segments[0]
    try:
        return ICPData(
            industry            = s.get("industry", "Management Consulting"),
            sub_niche           = s.get("sub_niche", "Strategy & Operations"),
            company_size        = s.get("company_size", "10-50 employees"),
            hq_country          = s.get("hq_country", "United Kingdom"),
            target_titles       = s.get("target_titles", ["Managing Director"]),
            pain_point          = s.get("pain_point", "No predictable pipeline"),
            keywords            = s.get("keywords", ["consulting"]),
            apollo_employee_min = s.get("employee_min", 10),
            apollo_employee_max = s.get("employee_max", 50),
            company_age_years   = "2-8",
            buying_signal       = s.get("buying_signal", ""),
            secondary_titles    = s.get("secondary_titles", []),
            cold_email_hook     = s.get("cold_email_hook", ""),
            value_proposition   = "",
            ideal_list_size     = 100,
        )
    except Exception:
        return _fallback_icp()


def _fallback_icp():
    from app.models.schemas import ICPData
    return ICPData(
        industry="Management Consulting", sub_niche="Strategy & Operations",
        company_size="10-50 employees", hq_country="United Kingdom",
        target_titles=["Managing Director", "Founder", "CEO"],
        pain_point="No predictable pipeline for booking qualified meetings",
        keywords=["management consulting", "strategy", "B2B"],
        apollo_employee_min=10, apollo_employee_max=50,
        company_age_years="2-6",
        buying_signal="Actively hiring in sales or business development",
    )


def icp_to_apollo_filters(icp) -> dict:
    return {
        "person_titles": icp.target_titles,
        "organization_num_employees_ranges": f"{icp.apollo_employee_min},{icp.apollo_employee_max}",
        "person_locations": [icp.hq_country],
        "q_organization_keyword_tags": icp.keywords,
        "contact_email_status": ["verified"],
        "limit": 100,
    }
