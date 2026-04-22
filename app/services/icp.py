"""
ICP Generation Service

Generates 5 distinct ICP segments using Claude AI.
Each segment represents a different audience the prospect's business could target,
with a ready-to-use Apollo.io search prompt for manual list building.

No Apollo API required — Claude does the intelligence, you do the search manually.
"""

import json
import logging
import httpx
from app.config import settings
from app.models.schemas import ProspectData

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


async def scrape_website(url: str) -> str:
    """Fetch and extract readable text from the prospect's website."""
    if not url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            text = resp.text
            import re
            text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
            text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            return text[:5000]
    except Exception as exc:
        logger.warning("Website scrape failed for %s: %s", url, exc)
        return ""


async def generate_icp_segments(prospect: ProspectData, reply_body: str = "") -> list[dict]:
    """
    Generate 5 distinct ICP segments for the prospect's business.

    Data sources used (in order of priority):
      1. Prospect's reply text          — primary signal
      2. Prospect profile (Instantly)   — job title, industry, location, etc.
      3. Prospect's website             — what they do / who they serve
      4. Apify Google Search            — external market intelligence (if APIFY_API_TOKEN set)

    Falls back to sensible defaults if the Claude API call fails.
    """
    from app.services.apify import enrich_icp_context

    # ── 1. Scrape website ────────────────────────────────────────────────────
    website_content = await scrape_website(prospect.website or "")
    if website_content:
        logger.info("Website scraped for %s (%d chars)", prospect.company, len(website_content))

    # ── 2. Apify external enrichment ─────────────────────────────────────────
    # Extract keywords from the reply to focus the searches
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
        industry       = prospect.industry or "",
        reply_keywords = reply_keywords,
        location       = prospect.location or "United Kingdom",
    )

    # ── 3. Build Apify context block ─────────────────────────────────────────
    apify_block = ""
    if apify_intel:
        parts = []
        if apify_intel.get("client_signals"):
            parts.append("WHO THEY SERVE (from Google search):\n" +
                         "\n".join(apify_intel["client_signals"]))
        if apify_intel.get("pain_points"):
            parts.append("SECTOR PAIN POINTS (from Google search):\n" +
                         "\n".join(apify_intel["pain_points"]))
        if apify_intel.get("market_context"):
            parts.append("MARKET CONTEXT & BUYING SIGNALS (from Google search):\n" +
                         "\n".join(apify_intel["market_context"]))
        if parts:
            apify_block = "\n\n=== EXTERNAL MARKET INTELLIGENCE (Apify) ===\n" + \
                          "\n\n".join(parts) + \
                          "\n=== END EXTERNAL INTELLIGENCE ===\n"
            logger.info("Apify context injected for %s (%d chars)", prospect.company, len(apify_block))

    # ── 4. Build reply signal block ──────────────────────────────────────────
    reply_signals = ""
    if reply_body and reply_body.strip() and reply_body != "No reply text captured":
        reply_signals = f"""
=== CRITICAL: PROSPECT'S REPLY — READ THIS FIRST ===
This is the most important signal. Extract any mentions of:
- Specific industries or sectors they work in
- Types of clients or customers they serve
- Problems or challenges they mentioned
- Geographic focus
- Company types they target
- Any specific names, sectors, or niches mentioned

REPLY TEXT:
"{reply_body}"
=== END OF REPLY ===
"""

    # Build rich Instantly data block — include every non-empty field
    instantly_fields = []
    if prospect.name:        instantly_fields.append(f"  Name: {prospect.name}")
    if prospect.job_title:   instantly_fields.append(f"  Job title: {prospect.job_title}")
    if prospect.job_level:   instantly_fields.append(f"  Seniority: {prospect.job_level}")
    if prospect.department:  instantly_fields.append(f"  Department: {prospect.department}")
    if prospect.company:     instantly_fields.append(f"  Company: {prospect.company}")
    if prospect.website or prospect.domain:
        instantly_fields.append(f"  Website: {prospect.website or prospect.domain}")
    if prospect.linkedin:    instantly_fields.append(f"  LinkedIn: {prospect.linkedin}")
    if prospect.location:    instantly_fields.append(f"  Location: {prospect.location}")
    if prospect.headcount:   instantly_fields.append(f"  Company size: {prospect.headcount} employees")
    if prospect.industry:    instantly_fields.append(f"  Industry: {prospect.industry}")
    if prospect.sub_industry:instantly_fields.append(f"  Sub-industry: {prospect.sub_industry}")
    if prospect.headline:    instantly_fields.append(f"  Headline/bio: {prospect.headline}")
    if prospect.description: instantly_fields.append(f"  Company description: {prospect.description[:800]}")

    instantly_block = "\n".join(instantly_fields) if instantly_fields else "  (no Instantly data available)"

    prospect_context = f"""
{reply_signals}
=== INSTANTLY.AI LEAD DATA (real enriched data — use every field) ===
{instantly_block}
=== END INSTANTLY DATA ===

=== WEBSITE CONTENT (live scraped) ===
{website_content if website_content else 'Could not scrape. Use the Instantly data and reply above.'}
=== END WEBSITE CONTENT ===
{apify_block}
""".strip()

    prompt = f"""You are a world-class B2B market segmentation expert. Your job is to generate 5 sharp, specific, actionable ICP segments for this prospect's outbound sales.

CRITICAL RULES:
1. Use ONLY the real data provided below. Do not invent industries, company types, or pain points not evidenced in the data.
2. The prospect's reply is the PRIMARY signal - extract every specific detail from it (sector names, client types, problems mentioned, locations).
3. The Instantly.ai data contains real enriched profile data - the company description and headline are particularly valuable. Use them directly.
4. The website content shows exactly what services they offer and who they serve. Mine it for specific client types and use cases.
5. Apify market intelligence (if present) contains real Google search data - use it to validate and sharpen pain points.
6. Each segment must be MEANINGFULLY DIFFERENT - different industry, buyer type, company size, or pain point.
7. Be hyper-specific: "Ecommerce founders running $1M-$10M Shopify brands" beats "online retailers".
8. Never use em dashes (—). Use a regular hyphen (-) or rewrite the sentence.
9. If a field has no real data to support it, use the closest evidenced inference and note it in reply_signal.

{prospect_context}

Return ONLY valid JSON - no markdown, no explanation, no fences. Use this exact structure:
IMPORTANT: Never use em dashes (—) in any text values. Use a regular hyphen (-) or rewrite the sentence instead.

{{
  "segments": [
    {{
      "segment_number": 1,
      "segment_name": "Short specific name e.g. NHS Procurement Directors",
      "reply_signal": "Quote or paraphrase from the reply that led you to this segment (or 'inferred from website' if not in reply)",
      "industry": "Primary industry",
      "sub_niche": "Specific sub-niche",
      "company_size": "e.g. 10-50 employees",
      "employee_min": 10,
      "employee_max": 50,
      "hq_country": "e.g. United Kingdom",
      "target_titles": ["Title 1", "Title 2", "Title 3"],
      "secondary_titles": ["Alt Title 1", "Alt Title 2"],
      "pain_point": "The single most pressing pain this segment faces that your prospect can solve",
      "buying_signal": "The clearest signal that a company in this segment needs help right now",
      "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
      "cold_email_hook": "One punchy sentence that would grab this audience's attention",
      "apollo_prompt": "A 2-3 sentence plain English description to paste into Apollo AI search. Include job titles, industry, company size, location, and specific characteristics of this segment."
    }},
    {{segment 2}},
    {{segment 3}},
    {{segment 4}},
    {{segment 5}}
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
            raw = resp.json()["content"][0]["text"]
            raw = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
            segments = data.get("segments", [])
            if segments:
                logger.info(
                    "Generated %d ICP segments for %s: %s",
                    len(segments), prospect.company,
                    [s.get("segment_name","?") for s in segments],
                )
                return segments[:5]
    except Exception as exc:
        logger.exception("ICP segment generation failed, using fallback: %s", exc)

    # Fallback segments
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
            "cold_email_hook": "Most consulting firms grow purely on referrals — we add a predictable outbound engine alongside that.",
            "apollo_prompt": f"Find Managing Directors, CEOs and Founders at consulting and advisory firms in {country} with 10-50 employees. Target companies that offer strategic or operational consulting services to B2B clients. Look for firms that are growing but don't have a structured outbound sales process.",
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
            "apollo_prompt": f"Search for Managing Directors and Practice Leads at healthcare consulting firms in {country} with 5-30 employees. Target boutique consultancies specialising in NHS procurement, private healthcare strategy, or clinical operations. Prioritise firms that have recently hired or expanded their team.",
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
            "pain_point": "Over-reliance on job boards and inbound — no consistent outbound to attract new client businesses",
            "buying_signal": "Agency growing headcount but client base not keeping pace",
            "keywords": ["recruitment agency", "staffing", "talent acquisition", "executive search", "headhunting"],
            "cold_email_hook": "Most recruitment agencies compete on the same job boards — we help you reach new client businesses before they even post a role.",
            "apollo_prompt": f"Find Founders and Managing Directors at specialist recruitment and staffing agencies in {country} with 5-25 employees. Focus on boutique agencies in niche sectors like healthcare, tech, or finance. Target those who place permanent candidates and rely heavily on repeat clients rather than having a structured business development function.",
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
            "keywords": ["professional services", "accounting", "financial advisory", "legal", "audit"],
            "cold_email_hook": "Referral networks plateau — we build the outbound system that runs alongside them.",
            "apollo_prompt": f"Search for Managing Partners, Directors and Founders at professional services firms in {country} with 10-75 employees. Target accountancy practices, financial advisory firms, and legal firms that serve B2B clients. Look for firms that have grown to a point where referrals alone are no longer sufficient to hit growth targets.",
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
            "keywords": ["SaaS", "B2B software", "tech startup", "sales automation", "revenue growth"],
            "cold_email_hook": "B2B SaaS teams hiring more SDRs rarely fix the underlying targeting problem — we fix the targeting.",
            "apollo_prompt": f"Find CEOs, Heads of Sales and VP Sales at B2B SaaS and technology service companies in {country} with 15-100 employees. Target companies that sell to other businesses and have a dedicated sales team. Prioritise those actively hiring in sales or business development roles, which signals they're trying to scale outbound.",
        },
    ]


# Keep backward compatibility — single ICP for pipeline use
async def generate_icp(prospect: ProspectData, reply_body: str = ""):
    """
    Legacy single-ICP function — returns the first segment as an ICPData-compatible dict.
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
