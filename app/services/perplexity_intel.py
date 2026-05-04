"""
Perplexity.ai Company Research Service

Uses Perplexity Sonar API to research a company and extract structured
business intelligence. This is the primary website intel source — it uses
live web search so it works even when the company website blocks crawlers.

Falls back gracefully: if Perplexity is not configured or fails, the caller
should fall back to the direct website crawler.
"""

import json
import logging
import httpx

from app.config import settings

logger = logging.getLogger(__name__)

PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"
PERPLEXITY_MODEL = "sonar"  # sonar = fast + grounded web search


async def research_company_with_perplexity(
    website_url: str,
    company_name: str = "",
) -> dict | None:
    """
    Use Perplexity Sonar to research a company and return structured intel.

    Returns same dict shape as extract_website_intel():
      status, industry, target_customers, services, value_proposition,
      positioning, keywords, key_clients_mentioned, geographic_focus,
      source_pages, raw_text_chars

    Returns None if Perplexity is not configured, or raises on API error
    (caller should catch and fall back to crawler).
    """
    if not settings.perplexity_api_key:
        return None

    company_ref = company_name or website_url
    query = f"""Research the company "{company_ref}" (website: {website_url}).

Extract the following and return ONLY valid JSON:
{{
  "industry": "The specific industry or sector this company operates in",
  "target_customers": "Who are their customers? List specific customer types, sectors, roles, or company sizes they serve",
  "services": "Comma-separated list of their main services or products",
  "value_proposition": "Their core value proposition — what makes them different or better",
  "positioning": "How they position themselves: niche/broad, premium/budget, specialist/generalist, etc.",
  "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "key_clients_mentioned": "Specific client names, brands, or notable case study subjects — or NOT FOUND",
  "geographic_focus": "Countries, regions, or cities they operate in or target"
}}

Be specific and factual. Use real information from the company's website and public sources.
Return ONLY the JSON object, no markdown fences, no extra text."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                PERPLEXITY_URL,
                headers={
                    "Authorization": f"Bearer {settings.perplexity_api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":    PERPLEXITY_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a B2B company research expert. "
                                "Research companies thoroughly using web search and return structured JSON. "
                                "Be specific and factual — no vague answers."
                            ),
                        },
                        {"role": "user", "content": query},
                    ],
                    "temperature": 0.1,
                    "max_tokens":  800,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw = data["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()

        # Strip any leading/trailing text before/after the JSON object
        start = raw.find("{")
        end   = raw.rfind("}") + 1
        if start >= 0 and end > start:
            raw = raw[start:end]

        parsed = json.loads(raw)

        if "keywords" not in parsed or not isinstance(parsed["keywords"], list):
            parsed["keywords"] = []

        parsed["status"]         = "success"
        parsed["source_pages"]   = ["perplexity_research"]
        parsed["raw_text_chars"] = len(raw)
        parsed["source"]         = "perplexity"

        logger.info(
            "Perplexity intel for '%s': industry=%s, services=%s",
            company_ref,
            str(parsed.get("industry", "?"))[:60],
            str(parsed.get("services",  "?"))[:60],
        )
        return parsed

    except json.JSONDecodeError as exc:
        logger.warning("Perplexity returned non-JSON for %s: %s", company_ref, exc)
        return None
    except Exception as exc:
        logger.warning("Perplexity research failed for %s: %s", company_ref, exc)
        raise
