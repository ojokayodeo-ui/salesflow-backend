"""
Apify Integration Service

Uses Apify's Google Search Scraper actor to gather external market intelligence
before ICP segments are generated. This grounds the ICP in real, current data
from multiple sources rather than just the prospect's website alone.

Three searches are run per prospect:
  1. What the prospect's company actually does / who they serve (case studies, clients)
  2. Pain points and challenges in their target industry right now
  3. Market context and buying signals for their sector

Requires: APIFY_API_TOKEN environment variable
Actor used: apify/google-search-scraper (pay-per-result, very low cost)
"""

import logging
import re
import httpx
from app.config import settings

logger = logging.getLogger(__name__)

APIFY_BASE     = "https://api.apify.com/v2"
ACTOR_ID       = "apify~google-search-scraper"
REQUEST_TIMEOUT = 90  # seconds — actor can take up to ~60s on cold start


async def _run_google_search(queries: list[str], results_per_page: int = 5) -> list[dict]:
    """
    Run Apify Google Search Scraper synchronously and return raw dataset items.
    Each item corresponds to one query and contains organicResults, etc.
    Returns [] on any failure so callers always get a safe fallback.
    """
    if not settings.apify_api_token:
        return []

    actor_input = {
        "queries":               "\n".join(queries),
        "maxPagesPerQuery":      1,
        "resultsPerPage":        results_per_page,
        "mobileResults":         False,
        "languageCode":          "en",
        "countryCode":           "gb",
        "saveHtml":              False,
        "saveMarkdown":          False,
        "includeUnfilteredResults": False,
    }

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.post(
                f"{APIFY_BASE}/acts/{ACTOR_ID}/run-sync-gets-dataset-items",
                params={"token": settings.apify_api_token},
                json=actor_input,
            )
            resp.raise_for_status()
            data = resp.json()
            # Actor returns a list of result objects directly
            return data if isinstance(data, list) else []
    except httpx.HTTPStatusError as exc:
        logger.warning("Apify actor error %s: %s", exc.response.status_code, exc.response.text[:300])
        return []
    except Exception as exc:
        logger.warning("Apify search failed: %s", exc)
        return []


def _clean_text(text: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_snippets(raw_results: list[dict], max_per_query: int = 4) -> list[str]:
    """
    Pull clean title + snippet strings from Apify Google Search results.
    Skips low-quality results (too short, no snippet).
    """
    snippets = []
    for item in raw_results:
        organic = item.get("organicResults", []) if isinstance(item, dict) else []
        count   = 0
        for r in organic:
            if count >= max_per_query:
                break
            title   = _clean_text(r.get("title", ""))
            snippet = _clean_text(r.get("description", "") or r.get("snippet", ""))
            if snippet and len(snippet) > 40:
                snippets.append(f"• {title} — {snippet}")
                count += 1
    return snippets


async def enrich_icp_context(
    company_name: str,
    domain: str,
    industry: str,
    reply_keywords: list[str],
    location: str = "United Kingdom",
) -> dict:
    """
    Gather external market intelligence to enrich ICP generation.

    Returns a dict with:
      - client_signals:    what the prospect's company does / who they serve
      - pain_points:       sector pain points from news/research
      - market_context:    market trends, buying signals for their industry
      - queries_used:      the exact queries that were run (for transparency)

    Returns {} if APIFY_API_TOKEN is not set or all queries fail.
    """
    if not settings.apify_api_token:
        logger.info("APIFY_API_TOKEN not set — ICP will use website + reply data only")
        return {}

    # Build focused search queries
    sector = industry or " ".join(reply_keywords[:3]) or "B2B professional services"
    company_q = company_name.replace('"', '') if company_name else ""

    queries = [
        f'"{company_q}" clients OR customers OR "case study" OR "we work with"',
        f'{sector} "pain points" OR challenges OR problems B2B 2024 2025',
        f'{sector} market {location} outbound sales buying signals 2024 2025',
    ]

    logger.info("Apify enrichment — running %d queries for '%s'", len(queries), company_name)

    raw = await _run_google_search(queries, results_per_page=5)

    if not raw:
        logger.info("Apify returned no results for '%s'", company_name)
        return {}

    # Split results by query index
    def _get_query_snippets(idx: int) -> list[str]:
        item = raw[idx] if idx < len(raw) else {}
        organic = item.get("organicResults", []) if isinstance(item, dict) else []
        out = []
        for r in organic[:4]:
            snippet = _clean_text(r.get("description", "") or r.get("snippet", ""))
            title   = _clean_text(r.get("title", ""))
            if snippet and len(snippet) > 40:
                out.append(f"• {title} — {snippet}")
        return out

    client_signals  = _get_query_snippets(0)
    pain_points     = _get_query_snippets(1)
    market_context  = _get_query_snippets(2)

    total = len(client_signals) + len(pain_points) + len(market_context)
    logger.info(
        "Apify enrichment complete for '%s': %d snippets "
        "(clients=%d, pain_points=%d, market=%d)",
        company_name, total,
        len(client_signals), len(pain_points), len(market_context),
    )

    if total == 0:
        return {}

    return {
        "client_signals": client_signals,
        "pain_points":    pain_points,
        "market_context": market_context,
        "queries_used":   queries,
    }
