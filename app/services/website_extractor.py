"""
Website Extraction Service

Multi-page website crawler that extracts structured business intelligence using Claude.

Pages crawled in order:
  1. Homepage
  2. About / About-us
  3. Services / What-we-do / Solutions
  4. Products / Platform
  5. Case studies / Testimonials / Work
  6. Blog / Insights / Resources

STRICT RULES:
  - Returns "NOT FOUND" for any field not explicitly found in page content
  - No inference, no hallucination, no guessing
  - Uses only real crawled content — no synthetic data
"""

import re
import json
import logging
import httpx
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PALM/1.0 research)"}
PAGE_TIMEOUT = 12
MAX_CHARS_PER_PAGE = 4500
MAX_TOTAL_CHARS = 22000

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

PAGE_PATTERNS = [
    ("homepage",     ["/"]),
    ("about",        ["/about", "/about-us", "/who-we-are", "/our-story", "/company", "/the-team"]),
    ("services",     ["/services", "/what-we-do", "/solutions", "/our-services", "/offerings"]),
    ("products",     ["/products", "/product", "/platform", "/software", "/tools"]),
    ("case_studies", [
        "/case-studies", "/case-study", "/clients", "/testimonials",
        "/success-stories", "/work", "/portfolio", "/results", "/our-work",
    ]),
    ("blog",         ["/blog", "/insights", "/resources", "/articles", "/news", "/thought-leadership"]),
]


def _strip_html(html: str) -> str:
    """Remove scripts, styles, nav, footer; strip all remaining tags; collapse whitespace."""
    html = re.sub(r"<style[^>]*>.*?</style>",  " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<nav[^>]*>.*?</nav>",       " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<footer[^>]*>.*?</footer>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<header[^>]*>.*?</header>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"\s+",    " ", html).strip()
    return html[:MAX_CHARS_PER_PAGE]


async def _fetch_page(url: str) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=PAGE_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url, headers=HEADERS)
            if resp.status_code == 200:
                text = _strip_html(resp.text)
                if len(text) > 150:
                    return text
    except Exception as exc:
        logger.debug("Page fetch failed %s: %s", url, exc)
    return None


async def crawl_website(website_url: str) -> dict[str, str]:
    """
    Crawl up to 6 page types from the website.
    Returns dict: page_type -> cleaned text content.
    """
    if not website_url:
        return {}

    base = website_url.rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base

    pages: dict[str, str] = {}
    total_chars = 0

    for page_type, slugs in PAGE_PATTERNS:
        if total_chars >= MAX_TOTAL_CHARS:
            break

        if page_type == "homepage":
            text = await _fetch_page(base + "/")
            if text:
                pages[page_type] = text
                total_chars += len(text)
            continue

        for slug in slugs:
            if total_chars >= MAX_TOTAL_CHARS:
                break
            text = await _fetch_page(base + slug)
            if not text:
                continue
            # Skip pages too similar to homepage (likely a catch-all redirect)
            home_words = set(pages.get("homepage", "").split()[:80])
            page_words = set(text.split()[:80])
            overlap = len(home_words & page_words)
            if home_words and overlap >= 40:
                continue
            pages[page_type] = text
            total_chars += len(text)
            break

    logger.info(
        "Crawled %d page(s) for %s (%d chars total): %s",
        len(pages), base, total_chars, list(pages.keys()),
    )
    return pages


_NOT_FOUND_RESULT = {
    "status":               "failed",
    "reason":               "",
    "industry":             "NOT FOUND",
    "target_customers":     "NOT FOUND",
    "services":             "NOT FOUND",
    "value_proposition":    "NOT FOUND",
    "positioning":          "NOT FOUND",
    "keywords":             [],
    "key_clients_mentioned":"NOT FOUND",
    "geographic_focus":     "NOT FOUND",
    "source_pages":         [],
    "raw_text_chars":       0,
}


async def extract_website_intel(website_url: str, company_name: str = "") -> dict:
    """
    Crawl the website and use Claude to extract structured business intelligence.

    Returns a dict with:
      status, industry, target_customers, services, value_proposition,
      positioning, keywords, key_clients_mentioned, geographic_focus,
      source_pages, raw_text_chars

    All text fields are either explicitly extracted content or exactly "NOT FOUND".
    """
    if not website_url:
        logger.info("No website URL for '%s' — skipping website intel", company_name)
        result = dict(_NOT_FOUND_RESULT)
        result["status"] = "skipped"
        result["reason"] = "No website URL provided"
        return result

    pages = await crawl_website(website_url)

    if not pages:
        logger.warning("No pages crawled for %s (%s)", company_name, website_url)
        result = dict(_NOT_FOUND_RESULT)
        result["reason"] = "Could not fetch any pages from the website"
        return result

    context_parts = []
    for page_type, text in pages.items():
        context_parts.append(
            f"=== {page_type.upper().replace('_', ' ')} ===\n{text[:4000]}"
        )
    all_context = "\n\n".join(context_parts)
    total_chars = sum(len(t) for t in pages.values())

    if not settings.anthropic_api_key:
        result = dict(_NOT_FOUND_RESULT)
        result["status"] = "skipped"
        result["reason"] = "ANTHROPIC_API_KEY not configured"
        result["source_pages"] = list(pages.keys())
        result["raw_text_chars"] = total_chars
        return result

    prompt = f"""Extract structured business intelligence from this website content.

STRICT RULES:
1. Use ONLY text explicitly present in the content below.
2. Write exactly "NOT FOUND" (no quotes) for any field you cannot find clear evidence for.
3. Do NOT infer, guess, or add context not explicitly stated.
4. For target_customers - list only customer types the company explicitly names or describes serving.
5. For keywords - extract actual terms that appear repeatedly across multiple pages.
6. Never write "likely", "probably", "appears to be", or similar hedging language.
7. Quote or closely paraphrase actual language from the site, don't rewrite in your own words.

Company: {company_name or "Unknown"}

WEBSITE CONTENT ({len(pages)} pages crawled: {', '.join(pages.keys())}):
{all_context[:18000]}

Return ONLY valid JSON (no markdown fences, no extra text):
{{
  "industry": "The specific industry stated on the site — exact words, or NOT FOUND",
  "target_customers": "Explicitly named customer types, sectors, or roles they state they serve — or NOT FOUND",
  "services": "Comma-separated list of services/products they explicitly offer — or NOT FOUND",
  "value_proposition": "Their stated core promise or differentiator in their own words — or NOT FOUND",
  "positioning": "How they explicitly position themselves (niche/broad, budget/premium, specialist/generalist, etc.) — or NOT FOUND",
  "keywords": ["repeated_term1", "repeated_term2", "repeated_term3"],
  "key_clients_mentioned": "Specific client names, brand names, or case study subjects explicitly named — or NOT FOUND",
  "geographic_focus": "Countries, regions, or cities explicitly stated as their market — or NOT FOUND"
}}"""

    try:
        async with httpx.AsyncClient(timeout=50) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-6",
                    "max_tokens": 1200,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            raw  = resp.json()["content"][0]["text"].strip()
            raw  = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
    except Exception as exc:
        logger.exception("Website intel extraction failed for %s: %s", website_url, exc)
        result = dict(_NOT_FOUND_RESULT)
        result["status"] = "extraction_failed"
        result["reason"] = str(exc)
        result["source_pages"] = list(pages.keys())
        result["raw_text_chars"] = total_chars
        return result

    data["status"]          = "success"
    data["source_pages"]    = list(pages.keys())
    data["raw_text_chars"]  = total_chars
    if "keywords" not in data or not isinstance(data["keywords"], list):
        data["keywords"] = []

    logger.info(
        "Website intel extracted for '%s': %d page(s), industry=%s, services=%s",
        company_name, len(pages),
        str(data.get("industry", "?"))[:60],
        str(data.get("services", "?"))[:60],
    )
    return data
