"""
Swipe File Context Builder

Shared helper that loads swipe files from the database and formats them
as a knowledge base block for any AI agent's system prompt.
"""

import logging
from app import db

logger = logging.getLogger(__name__)


async def build_swipe_context(limit: int = 10, chars_per_file: int = 1500) -> str:
    """
    Load up to `limit` swipe files and return a formatted context block
    ready to be injected into any agent's prompt.

    Returns empty string if no swipe files exist.
    """
    try:
        files = await db.list_swipe_files()
    except Exception as exc:
        logger.debug("Could not load swipe files: %s", exc)
        return ""

    if not files:
        return ""

    parts = []
    for f in files[:limit]:
        content = (f.get("content") or "").strip()
        if not content:
            continue
        snippet = content[:chars_per_file]
        truncated = "…" if len(content) > chars_per_file else ""
        cat = f.get("category", "general").upper().replace("_", " ")
        title = f.get("title", "Untitled")
        parts.append(f"[{cat}] {title}\n{snippet}{truncated}")

    if not parts:
        return ""

    return (
        "KNOWLEDGE BASE — SWIPE FILES\n"
        "(Use these as reference material, style inspiration, and strategic guidance "
        "when building ICPs, selecting leads, and drafting emails.)\n\n"
        + "\n\n---\n\n".join(parts)
    )
