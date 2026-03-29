"""
Reply Sentiment Scoring Service

Scores each prospect reply as hot / warm / cold using Claude AI.
Also extracts a one-line reason to show in the CRM.
"""

import logging
import httpx
import json
from app.config import settings

logger = logging.getLogger(__name__)
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


async def score_reply(reply_text: str, prospect_name: str = "", company: str = "") -> dict:
    """
    Score a prospect reply as hot/warm/cold.
    Returns: {"score": "hot|warm|cold", "reason": "...", "emoji": "..."}
    """
    if not reply_text or not reply_text.strip():
        return {"score": "cold", "reason": "No reply text", "emoji": "❄️"}

    # Quick rule-based pre-check for obvious cases
    reply_lower = reply_text.lower()

    # Auto-reply / out of office detection
    auto_reply_signals = ["out of office", "automatic reply", "auto-reply", "on leave",
                          "on holiday", "away from", "not be available", "maternity leave",
                          "extended leave", "annual leave"]
    if any(signal in reply_lower for signal in auto_reply_signals):
        return {"score": "cold", "reason": "Auto-reply / out of office", "emoji": "❄️"}

    # Strong positive signals
    hot_signals = ["yes", "interested", "love to", "let's chat", "book a call",
                   "calendly", "when are you", "tell me more", "send me",
                   "would like to", "can we", "let's speak", "definitely",
                   "sounds good", "perfect timing", "been looking"]
    if any(signal in reply_lower for signal in hot_signals):
        # Still verify with Claude for nuance
        pass

    if not settings.anthropic_api_key:
        # Fallback to rule-based scoring
        if any(s in reply_lower for s in hot_signals):
            return {"score": "hot", "reason": "Expressed clear interest", "emoji": "🔥"}
        return {"score": "warm", "reason": "Replied but intent unclear", "emoji": "☀️"}

    prompt = f"""Score this B2B cold email reply as hot, warm, or cold.

Prospect: {prospect_name} at {company}
Reply: "{reply_text[:500]}"

Rules:
- HOT: Clear buying intent, asks to book a call, wants more info, positive engagement
- WARM: Polite but non-committal, asks for more details without clear intent, general interest
- COLD: Unsubscribe, not interested, auto-reply, out of office, negative

Return ONLY valid JSON, no markdown:
{{"score": "hot|warm|cold", "reason": "One sentence explaining why", "emoji": "🔥|☀️|❄️"}}"""

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 100,
                    "messages": [{"role": "user", "content": prompt}],
                },
                headers={
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"].strip()
            result = json.loads(raw)
            logger.info("Sentiment: %s → %s (%s)", company, result.get("score"), result.get("reason"))
            return result
    except Exception as exc:
        logger.warning("Sentiment scoring failed: %s", exc)
        # Fallback
        if any(s in reply_lower for s in hot_signals):
            return {"score": "hot", "reason": "Expressed interest", "emoji": "🔥"}
        return {"score": "warm", "reason": "Replied — review needed", "emoji": "☀️"}
