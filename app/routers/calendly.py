"""
Calendly Webhook Router

Listens for Calendly booking events and auto-updates CRM deal stages.

Events handled:
  invitee.created   → advance deal to 'meeting'
  invitee.canceled  → revert deal to 'delivered'

Setup:
  1. Calendly → Integrations → Webhooks → New Webhook
  2. URL: https://<your-railway-app>/api/calendly/webhook
  3. Events: invitee.created, invitee.canceled
  4. Copy the signing key → set CALENDLY_SIGNING_KEY env var on Railway

Signing key verification is skipped if CALENDLY_SIGNING_KEY is not set
(safe for initial setup / testing).
"""

import hashlib
import hmac
import logging
import os

from fastapi import APIRouter, Header, HTTPException, Request
from app.services import database as db

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Signature verification ───────────────────────────────────────────────────

def _verify_signature(body: bytes, calendly_webhook_signature: str | None) -> None:
    """
    Calendly signs webhooks with HMAC-SHA256.
    Header format: "t=<timestamp>,v1=<signature>"
    Skip verification if CALENDLY_SIGNING_KEY is not configured.
    """
    signing_key = os.environ.get("CALENDLY_SIGNING_KEY", "")
    if not signing_key:
        return  # not configured — allow all (dev/testing)

    if not calendly_webhook_signature:
        raise HTTPException(status_code=401, detail="Missing Calendly-Webhook-Signature header")

    # Parse "t=1234567890,v1=abcdef..."
    parts = dict(p.split("=", 1) for p in calendly_webhook_signature.split(",") if "=" in p)
    timestamp = parts.get("t", "")
    signature = parts.get("v1", "")

    if not timestamp or not signature:
        raise HTTPException(status_code=401, detail="Malformed Calendly-Webhook-Signature header")

    # Reconstruct the signed payload: "<timestamp>.<raw_body>"
    signed_payload = f"{timestamp}.".encode() + body
    expected = hmac.new(signing_key.encode(), signed_payload, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_email(payload: dict) -> str:
    """Pull invitee email from Calendly v2 payload structure."""
    # v2 shape: payload.invitee.email
    invitee = payload.get("invitee") or {}
    email = invitee.get("email", "").strip().lower()
    if email:
        return email

    # Fallback: some older payloads nest differently
    questions = payload.get("questions_and_answers") or []
    for qa in questions:
        if "email" in (qa.get("question") or "").lower():
            return (qa.get("answer") or "").strip().lower()

    return ""


def _extract_event_name(payload: dict) -> str:
    """Pull event/meeting name for logging."""
    event_type = payload.get("event_type") or {}
    return event_type.get("name", "meeting")


# ── Webhook endpoint ─────────────────────────────────────────────────────────

@router.post("/webhook")
async def calendly_webhook(
    request: Request,
    calendly_webhook_signature: str | None = Header(default=None),
):
    """
    Receive Calendly webhook events.

    invitee.created  → deal stage set to 'meeting'
    invitee.canceled → deal stage reverted to 'delivered'
    """
    body = await request.body()

    # Verify signature (no-op if CALENDLY_SIGNING_KEY not set)
    _verify_signature(body, calendly_webhook_signature)

    try:
        import json
        data = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event     = data.get("event", "")
    payload   = data.get("payload") or {}

    logger.info("Calendly webhook received: event=%s", event)

    # ── invitee.created — booking confirmed ──────────────────────────────────
    if event == "invitee.created":
        email = _extract_email(payload)
        if not email:
            logger.warning("Calendly invitee.created: no email in payload")
            return {"received": True, "processed": False, "reason": "no email in payload"}

        deal = await db.get_deal_by_email(email)
        if not deal:
            logger.info("Calendly booking: no CRM deal found for %s", email)
            return {"received": True, "processed": False, "reason": "no deal found for email"}

        prev_stage = deal.get("stage", "")
        await db.advance_deal_stage(deal["id"], "meeting")
        await db.update_last_activity(deal["id"])

        event_name = _extract_event_name(payload)
        logger.info(
            "Calendly booking confirmed: %s (%s) — %s → meeting [%s]",
            deal["name"], email, prev_stage, event_name,
        )
        return {
            "received":   True,
            "processed":  True,
            "event":      event,
            "deal_id":    deal["id"],
            "deal_name":  deal["name"],
            "company":    deal.get("company", ""),
            "prev_stage": prev_stage,
            "new_stage":  "meeting",
        }

    # ── invitee.canceled — booking cancelled ─────────────────────────────────
    if event == "invitee.canceled":
        email = _extract_email(payload)
        if not email:
            logger.warning("Calendly invitee.canceled: no email in payload")
            return {"received": True, "processed": False, "reason": "no email in payload"}

        deal = await db.get_deal_by_email(email)
        if not deal:
            logger.info("Calendly cancellation: no CRM deal found for %s", email)
            return {"received": True, "processed": False, "reason": "no deal found for email"}

        # Only revert if the deal is currently at 'meeting' — don't touch won/lost deals
        if deal.get("stage") == "meeting":
            await db.advance_deal_stage(deal["id"], "delivered")
            await db.update_last_activity(deal["id"])
            logger.info(
                "Calendly booking cancelled: %s (%s) — meeting → delivered",
                deal["name"], email,
            )
            return {
                "received":  True,
                "processed": True,
                "event":     event,
                "deal_id":   deal["id"],
                "new_stage": "delivered",
            }
        else:
            logger.info(
                "Calendly cancellation for %s — deal stage is '%s', not reverting",
                email, deal.get("stage"),
            )
            return {
                "received":  True,
                "processed": False,
                "reason":    f"deal stage is '{deal.get('stage')}', not reverting",
            }

    # ── All other events — acknowledge and ignore ─────────────────────────────
    logger.info("Calendly event '%s' not handled — acknowledged", event)
    return {"received": True, "processed": False, "reason": f"event '{event}' not handled"}


# ── Health / test endpoint ────────────────────────────────────────────────────

@router.get("/status")
async def calendly_status():
    """Quick check that the Calendly router is mounted and signing key is configured."""
    signing_key_set = bool(os.environ.get("CALENDLY_SIGNING_KEY", ""))
    return {
        "status":          "ok",
        "signing_key_set": signing_key_set,
        "webhook_url":     "/api/calendly/webhook",
        "events_handled":  ["invitee.created", "invitee.canceled"],
    }
