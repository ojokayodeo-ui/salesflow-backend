"""
Mail inbox/sent reader + email open tracking — Microsoft Graph API.
Requires Mail.Read application permission in Azure AD.
"""

import asyncio
import logging
import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from app.services.outlook import get_messages, get_message_body, get_access_token, GRAPH_BASE
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

PERMISSION_HINT = (
    "Mail.Read permission required. In Azure Portal → App registrations → "
    "API permissions → Add Microsoft Graph Application permission 'Mail.Read', "
    "then grant admin consent."
)

# 1×1 transparent GIF
TRANSPARENT_GIF = bytes([
    0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,0x80,0x00,0x00,
    0xFF,0xFF,0xFF,0x00,0x00,0x00,0x21,0xF9,0x04,0x00,0x00,0x00,0x00,
    0x00,0x2C,0x00,0x00,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,
    0x44,0x01,0x00,0x3B,
])


# ── Inbox / Sent ─────────────────────────────────────────────────────────────

@router.get("/inbox")
async def inbox(top: int = Query(50, le=100)):
    try:
        messages = await get_messages("inbox", top)
        return {"messages": messages, "count": len(messages)}
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise HTTPException(status_code=403, detail=PERMISSION_HINT)
        raise HTTPException(status_code=502, detail=f"Graph API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Failed to fetch inbox")
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/sent")
async def sent(top: int = Query(50, le=100)):
    try:
        messages = await get_messages("sent", top)
        return {"messages": messages, "count": len(messages)}
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise HTTPException(status_code=403, detail=PERMISSION_HINT)
        raise HTTPException(status_code=502, detail=f"Graph API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Failed to fetch sent items")
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/message/{message_id}")
async def get_message(message_id: str):
    try:
        msg = await get_message_body(message_id)
        return {"message": msg}
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise HTTPException(status_code=403, detail=PERMISSION_HINT)
        raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to fetch message %s", message_id)
        raise HTTPException(status_code=502, detail=str(exc))


# ── Email Open Tracking ───────────────────────────────────────────────────────

@router.get("/track/{token}")
async def track_open(token: str):
    """Tracking pixel — records when recipient opens the email."""
    try:
        opened = await db.record_email_open(token)
        if opened:
            logger.info("Email opened — token %s", token[:8])
    except Exception as exc:
        logger.warning("Failed to record email open: %s", exc)
    return Response(
        content=TRANSPARENT_GIF,
        media_type="image/gif",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, private",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ── Deal-Matched Mail Thread ──────────────────────────────────────────────────

@router.get("/debug-thread")
async def debug_mail_thread(email: str = Query(...)):
    """Debug: returns raw Graph API responses for both inbox search and sent search."""
    if not settings.ms_sender_email:
        return {"error": "MS_SENDER_EMAIL not set"}
    try:
        token = await get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        inbox_url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages"
        sent_url  = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sentItems"

        async with httpx.AsyncClient(timeout=30) as client:
            r1 = await client.get(inbox_url, headers=headers, params={
                "$search": f'"from:{email}"',
                "$select": "id,subject,receivedDateTime,bodyPreview,isRead",
                "$top": 5,
            })
            r2 = await client.get(sent_url, headers=headers, params={
                "$filter": f"toRecipients/any(r:r/emailAddress/address eq '{email}')",
                "$select": "id,subject,sentDateTime,bodyPreview",
                "$top": 5,
            })

        return {
            "sender_mailbox": settings.ms_sender_email,
            "prospect_email": email,
            "inbox_status": r1.status_code,
            "inbox_body": r1.json() if r1.headers.get("content-type","").startswith("application/json") else r1.text[:500],
            "sent_status": r2.status_code,
            "sent_body": r2.json() if r2.headers.get("content-type","").startswith("application/json") else r2.text[:500],
        }
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/for-deal")
async def mail_for_deal(email: str = Query(...), top: int = Query(20, le=50)):
    """
    Fetch inbox + sent messages matching a prospect.
    Inbox: KQL $search "from:{email}" (supports full-text).
    SentItems: OData $filter on toRecipients (KQL "to:" not supported on sentItems).
    Requires Mail.Read.
    """
    if not settings.ms_sender_email:
        raise HTTPException(status_code=503, detail="MS_SENDER_EMAIL not configured")
    try:
        token = await get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        inbox_url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages"
        sent_url  = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sentItems"

        async with httpx.AsyncClient(timeout=30) as client:
            inbox_resp, sent_resp = await asyncio.gather(
                client.get(inbox_url, headers=headers, params={
                    "$search": f'"from:{email}"',
                    "$select": "id,subject,receivedDateTime,bodyPreview,isRead,conversationId",
                    "$top": top,
                }),
                # sentItems: KQL "to:" not supported — use OData $filter; no $orderby (incompatible with $filter on this endpoint)
                client.get(sent_url, headers=headers, params={
                    "$filter": f"toRecipients/any(r:r/emailAddress/address eq '{email}')",
                    "$select": "id,subject,sentDateTime,bodyPreview,conversationId",
                    "$top": top,
                }),
            )

        if inbox_resp.status_code not in (200, 206):
            logger.warning("Inbox search error %s: %s", inbox_resp.status_code, inbox_resp.text[:400])
        if sent_resp.status_code not in (200, 206):
            logger.warning("Sent search error %s: %s", sent_resp.status_code, sent_resp.text[:400])

        received = [
            {"direction": "received", "date": m.get("receivedDateTime"), **m}
            for m in (inbox_resp.json().get("value", []) if inbox_resp.status_code == 200 else [])
        ]
        sent_msgs = [
            {"direction": "sent", "date": m.get("sentDateTime"), **m}
            for m in (sent_resp.json().get("value", []) if sent_resp.status_code == 200 else [])
        ]

        all_msgs = sorted(
            received + sent_msgs,
            key=lambda m: m.get("date") or "",
            reverse=True,
        )

        return {
            "messages": all_msgs,
            "received_count": len(received),
            "sent_count": len(sent_msgs),
            "inbox_status": inbox_resp.status_code,
            "sent_status": sent_resp.status_code,
        }

    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise HTTPException(status_code=403, detail=PERMISSION_HINT)
        raise HTTPException(status_code=502, detail=f"Graph API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Failed to fetch mail for deal %s", email)
        raise HTTPException(status_code=502, detail=str(exc))


# ── Deal Email Metrics ────────────────────────────────────────────────────────

@router.get("/metrics/{deal_id}")
async def deal_metrics(deal_id: str):
    """Return tracked email send/open metrics for a deal."""
    metrics = await db.get_email_metrics(deal_id)
    return metrics


@router.get("/metrics-bulk")
async def deal_metrics_bulk(deal_ids: str = Query(...)):
    """
    Return metrics for multiple deals at once.
    deal_ids: comma-separated deal IDs.
    """
    ids = [d.strip() for d in deal_ids.split(",") if d.strip()]
    if not ids:
        return {"metrics": {}}
    results = {}
    for did in ids[:50]:
        try:
            results[did] = await db.get_email_metrics(did)
        except Exception:
            results[did] = {}
    return {"metrics": results}
