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

@router.get("/for-deal/{prospect_email}")
async def mail_for_deal(prospect_email: str, top: int = Query(20, le=50)):
    """
    Fetch inbox + sent messages matching a prospect's email address via Graph filter.
    Requires Mail.Read.
    """
    if not settings.ms_sender_email:
        raise HTTPException(status_code=503, detail="MS_SENDER_EMAIL not configured")
    try:
        token = await get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        safe_email = prospect_email.replace("'", "''")

        inbox_url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages"
        inbox_params = {
            "$filter": f"from/emailAddress/address eq '{safe_email}'",
            "$select": "id,subject,receivedDateTime,bodyPreview,isRead,conversationId",
            "$top": top,
            "$orderby": "receivedDateTime desc",
        }

        sent_url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sentItems"
        sent_params = {
            "$filter": f"toRecipients/any(r: r/emailAddress/address eq '{safe_email}')",
            "$select": "id,subject,sentDateTime,bodyPreview,conversationId",
            "$top": top,
            "$orderby": "sentDateTime desc",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            inbox_resp, sent_resp = await asyncio.gather(
                client.get(inbox_url, headers=headers, params=inbox_params),
                client.get(sent_url, headers=headers, params=sent_params),
            )
            inbox_resp.raise_for_status()
            sent_resp.raise_for_status()

        received = [
            {"direction": "received", "date": m.get("receivedDateTime"), **m}
            for m in inbox_resp.json().get("value", [])
        ]
        sent_msgs = [
            {"direction": "sent", "date": m.get("sentDateTime"), **m}
            for m in sent_resp.json().get("value", [])
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
        }

    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise HTTPException(status_code=403, detail=PERMISSION_HINT)
        raise HTTPException(status_code=502, detail=f"Graph API error: {exc.response.status_code}")
    except Exception as exc:
        logger.exception("Failed to fetch mail for deal %s", prospect_email)
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
