"""
Mail inbox/sent reader — Microsoft Graph API.
Requires Mail.Read application permission in Azure AD.
"""

import logging
import httpx
from fastapi import APIRouter, HTTPException, Query
from app.services.outlook import get_messages, get_message_body

router = APIRouter()
logger = logging.getLogger(__name__)

PERMISSION_HINT = (
    "Mail.Read permission required. In Azure Portal → App registrations → "
    "API permissions → Add Microsoft Graph Application permission 'Mail.Read', "
    "then grant admin consent."
)


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
