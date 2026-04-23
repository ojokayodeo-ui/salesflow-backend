"""
Microsoft Outlook Email Service
Uses Microsoft Graph API with OAuth2 client credentials flow.

Setup steps:
  1. Go to https://portal.azure.com → Azure Active Directory → App registrations
  2. Create a new app registration
  3. Under "API permissions" add:
       Microsoft Graph → Application permissions → Mail.Send
  4. Grant admin consent
  5. Under "Certificates & secrets" create a client secret
  6. Copy Tenant ID, Client ID, Client Secret into your .env file
"""

import base64
import logging
import secrets
import httpx
from app.config import settings

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


async def get_access_token() -> str:
    """
    Obtain a short-lived access token from Azure AD using
    the client credentials (app-only) flow.
    """
    url = TOKEN_URL.format(tenant_id=settings.ms_tenant_id)
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     settings.ms_client_id,
        "client_secret": settings.ms_client_secret,
        "scope":         "https://graph.microsoft.com/.default",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url, data=payload)
        response.raise_for_status()
        return response.json()["access_token"]


def _text_to_html(text: str, tracking_pixel_url: str | None = None) -> str:
    """Convert plain text email body to HTML, optionally appending a tracking pixel."""
    html = "<p>" + text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\r\n", "\n").replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"
    if tracking_pixel_url:
        html += f'<img src="{tracking_pixel_url}" width="1" height="1" style="display:none;border:0" alt="" />'
    return html


def _build_message(
    to_email:    str,
    to_name:     str,
    from_name:   str,
    subject:     str,
    body:        str,
    csv_data:    str | None = None,
    csv_filename: str = "leads_100.csv",
    tracking_pixel_url: str | None = None,
    extra_attachments: list[dict] | None = None,
) -> dict:
    """
    Construct the Graph API sendMail message object.
    Attaches the CSV as a base64 file attachment when csv_data is provided.
    extra_attachments: list of {"name": str, "content_type": str, "content_bytes": str (base64)}
    Always sends as HTML so tracking pixel works.
    """
    html_body = _text_to_html(body, tracking_pixel_url)
    message: dict = {
        "subject": subject,
        "importance": "normal",
        "body": {
            "contentType": "HTML",
            "content": html_body,
        },
        "from": {
            "emailAddress": {
                "name":    from_name,
                "address": settings.ms_sender_email,
            }
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "name":    to_name,
                    "address": to_email,
                }
            }
        ],
    }

    attachments = []
    if csv_data:
        encoded = base64.b64encode(csv_data.encode("utf-8")).decode("utf-8")
        attachments.append({
            "@odata.type":  "#microsoft.graph.fileAttachment",
            "name":          csv_filename,
            "contentType":   "text/csv",
            "contentBytes":  encoded,
        })

    for att in (extra_attachments or []):
        attachments.append({
            "@odata.type":  "#microsoft.graph.fileAttachment",
            "name":          att.get("name", "attachment"),
            "contentType":   att.get("content_type", "application/octet-stream"),
            "contentBytes":  att.get("content_bytes", ""),
        })

    if attachments:
        message["attachments"] = attachments

    return message


async def get_messages(folder: str = "inbox", top: int = 50) -> list[dict]:
    """Fetch messages from a mail folder via Graph API. Requires Mail.Read permission."""
    token = await get_access_token()
    folder_map = {"inbox": "inbox", "sent": "sentItems"}
    folder_path = folder_map.get(folder, folder)
    url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/mailFolders/{folder_path}/messages"
    params = {
        "$top": min(top, 100),
        "$select": "id,subject,from,toRecipients,receivedDateTime,bodyPreview,isRead,hasAttachments,conversationId",
        "$orderby": "receivedDateTime desc",
    }
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
    return resp.json().get("value", [])


async def get_message_body(message_id: str) -> dict:
    """Fetch a single message with full HTML/text body."""
    token = await get_access_token()
    url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/messages/{message_id}"
    params = {
        "$select": "id,subject,from,toRecipients,receivedDateTime,body,isRead,conversationId,hasAttachments"
    }
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
    return resp.json()


async def send_email_via_outlook(
    to_email:    str,
    to_name:     str,
    from_name:   str,
    subject:     str,
    body:        str,
    csv_data:    str | None = None,
    csv_filename: str = "leads_100.csv",
    deal_id:     str | None = None,
    extra_attachments: list[dict] | None = None,
) -> dict:
    """
    Send an email through Microsoft Graph on behalf of the configured
    sender mailbox. Returns {"success": True, "message_id": "..."} or
    {"success": False, "error": "..."}.
    """
    try:
        token = await get_access_token()

        # Generate tracking pixel if deal_id and backend URL are configured
        tracking_pixel_url = None
        tracking_token = None
        if deal_id and settings.backend_url:
            tracking_token = secrets.token_urlsafe(24)
            tracking_pixel_url = f"{settings.backend_url}/api/mail/track/{tracking_token}"

        message = _build_message(
            to_email=to_email,
            to_name=to_name,
            from_name=from_name,
            subject=subject,
            body=body,
            csv_data=csv_data,
            csv_filename=csv_filename,
            tracking_pixel_url=tracking_pixel_url,
            extra_attachments=extra_attachments,
        )

        # POST /users/{sender}/sendMail
        # The sender must have a mailbox in the tenant.
        url = f"{GRAPH_BASE}/users/{settings.ms_sender_email}/sendMail"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json={"message": message}, headers=headers)

        # Graph returns 202 Accepted on success (no body)
        if resp.status_code == 202:
            logger.info("Email sent to %s via Outlook Graph API", to_email)
            import time
            message_id = f"graph-{to_email}-{int(time.time())}"
            # Save tracking event to DB if we generated a pixel
            if deal_id and tracking_token:
                try:
                    from app.services import database as db
                    await db.save_email_event(deal_id, tracking_token, "sent", subject)
                    logger.info("Email event saved for deal %s token %s", deal_id, tracking_token[:8])
                except Exception as exc:
                    logger.warning("Failed to save email event: %s", exc)
            return {"success": True, "message_id": message_id}

        # Anything else is an error
        error_detail = resp.text
        logger.error("Graph API error %s: %s", resp.status_code, error_detail)
        return {"success": False, "error": f"Graph API {resp.status_code}: {error_detail}"}

    except httpx.HTTPStatusError as exc:
        logger.exception("HTTP error sending email")
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        logger.exception("Unexpected error sending email")
        return {"success": False, "error": str(exc)}
