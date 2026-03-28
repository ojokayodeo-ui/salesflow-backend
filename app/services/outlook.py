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


def _build_message(
    to_email:    str,
    to_name:     str,
    from_name:   str,
    subject:     str,
    body:        str,
    csv_data:    str | None = None,
    csv_filename: str = "leads_100.csv",
) -> dict:
    """
    Construct the Graph API sendMail message object.
    Attaches the CSV as a base64 file attachment when csv_data is provided.
    """
    message: dict = {
        "subject": subject,
        "importance": "normal",
        "body": {
            "contentType": "Text",
            "content": body,
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

    if csv_data:
        # csv_data should be plain text (the CSV string).
        # Graph API expects base64-encoded content bytes.
        encoded = base64.b64encode(csv_data.encode("utf-8")).decode("utf-8")
        message["attachments"] = [
            {
                "@odata.type":  "#microsoft.graph.fileAttachment",
                "name":          csv_filename,
                "contentType":   "text/csv",
                "contentBytes":  encoded,
            }
        ]

    return message


async def send_email_via_outlook(
    to_email:    str,
    to_name:     str,
    from_name:   str,
    subject:     str,
    body:        str,
    csv_data:    str | None = None,
    csv_filename: str = "leads_100.csv",
) -> dict:
    """
    Send an email through Microsoft Graph on behalf of the configured
    sender mailbox. Returns {"success": True, "message_id": "..."} or
    {"success": False, "error": "..."}.
    """
    try:
        token = await get_access_token()

        message = _build_message(
            to_email=to_email,
            to_name=to_name,
            from_name=from_name,
            subject=subject,
            body=body,
            csv_data=csv_data,
            csv_filename=csv_filename,
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
            # Graph doesn't return a message ID on sendMail, so we fabricate one
            # from the recipient + timestamp for traceability.
            import time
            message_id = f"graph-{to_email}-{int(time.time())}"
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
