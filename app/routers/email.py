from fastapi import APIRouter, HTTPException
from app.models.schemas import (
    EmailRequest, EmailResponse,
    OutlookAccountCreate, OutlookAccountUpdate, OutlookAccountResponse,
)
from app.services.outlook import send_email_via_outlook, get_access_token
from app.services import database as db

router = APIRouter()


@router.post("/send", response_model=EmailResponse)
async def send_delivery_email(req: EmailRequest):
    """
    Send a lead delivery email via Microsoft Outlook (Graph API).

    Pass csv_data as a plain UTF-8 CSV string — the service will
    base64-encode it before attaching.
    """
    result = await send_email_via_outlook(
        to_email=req.to_email,
        to_name=req.to_name,
        from_name=req.from_name,
        subject=req.subject,
        body=req.body,
        csv_data=req.csv_data if req.attach_csv else None,
        csv_filename=req.csv_filename,
    )

    if not result["success"]:
        raise HTTPException(status_code=502, detail=result["error"])

    return EmailResponse(success=True, message_id=result.get("message_id"))


@router.get("/test-connection")
async def test_outlook_connection():
    """
    Smoke-test the Microsoft Graph token endpoint using environment-variable credentials.
    Does NOT send an email — just confirms credentials work.
    """
    try:
        token = await get_access_token()
        return {"success": True, "token_preview": token[:12] + "..."}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


# ── Outlook account management ────────────────────────────────────────────────

@router.get("/accounts", response_model=list[OutlookAccountResponse])
async def list_outlook_accounts():
    """Return all saved Outlook sender accounts."""
    return await db.get_outlook_accounts()


@router.post("/accounts", response_model=OutlookAccountResponse, status_code=201)
async def create_outlook_account(req: OutlookAccountCreate):
    """Save a new Outlook sender account."""
    account = await db.create_outlook_account(
        tenant_id=req.tenant_id,
        client_id=req.client_id,
        client_secret=req.client_secret,
        sender_email=req.sender_email,
        display_name=req.display_name,
        is_default=req.is_default,
    )
    return account


@router.put("/accounts/{account_id}", response_model=OutlookAccountResponse)
async def update_outlook_account(account_id: str, req: OutlookAccountUpdate):
    """Update an existing Outlook sender account."""
    account = await db.get_outlook_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    updated = await db.update_outlook_account(account_id, req.model_dump(exclude_none=True))
    return updated


@router.delete("/accounts/{account_id}", status_code=204)
async def delete_outlook_account(account_id: str):
    """Delete a saved Outlook sender account."""
    account = await db.get_outlook_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    await db.delete_outlook_account(account_id)


@router.post("/accounts/test-credentials")
async def test_new_credentials(req: OutlookAccountCreate):
    """
    Test Outlook credentials before saving them.
    Does NOT send an email — just confirms the token endpoint accepts the credentials.
    """
    credentials = {
        "tenant_id":     req.tenant_id,
        "client_id":     req.client_id,
        "client_secret": req.client_secret,
    }
    try:
        token = await get_access_token(credentials)
        return {"success": True, "token_preview": token[:12] + "..."}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.post("/accounts/{account_id}/test")
async def test_saved_account(account_id: str):
    """
    Test connection for a saved Outlook account.
    Does NOT send an email — just confirms credentials still work.
    """
    account = await db.get_outlook_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    credentials = {
        "tenant_id":     account["tenant_id"],
        "client_id":     account["client_id"],
        "client_secret": account["client_secret"],
    }
    try:
        token = await get_access_token(credentials)
        return {"success": True, "token_preview": token[:12] + "..."}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
