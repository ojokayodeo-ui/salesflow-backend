from fastapi import APIRouter, HTTPException
from app.models.schemas import EmailRequest, EmailResponse
from app.services.outlook import send_email_via_outlook

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
    Smoke-test the Microsoft Graph token endpoint.
    Does NOT send an email — just confirms credentials work.
    """
    from app.services.outlook import get_access_token
    try:
        token = await get_access_token()
        return {"success": True, "token_preview": token[:12] + "..."}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
