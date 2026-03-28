from fastapi import APIRouter, BackgroundTasks
from app.models.schemas import PipelineRunRequest, PipelineRunResponse
from app.services.icp import generate_icp, icp_to_apollo_filters
from app.services.composer import compose_email_body
from app.services.outlook import send_email_via_outlook
from app.config import settings

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
async def run_pipeline(req: PipelineRunRequest):
    """
    Manually trigger the full pipeline for a given prospect.
    Use this from the SalesFlow frontend when you want to run
    outside of an Instantly webhook (e.g. from the Webhook Sim tab).

    If auto_send=True the email fires immediately.
    If auto_send=False the ICP and filters are returned for review.
    """
    icp = await generate_icp(req.prospect, "")
    apollo_filters = icp_to_apollo_filters(icp)

    email_status = "not_sent"

    if req.auto_send:
        body = await compose_email_body(
            prospect=req.prospect,
            icp=icp,
            from_name=settings.default_from_name,
            sender_email=settings.ms_sender_email,
            template=settings.default_email_template,
        )
        result = await send_email_via_outlook(
            to_email=req.prospect.email,
            to_name=req.prospect.name,
            from_name=settings.default_from_name,
            subject=f"Your 100 leads — {req.prospect.company}",
            body=body,
        )
        email_status = "sent" if result["success"] else f"failed: {result.get('error')}"

    return PipelineRunResponse(
        success=True,
        icp=icp,
        apollo_filters=apollo_filters,
        email_status=email_status,
    )


@router.post("/compose-email")
async def compose_email_endpoint(
    prospect_email: str,
    prospect_name: str,
    prospect_company: str,
    template: str = "warm",
):
    """
    Compose (but do not send) a personalised email body.
    Returns the draft for review in the frontend.
    """
    from app.models.schemas import ProspectData, ICPData
    prospect = ProspectData(
        name=prospect_name,
        email=prospect_email,
        company=prospect_company,
    )
    # Use a minimal fallback ICP for standalone composition
    icp = ICPData(
        industry="Consulting",
        sub_niche="Management Consulting",
        company_size="10–50",
        hq_country="United Kingdom",
        target_titles=["Managing Director"],
        pain_point="No predictable pipeline",
        keywords=["consulting"],
        apollo_employee_min=10,
        apollo_employee_max=50,
        company_age_years="2–6",
        buying_signal="Hiring in sales",
    )
    body = await compose_email_body(
        prospect=prospect,
        icp=icp,
        from_name=settings.default_from_name,
        sender_email=settings.ms_sender_email,
        template=template,
    )
    return {"body": body}
