import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException, Header
from app.models.schemas import InstantlyWebhookPayload, ProspectData
from app.services.icp import generate_icp
from app.services.apollo import search_leads, leads_to_csv
from app.services.composer import compose_email_body
from app.services.outlook import send_email_via_outlook
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

POSITIVE_EVENTS  = {"reply.positive", "lead_interested"}
ANY_REPLY_EVENTS = {"reply.positive", "reply", "email_reply",
                    "lead_interested", "reply.negative", "reply.all"}


def verify_webhook_secret(x_instantly_secret: str | None):
    if settings.instantly_webhook_secret and x_instantly_secret != settings.instantly_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


async def stop_sequence_if_active(prospect_email: str) -> bool:
    all_deals = await db.list_deals()
    matching = [
        d for d in all_deals
        if d.get("email", "").lower() == prospect_email.lower()
        and d.get("seq_active")
    ]
    for deal in matching:
        await db.stop_sequence(deal["id"], reason="prospect_replied")
        await db.advance_deal_stage(deal["id"], "replied")
        logger.info("Sequence stopped for %s", prospect_email)
    return bool(matching)


async def run_full_pipeline(payload: InstantlyWebhookPayload):
    """
    Full automated pipeline triggered by Instantly.ai webhook.
    Uses the normalised helper methods on the payload to handle
    Instantly's flat field format.
    """
    prospect = payload.to_prospect_data()
    reply    = payload.get_reply()

    logger.info("=== Pipeline start: %s <%s> ===", prospect.company, prospect.email)

    # Stage 1 — create CRM deal
    deal = await db.create_deal(
        name       = prospect.name,
        email      = prospect.email,
        company    = prospect.company,
        domain     = prospect.domain or "",
        campaign   = payload.campaign_name or payload.campaign or "",
        reply_body = reply,
    )
    deal_id = deal["id"]
    run_id  = await db.start_pipeline_run(deal_id)

    # Stage 2 — ICP generation
    await db.update_pipeline_run(run_id, stage="icp_generation")
    try:
        icp = await generate_icp(prospect, reply)
        await db.set_deal_icp(deal_id, icp.model_dump())
        await db.advance_deal_stage(deal_id, "icp")
        logger.info("ICP done: %s / %s", icp.industry, icp.sub_niche)
    except Exception as exc:
        logger.exception("ICP failed for %s", deal_id)
        await db.finish_pipeline_run(run_id, status="failed", error=str(exc))
        return

    # Stage 3 — Apollo lead search
    await db.update_pipeline_run(run_id, stage="apollo_search")
    try:
        leads = await search_leads(icp, limit=100)
        if leads:
            await db.save_leads(deal_id, run_id, leads)
            logger.info("%d leads saved", len(leads))
        else:
            logger.warning("Apollo returned 0 leads for %s", deal_id)
    except Exception as exc:
        logger.exception("Apollo failed for %s", deal_id)
        leads = []

    # Stage 4 — send or hold for review
    if settings.review_mode:
        await db.advance_deal_stage(deal_id, "pending_review")
        await db.update_pipeline_run(run_id, stage="awaiting_review", status="paused")
        logger.info("Review mode — deal %s paused", deal_id)
        return

    await _send_leads_email(deal_id, run_id, prospect, icp, leads)


async def _send_leads_email(deal_id, run_id, prospect, icp, leads):
    await db.update_pipeline_run(run_id, stage="email_delivery")
    try:
        approved_leads = await db.get_leads_for_deal(deal_id, approved_only=True)
        csv_data = leads_to_csv(approved_leads if approved_leads else leads)

        email_body = await compose_email_body(
            prospect     = prospect,
            icp          = icp,
            from_name    = settings.default_from_name,
            sender_email = settings.ms_sender_email,
            template     = settings.default_email_template,
        )

        result = await send_email_via_outlook(
            to_email     = prospect.email,
            to_name      = prospect.name,
            from_name    = settings.default_from_name,
            subject      = f"Your 100 leads — {prospect.company}",
            body         = email_body,
            csv_data     = csv_data,
            csv_filename = "leads_100.csv",
        )

        if result["success"]:
            await db.advance_deal_stage(deal_id, "delivered")
            await db.finish_pipeline_run(run_id, status="complete")
            logger.info("Email sent to %s", prospect.email)
        else:
            raise RuntimeError(result.get("error", "Send failed"))

    except Exception as exc:
        logger.exception("Email delivery failed for %s", deal_id)
        await db.finish_pipeline_run(run_id, status="failed", error=str(exc))


@router.post("/instantly")
async def receive_instantly_webhook(
    payload: InstantlyWebhookPayload,
    background_tasks: BackgroundTasks,
    x_instantly_secret: str | None = Header(default=None),
):
    """
    Receives webhook events from Instantly.ai.
    Handles Instantly's actual flat payload format.
    """
    verify_webhook_secret(x_instantly_secret)

    prospect_email = payload.get_prospect_email()
    event          = payload.get_event()

    logger.info("Webhook received: event=%s email=%s", event, prospect_email)

    # Stop any active sequence for this prospect on any reply
    sequence_stopped = False
    if event in ANY_REPLY_EVENTS:
        sequence_stopped = await stop_sequence_if_active(prospect_email)

    # Only run full pipeline for positive/interested replies
    if event not in POSITIVE_EVENTS:
        return {
            "received":         True,
            "processed":        False,
            "sequence_stopped": sequence_stopped,
            "reason":           f"event '{event}' is not a positive reply",
        }

    if not prospect_email:
        raise HTTPException(status_code=422, detail="No email address in payload")

    background_tasks.add_task(run_full_pipeline, payload)
    logger.info("Pipeline queued for %s <%s>", payload.get_company(), prospect_email)

    return {
        "received":    True,
        "processed":   True,
        "prospect":    prospect_email,
        "company":     payload.get_company(),
        "event":       event,
        "review_mode": settings.review_mode,
    }
