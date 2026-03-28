import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException, Header
from app.models.schemas import InstantlyWebhookPayload
from app.services.icp import generate_icp
from app.services.apollo import search_leads, leads_to_csv
from app.services.composer import compose_email_body
from app.services.outlook import send_email_via_outlook
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# All reply event types Instantly may send
POSITIVE_EVENTS = {"reply.positive", "reply", "email_reply"}
ANY_REPLY_EVENTS = {"reply.positive", "reply", "email_reply", "reply.negative", "reply.all"}


def verify_webhook_secret(x_instantly_secret: str | None):
    if settings.instantly_webhook_secret and x_instantly_secret != settings.instantly_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


async def stop_sequence_if_active(prospect_email: str) -> bool:
    """
    Check if this prospect is enrolled in a sequence.
    If yes, stop it immediately and advance their deal to 'replied'.

    Returns True if a sequence was stopped.
    """
    all_deals = await db.list_deals()

    # Find any deal for this email address that has an active sequence
    matching = [
        d for d in all_deals
        if d.get("email", "").lower() == prospect_email.lower()
        and d.get("seq_active")
    ]

    if not matching:
        return False

    for deal in matching:
        deal_id = deal["id"]
        await db.stop_sequence(deal_id, reason="prospect_replied")
        await db.advance_deal_stage(deal_id, "replied")
        logger.info(
            "Sequence stopped for %s (deal %s) — prospect replied",
            prospect_email, deal_id,
        )

    return True


async def run_full_pipeline(payload: InstantlyWebhookPayload):
    """
    Full automated pipeline — fires for POSITIVE replies from new prospects.

    AUTO mode  → ICP → Apollo → Email sent → CRM = delivered
    REVIEW mode → ICP → Apollo → CRM = pending_review (awaits approval)
    """
    prospect = payload.prospect
    reply    = payload.reply_body or ""

    logger.info("=== Pipeline start: %s <%s> ===", prospect.company, prospect.email)

    deal = await db.create_deal(
        name       = prospect.name,
        email      = prospect.email,
        company    = prospect.company,
        domain     = prospect.domain or "",
        campaign   = payload.campaign or "",
        reply_body = reply,
    )
    deal_id = deal["id"]
    run_id  = await db.start_pipeline_run(deal_id)

    # ICP generation
    await db.update_pipeline_run(run_id, stage="icp_generation")
    try:
        icp = await generate_icp(prospect, reply)
        await db.set_deal_icp(deal_id, icp.model_dump())
        await db.advance_deal_stage(deal_id, "icp")
    except Exception as exc:
        logger.exception("ICP failed for %s", deal_id)
        await db.finish_pipeline_run(run_id, status="failed", error=str(exc))
        return

    # Apollo search
    await db.update_pipeline_run(run_id, stage="apollo_search")
    try:
        leads = await search_leads(icp, limit=100)
        if leads:
            await db.save_leads(deal_id, run_id, leads)
    except Exception as exc:
        logger.exception("Apollo failed for %s", deal_id)
        leads = []

    # Send or hold
    if settings.review_mode:
        await db.advance_deal_stage(deal_id, "pending_review")
        await db.update_pipeline_run(run_id, stage="awaiting_review", status="paused")
        logger.info("Review mode — deal %s paused", deal_id)
        return

    await _send_leads_email(deal_id, run_id, prospect, icp, leads)


async def _send_leads_email(deal_id, run_id, prospect, icp, leads):
    """Send the lead delivery email and mark deal as delivered."""
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
    Receives ALL reply events from Instantly.ai.

    If the prospect is already in the CRM with an active sequence,
    the sequence is stopped immediately regardless of event type.

    If it's a positive reply from a new prospect, the full pipeline runs.
    """
    verify_webhook_secret(x_instantly_secret)

    prospect_email = payload.prospect.email
    event          = payload.event or ""

    # ── Step 1: Always check if this prospect has an active sequence ──────────
    # Any reply at all — positive, negative, or neutral — stops the sequence.
    sequence_stopped = False
    if event in ANY_REPLY_EVENTS:
        sequence_stopped = await stop_sequence_if_active(prospect_email)
        if sequence_stopped:
            logger.info(
                "Reply received from %s — sequence stopped (event: %s)",
                prospect_email, event,
            )

    # ── Step 2: Only run the full pipeline for positive replies ───────────────
    if event not in POSITIVE_EVENTS:
        return {
            "received":         True,
            "processed":        False,
            "sequence_stopped": sequence_stopped,
            "reason":           "not a positive reply — sequence stopped if active",
        }

    # Positive reply → run pipeline in background
    background_tasks.add_task(run_full_pipeline, payload)
    logger.info("Pipeline queued for %s", prospect_email)
    return {
        "received":    True,
        "processed":   True,
        "prospect":    prospect_email,
        "company":     payload.prospect.company,
        "review_mode": settings.review_mode,
    }
