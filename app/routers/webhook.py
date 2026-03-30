import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException, Header
from app.models.schemas import InstantlyWebhookPayload
from app.services.icp import generate_icp_segments
from app.services.sentiment import score_reply
from app.services.instantly import get_lead_by_email, extract_prospect_data
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
    Pipeline: Webhook → CRM deal created → 5 ICP segments generated → Notification email sent

    Apollo search is now MANUAL:
    - Claude generates 5 audience segments with Apollo search prompts
    - You copy the prompt, search Apollo manually, pull the list
    - No Apollo API needed

    Email delivery still uses Outlook if configured.
    """
    prospect = payload.to_prospect_data()
    reply    = payload.get_reply()

    # Fix "Unknown" name — fall back to email or company if name is missing
    if not prospect.name or prospect.name.strip() == "Unknown":
        if prospect.email:
            prospect.name = prospect.email.split("@")[0].replace(".", " ").title()
        elif prospect.company:
            prospect.name = prospect.company
        else:
            prospect.name = "Unknown Prospect"

    # Derive company from email domain if missing
    if not prospect.company and prospect.email and "@" in prospect.email:
        domain = prospect.email.split("@")[1]
        # Convert domain to readable company name e.g. gkrecruitment.com -> Gk Recruitment
        company_guess = domain.split(".")[0].replace("-", " ").replace("_", " ").title()
        prospect.company = company_guess
        if not prospect.domain:
            prospect.domain = domain

    # Try to enrich prospect data from Instantly API
    email = prospect.email or payload.get_prospect_email()
    if email:
        try:
            lead_data = await get_lead_by_email(email)
            if lead_data:
                enriched = extract_prospect_data(lead_data, payload)
                from app.models.schemas import ProspectData
                prospect = ProspectData(
                    name         = enriched["name"] or prospect.name,
                    email        = email,
                    company      = enriched["company"] or prospect.company,
                    domain       = enriched["domain"] or prospect.domain,
                    website      = enriched["website"] or prospect.website,
                    job_title    = enriched["job_title"] or prospect.job_title,
                    job_level    = enriched["job_level"] or prospect.job_level,
                    linkedin     = enriched["linkedin"] or prospect.linkedin,
                    location     = enriched["location"] or prospect.location,
                    headcount    = enriched["headcount"] or prospect.headcount,
                    industry     = enriched["industry"] or prospect.industry,
                    sub_industry = enriched["sub_industry"] or prospect.sub_industry,
                    description  = enriched["description"] or prospect.description,
                    headline     = enriched["headline"] or prospect.headline,
                    department   = enriched["department"] or prospect.department,
                )
                logger.info("Enriched prospect: %s at %s (%s)",
                            prospect.name, prospect.company, prospect.job_title)
        except Exception as exc:
            logger.warning("Instantly enrichment failed for %s: %s", email, exc)

    # Final fallback — log what we have
    logger.info("Prospect data: name=%s company=%s job_title=%s location=%s",
                prospect.name, prospect.company, prospect.job_title, prospect.location)

    logger.info("=== Pipeline start: %s <%s> ===", prospect.company, prospect.email)

    # Duplicate check — if a deal already exists for this email, skip
    existing = await db.get_deal_by_email(prospect.email)
    if existing:
        logger.info(
            "Deal already exists for %s (deal %s, stage %s) — skipping duplicate",
            prospect.email, existing["id"], existing["stage"],
        )
        return

    # Stage 1 — Create CRM deal
    deal = await db.create_deal(
        name            = prospect.name,
        email           = prospect.email,
        company         = prospect.company,
        domain          = prospect.domain or "",
        campaign        = payload.campaign_name or payload.campaign or "",
        reply_body      = reply,
        job_title       = prospect.job_title or "",
        job_level       = payload.jobLevel or "",
        department      = payload.department or "",
        linkedin        = prospect.linkedin or "",
        location        = prospect.location or "",
        headcount       = prospect.headcount or "",
        industry        = prospect.industry or "",
        sub_industry    = prospect.sub_industry or "",
        company_website = prospect.website or "",
        company_desc    = prospect.description or "",
        headline        = prospect.headline or "",
        reply_subject   = payload.reply_subject or "",
    )
    deal_id = deal["id"]
    run_id  = await db.start_pipeline_run(deal_id)
    logger.info("Deal created: %s", deal_id)

    # Score reply sentiment
    try:
        sentiment = await score_reply(reply, prospect.name, prospect.company)
        await db.set_deal_sentiment(deal_id, sentiment["score"], sentiment["reason"], sentiment["emoji"])
        await db.update_last_activity(deal_id)
        logger.info("Sentiment: %s → %s", prospect.company, sentiment["score"])
    except Exception as exc:
        logger.warning("Sentiment scoring failed: %s", exc)

    # Stage 2 — Generate 5 ICP segments
    await db.update_pipeline_run(run_id, stage="icp_generation")
    try:
        segments = await generate_icp_segments(prospect, reply)
        # Store segments as ICP JSON (list of 5)
        await db.set_deal_icp(deal_id, {"segments": segments})
        await db.advance_deal_stage(deal_id, "icp")
        logger.info(
            "5 ICP segments generated for %s: %s",
            prospect.company,
            [s.get("segment_name", "?") for s in segments],
        )
    except Exception as exc:
        logger.exception("ICP generation failed for %s", deal_id)
        await db.finish_pipeline_run(run_id, status="failed", error=str(exc))
        return

    # Stage 3 — Send notification email (if Outlook is configured)
    # This notifies YOU that a new positive reply came in and ICP is ready
    if settings.ms_sender_email and settings.ms_tenant_id:
        await db.update_pipeline_run(run_id, stage="notification")
        try:
            from app.models.schemas import ICPData
            # Build a simple ICPData from first segment for the email template
            first_seg = segments[0] if segments else {}
            icp_for_email = ICPData(
                industry            = first_seg.get("industry", "Consulting"),
                sub_niche           = first_seg.get("sub_niche", ""),
                company_size        = first_seg.get("company_size", ""),
                hq_country          = first_seg.get("hq_country", ""),
                target_titles       = first_seg.get("target_titles", []),
                pain_point          = first_seg.get("pain_point", ""),
                keywords            = first_seg.get("keywords", []),
                apollo_employee_min = first_seg.get("employee_min", 10),
                apollo_employee_max = first_seg.get("employee_max", 50),
                company_age_years   = "2-8",
                buying_signal       = first_seg.get("buying_signal", ""),
                cold_email_hook     = first_seg.get("cold_email_hook", ""),
            )

            seg_summary = "\n".join([
                f"Segment {s.get('segment_number','')}: {s.get('segment_name','')}"
                for s in segments
            ])

            body = f"""New positive reply received from {prospect.name} at {prospect.company}.

Their reply: "{reply[:200]}{'...' if len(reply)>200 else ''}"

5 ICP segments have been generated. Open your PALM app to view them and get the Apollo search prompts.

Segments generated:
{seg_summary}

View deal: https://your-netlify-app.netlify.app (CRM tab)
"""

            await send_email_via_outlook(
                to_email     = settings.ms_sender_email,
                to_name      = "Kayode",
                from_name    = settings.default_from_name,
                subject      = f"New reply: {prospect.name} at {prospect.company} — ICP ready",
                body         = body,
                csv_data     = None,
                csv_filename = "",
            )
            logger.info("Notification email sent for %s", prospect.email)
        except Exception as exc:
            logger.warning("Notification email failed (non-critical): %s", exc)

    await db.finish_pipeline_run(run_id, status="complete")
    logger.info("=== Pipeline complete for %s ===", prospect.company)


@router.post("/instantly")
async def receive_instantly_webhook(
    payload: InstantlyWebhookPayload,
    background_tasks: BackgroundTasks,
    x_instantly_secret: str | None = Header(default=None),
):
    verify_webhook_secret(x_instantly_secret)

    prospect_email = payload.get_prospect_email()
    event          = payload.get_event()

    logger.info("Webhook received: event=%s email=%s", event, prospect_email)

    sequence_stopped = False
    if event in ANY_REPLY_EVENTS:
        sequence_stopped = await stop_sequence_if_active(prospect_email)

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
    }
