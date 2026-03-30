import logging
import time
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

# ── Idempotency lock ────────────────────────────────────────────────────────
# In-memory set of (email, event) keys with timestamps.
# Prevents the same webhook firing twice within 60 seconds.
_recent_webhooks: dict[str, float] = {}
IDEMPOTENCY_TTL = 60  # seconds

def _is_duplicate_webhook(email: str, event: str) -> bool:
    key = f"{email.lower()}:{event}"
    now = time.time()
    # Clean up expired keys
    expired = [k for k, t in _recent_webhooks.items() if now - t > IDEMPOTENCY_TTL]
    for k in expired:
        del _recent_webhooks[k]
    if key in _recent_webhooks:
        logger.info("Idempotency block: duplicate webhook for %s within %ss", email, IDEMPOTENCY_TTL)
        return True
    _recent_webhooks[key] = now
    return False


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


async def _find_existing_deal(email: str, name: str, company: str) -> dict | None:
    """
    3-layer duplicate detection:
      1. Exact email match
      2. Name + company match (catches partial-data duplicates)
      3. Company + domain match (catches same person with different email format)
    """
    # Layer 1 — email
    existing = await db.get_deal_by_email(email)
    if existing:
        return existing

    # Layer 2 — name + company (case-insensitive)
    if name and company:
        all_deals = await db.list_deals()
        name_l    = name.strip().lower()
        company_l = company.strip().lower()
        for d in all_deals:
            if (d.get("name", "").strip().lower() == name_l and
                    d.get("company", "").strip().lower() == company_l):
                logger.info("Duplicate by name+company: %s @ %s", name, company)
                return d

    # Layer 3 — domain match (same company domain, different email)
    if email and "@" in email:
        domain = email.split("@")[1].lower()
        if company and domain:
            all_deals = await db.list_deals()
            company_l = company.strip().lower()
            for d in all_deals:
                d_email = d.get("email", "")
                d_domain = d_email.split("@")[1].lower() if "@" in d_email else ""
                if (d_domain == domain and
                        d.get("company", "").strip().lower() == company_l):
                    logger.info("Duplicate by domain+company: %s / %s", domain, company)
                    return d

    return None


async def run_full_pipeline(payload: InstantlyWebhookPayload):
    """
    Pipeline: Webhook → CRM deal created → 5 ICP segments generated → Notification email sent
    """
    prospect = payload.to_prospect_data()
    reply    = payload.get_reply()

    # Fix "Unknown" name
    if not prospect.name or prospect.name.strip() == "Unknown":
        if prospect.email:
            prospect.name = prospect.email.split("@")[0].replace(".", " ").title()
        elif prospect.company:
            prospect.name = prospect.company
        else:
            prospect.name = "Unknown Prospect"

    # Enrich prospect data from Instantly API
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
                    job_title    = enriched["job_title"],
                    job_level    = enriched["job_level"],
                    linkedin     = enriched["linkedin"],
                    location     = enriched["location"],
                    headcount    = enriched["headcount"],
                    industry     = enriched["industry"],
                    sub_industry = enriched["sub_industry"],
                    description  = enriched["description"],
                    headline     = enriched["headline"],
                    department   = enriched["department"],
                )
                logger.info("Enriched prospect: %s at %s (%s)",
                            prospect.name, prospect.company, prospect.job_title)
        except Exception as exc:
            logger.warning("Instantly enrichment failed: %s", exc)

    logger.info("=== Pipeline start: %s <%s> ===", prospect.company, prospect.email)

    # ── Duplicate check (3 layers) ──────────────────────────────────────────
    existing = await _find_existing_deal(
        email   = prospect.email or "",
        name    = prospect.name or "",
        company = prospect.company or "",
    )
    if existing:
        logger.info(
            "Duplicate deal found for %s (id=%s stage=%s) — skipping",
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
    if settings.ms_sender_email and settings.ms_tenant_id:
        await db.update_pipeline_run(run_id, stage="notification")
        try:
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

    # ── Idempotency check — block duplicate webhooks within 60s ────────────
    if event in POSITIVE_EVENTS and prospect_email:
        if _is_duplicate_webhook(prospect_email, event):
            return {
                "received":  True,
                "processed": False,
                "reason":    "duplicate webhook blocked (idempotency)",
            }

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
        "received":  True,
        "processed": True,
        "prospect":  prospect_email,
        "company":   payload.get_company(),
        "event":     event,
    }
