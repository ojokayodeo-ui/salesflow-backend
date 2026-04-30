"""
Pipeline Router

Endpoints for triggering and monitoring the full automated pipeline:
  POST /full-run/{deal_id}           - Run Apollo search + email draft + follow-up generation
  GET  /status/{deal_id}             - Return current pipeline step statuses
  POST /send-and-schedule/{deal_id}  - Send drafted email + schedule follow-ups
  POST /extract-website/{deal_id}    - Extract structured website intelligence
  GET  /website-intel/{deal_id}      - Retrieve stored website intelligence
"""

import json
import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/full-run/{deal_id}")
async def full_pipeline_run(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Trigger the full automated pipeline for a deal that has ICP segments.

    Runs in background:
      0. Extract website intelligence (if not already done)
      1. Apollo search across all ICP segments
      2. Save leads to DB
      3. Generate personalised delivery email
      4. Generate 4-email follow-up sequence
      5. Store results on the deal (pipeline_status, followup_draft)
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    icp_data = deal.get("icp") or {}
    segments = icp_data.get("segments", []) if isinstance(icp_data, dict) else []
    if not segments:
        raise HTTPException(
            status_code=400,
            detail="No ICP segments found for this deal. Generate ICPs first.",
        )

    auto_send = bool(body.get("auto_send", False))
    background_tasks.add_task(_run_pipeline_bg, deal_id, deal, auto_send)
    return {
        "queued":    True,
        "deal_id":   deal_id,
        "auto_send": auto_send,
        "message":   "Pipeline started. Poll /api/pipeline/status/" + deal_id + " for progress.",
    }


async def _run_pipeline_bg(deal_id: str, deal: dict, auto_send: bool):
    from app.services.auto_pipeline import run_auto_pipeline
    try:
        result = await run_auto_pipeline(deal_id, deal, auto_send=auto_send)
        logger.info(
            "Auto pipeline complete for deal %s - leads: %d, sent: %s",
            deal_id, result.get("lead_count", 0), result.get("sent"),
        )
    except Exception as exc:
        logger.exception("Auto pipeline background task failed for deal %s", deal_id)


@router.get("/status/{deal_id}")
async def pipeline_status(deal_id: str):
    """
    Return the current pipeline step statuses for a deal.
    Used by the frontend to poll progress and know when results are ready.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    raw_status = deal.get("pipeline_status")
    steps = {}
    if raw_status:
        try:
            steps = json.loads(raw_status) if isinstance(raw_status, str) else raw_status
        except Exception:
            pass

    raw_followup = deal.get("followup_draft")
    followup_draft = []
    if raw_followup:
        try:
            followup_draft = json.loads(raw_followup) if isinstance(raw_followup, str) else raw_followup
        except Exception:
            pass

    step_statuses = [v.get("status") for v in steps.values()]
    if any(s == "running" for s in step_statuses):
        overall = "running"
    elif steps and all(s in ("done", "skipped", "ok") for s in step_statuses):
        overall = "ready"
    elif any(s == "error" for s in step_statuses):
        overall = "partial"
    else:
        overall = "idle"

    leads      = await db.get_leads_for_deal(deal_id, approved_only=False)
    lead_count = len(leads)

    return {
        "deal_id":        deal_id,
        "overall":        overall,
        "steps":          steps,
        "lead_count":     lead_count,
        "followup_ready": bool(followup_draft),
        "followup_draft": followup_draft,
        "icp_ready":      bool(deal.get("icp")),
        "website_intel_ready": bool(deal.get("website_intel")),
    }


@router.post("/extract-website/{deal_id}")
async def extract_website(deal_id: str, background_tasks: BackgroundTasks):
    """
    Trigger multi-page website extraction for a deal.
    Crawls up to 6 pages (home, about, services, products, case studies, blog)
    and stores structured intelligence on the deal.

    Results available via GET /api/pipeline/website-intel/{deal_id}
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    website_url = deal.get("company_website") or ""
    if not website_url and deal.get("domain"):
        website_url = "https://" + deal["domain"]

    if not website_url:
        raise HTTPException(
            status_code=400,
            detail="No company website URL for this deal. Add the website in the deal profile first.",
        )

    background_tasks.add_task(_extract_website_bg, deal_id, website_url, deal.get("company") or "")
    return {
        "queued":   True,
        "deal_id":  deal_id,
        "website":  website_url,
        "message":  "Extraction started. Poll /api/pipeline/website-intel/" + deal_id + " for results.",
    }


async def _extract_website_bg(deal_id: str, website_url: str, company_name: str):
    from app.services.website_extractor import extract_website_intel
    try:
        intel = await extract_website_intel(website_url, company_name)
        pool  = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE deals SET website_intel=$1, updated_at=$2 WHERE id=$3",
                json.dumps(intel), db.now_iso(), deal_id,
            )
        logger.info(
            "Website intel stored for deal %s: status=%s pages=%s",
            deal_id, intel.get("status"), intel.get("source_pages", []),
        )
    except Exception as exc:
        logger.exception("Website extraction background failed for deal %s", deal_id)


@router.get("/website-intel/{deal_id}")
async def get_website_intel(deal_id: str):
    """
    Retrieve stored website intelligence for a deal.
    Returns the structured data extracted from the prospect's website.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    raw = deal.get("website_intel")
    intel = None
    if raw:
        try:
            intel = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            pass

    return {
        "deal_id": deal_id,
        "intel":   intel,
        "ready":   bool(intel and intel.get("status") in ("success", "failed")),
    }


@router.post("/send-and-schedule/{deal_id}")
async def send_and_schedule(deal_id: str, background_tasks: BackgroundTasks, body: dict):
    """
    Send the delivery email + schedule follow-ups for a deal whose pipeline is ready.

    Body: {
        "subject": "...",
        "body": "...",
        "filename": "leads.csv",
        "followups": [{"delay_days": 3, "subject": "...", "body": "..."}, ...]
    }
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    subject     = (body.get("subject") or "").strip()
    body_txt    = (body.get("body") or "").strip()
    filename    = body.get("filename") or f"{(deal.get('company') or 'leads').lower().replace(' ', '_')}_leads.csv"
    followups   = body.get("followups") or []
    attachments = body.get("attachments") or []

    if not subject or not body_txt:
        raise HTTPException(status_code=400, detail="subject and body are required")

    background_tasks.add_task(
        _send_and_schedule_bg, deal_id, deal, subject, body_txt, filename, followups, attachments
    )
    return {"queued": True, "deal_id": deal_id}


async def _send_and_schedule_bg(
    deal_id: str, deal: dict,
    subject: str, body_txt: str, filename: str,
    followups: list, attachments: list,
):
    from app.services.outlook import send_email_via_outlook
    from app.services.apollo import leads_to_csv
    from app.services.scheduler import calculate_send_at
    from app.services.database import schedule_email
    from datetime import datetime
    import pytz

    leads    = await db.get_leads_for_deal(deal_id, approved_only=False)
    csv_data = leads_to_csv(leads) if leads else None

    result = await send_email_via_outlook(
        to_email          = deal["email"],
        to_name           = deal["name"],
        from_name         = settings.default_from_name or "Kayode",
        subject           = subject,
        body              = body_txt,
        csv_data          = csv_data,
        csv_filename      = filename,
        deal_id           = deal_id,
        extra_attachments = attachments,
    )

    if result.get("success"):
        await db.advance_deal_stage(deal_id, "delivered")
        logger.info("send-and-schedule: email sent for deal %s", deal_id)
    else:
        logger.error("send-and-schedule: email failed for deal %s: %s", deal_id, result.get("error"))
        return

    if followups:
        now_utc = datetime.now(pytz.UTC)
        for i, step in enumerate(followups):
            delay_days = int(step.get("delay_days") or ((i + 1) * 3))
            send_at    = calculate_send_at(
                now_utc, delay_days, "09:00", "Europe/London",
                ["mon", "tue", "wed", "thu", "fri"],
            )
            attachments_json = json.dumps(step.get("attachments") or []) or None
            await schedule_email(
                deal_id     = deal_id,
                seq_id      = "auto_followup",
                step_index  = i,
                subject     = step.get("subject", ""),
                body        = step.get("body", ""),
                send_at_utc = send_at.isoformat(),
                timezone    = "Europe/London",
                attachments = attachments_json,
            )
        logger.info("send-and-schedule: %d follow-ups scheduled for deal %s", len(followups), deal_id)


# ── Legacy endpoints ──────────────────────────────────────────────────────────

@router.post("/run")
async def run_pipeline_legacy(req: dict):
    return {"message": "Use POST /api/pipeline/full-run/{deal_id} instead"}


@router.post("/compose-email")
async def compose_email_endpoint(
    prospect_email: str,
    prospect_name:  str,
    prospect_company: str,
    template: str = "warm",
):
    from app.models.schemas import ProspectData, ICPData
    from app.services.composer import compose_email_body

    prospect = ProspectData(
        name=prospect_name, email=prospect_email, company=prospect_company,
    )
    icp = ICPData(
        industry="Consulting", sub_niche="Management Consulting",
        company_size="10-50", hq_country="United Kingdom",
        target_titles=["Managing Director"],
        pain_point="No predictable pipeline",
        keywords=["consulting"],
        apollo_employee_min=10, apollo_employee_max=50,
        company_age_years="2-6", buying_signal="Hiring in sales",
    )
    body = await compose_email_body(
        prospect=prospect, icp=icp,
        from_name=settings.default_from_name,
        sender_email=settings.ms_sender_email,
        template=template,
    )
    return {"body": body}
