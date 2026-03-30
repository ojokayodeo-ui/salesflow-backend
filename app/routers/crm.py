"""
CRM Router
Exposes deals, leads, and pipeline runs to the frontend.
Includes review queue endpoints for manual lead approval.
"""

import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.services import database as db
from app.services.apollo import leads_to_csv

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Deals ──────────────────────────────────────────────────────────────────

@router.get("/deals")
async def list_deals():
    deals = await db.list_deals()
    return {"deals": deals, "total": len(deals)}


@router.get("/deals/{deal_id}")
async def get_deal(deal_id: str):
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@router.post("/deals")
async def create_deal_manually(body: dict):
    name    = (body.get("name") or "").strip()
    email   = (body.get("email") or "").strip()
    company = (body.get("company") or "").strip()
    if not name or not email or not company:
        raise HTTPException(status_code=400, detail="name, email and company required")
    deal = await db.create_deal(
        name=name, email=email, company=company,
        domain=body.get("domain",""), campaign=body.get("campaign",""),
        reply_body=body.get("reply_body",""),
    )
    stage = body.get("stage","new")
    if stage != "new":
        deal = await db.advance_deal_stage(deal["id"], stage)
    return deal


@router.patch("/deals/{deal_id}/stage")
async def update_stage(deal_id: str, stage: str):
    valid = {"new","icp","pending_review","delivered","meeting","won"}
    if stage not in valid:
        raise HTTPException(status_code=400, detail=f"Stage must be one of {valid}")
    deal = await db.advance_deal_stage(deal_id, stage)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@router.delete("/deals/{deal_id}")
async def delete_deal(deal_id: str):
    from app.services.database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM leads WHERE deal_id=$1", deal_id)
        await conn.execute("DELETE FROM pipeline_runs WHERE deal_id=$1", deal_id)
        await conn.execute("DELETE FROM deals WHERE id=$1", deal_id)
    return {"deleted": True, "deal_id": deal_id}


# ── Review queue ────────────────────────────────────────────────────────────

@router.get("/review-queue")
async def get_review_queue():
    """
    Returns all deals currently paused and awaiting lead approval.
    The frontend polls this to show the review badge count.
    """
    all_deals = await db.list_deals()
    pending = [d for d in all_deals if d.get("stage") == "pending_review"]
    return {"pending": pending, "count": len(pending)}


@router.get("/deals/{deal_id}/leads")
async def get_deal_leads(deal_id: str, approved_only: bool = False):
    """Return leads for a deal. Pass approved_only=true to filter to approved leads only."""
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    leads = await db.get_leads_for_deal(deal_id, approved_only=approved_only)
    return {
        "deal_id":  deal_id,
        "leads":    leads,
        "total":    len(leads),
        "approved": sum(1 for l in leads if l.get("approved")),
        "rejected": sum(1 for l in leads if not l.get("approved")),
    }


@router.patch("/deals/{deal_id}/leads/{lead_id}")
async def update_lead_approval(deal_id: str, lead_id: int, body: dict):
    """Approve or reject a single lead. Body: {"approved": true|false}"""
    approved = bool(body.get("approved", True))
    await db.set_lead_approval(lead_id, approved)
    return {"lead_id": lead_id, "approved": approved}


@router.post("/deals/{deal_id}/leads/bulk-approve")
async def bulk_approve_leads(deal_id: str, body: dict):
    """
    Approve a specific subset of leads. Body: {"approved_ids": [1, 2, 3, ...]}
    All other leads for this deal are rejected.
    """
    approved_ids = body.get("approved_ids", [])
    await db.bulk_set_lead_approval(deal_id, approved_ids)
    leads = await db.get_leads_for_deal(deal_id)
    return {
        "deal_id":  deal_id,
        "approved": sum(1 for l in leads if l.get("approved")),
        "rejected": sum(1 for l in leads if not l.get("approved")),
    }


@router.post("/deals/{deal_id}/approve-and-send")
async def approve_and_send(deal_id: str, background_tasks: BackgroundTasks):
    """
    Called from the frontend review screen after you're happy with the lead list.
    Triggers the email delivery step and moves the deal to 'delivered'.
    Works whether you've edited the approval list or not.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    if deal.get("stage") != "pending_review":
        raise HTTPException(status_code=400, detail="Deal is not in pending_review stage")

    background_tasks.add_task(_send_approved_leads, deal_id, deal)
    return {"queued": True, "deal_id": deal_id, "message": "Email delivery started"}


async def _send_approved_leads(deal_id: str, deal: dict):
    """Background task: send the approved lead list and advance the deal."""
    from app.models.schemas import ProspectData
    from app.models.schemas import ICPData
    from app.services.composer import compose_email_body
    from app.services.outlook import send_email_via_outlook
    from app.config import settings

    prospect = ProspectData(
        name    = deal["name"],
        email   = deal["email"],
        company = deal["company"],
        domain  = deal.get("domain",""),
    )

    icp_dict = deal.get("icp") or {}
    try:
        icp = ICPData(**icp_dict)
    except Exception:
        icp = None

    # Get only approved leads
    leads = await db.get_leads_for_deal(deal_id, approved_only=True)
    if not leads:
        # fall back to all leads if none have been individually approved
        leads = await db.get_leads_for_deal(deal_id)

    csv_data = leads_to_csv(leads)

    run_id = await db.start_pipeline_run(deal_id)
    await db.update_pipeline_run(run_id, stage="email_delivery")

    try:
        email_body = await compose_email_body(
            prospect     = prospect,
            icp          = icp,
            from_name    = settings.default_from_name,
            sender_email = settings.ms_sender_email,
            template     = settings.default_email_template,
        )

        result = await send_email_via_outlook(
            to_email     = deal["email"],
            to_name      = deal["name"],
            from_name    = settings.default_from_name,
            subject      = f"Your 100 leads — {deal['company']}",
            body         = email_body,
            csv_data     = csv_data,
            csv_filename = "leads_100.csv",
        )

        if result["success"]:
            await db.advance_deal_stage(deal_id, "delivered")
            await db.finish_pipeline_run(run_id, status="complete")
            logger.info("Approved leads sent for deal %s — %d leads delivered", deal_id, len(leads))
        else:
            raise RuntimeError(result.get("error","Send failed"))

    except Exception as exc:
        logger.exception("Approved send failed for deal %s", deal_id)
        await db.finish_pipeline_run(run_id, status="failed", error=str(exc))


# ── CSV download ────────────────────────────────────────────────────────────

@router.get("/deals/{deal_id}/leads/csv")
async def download_leads_csv(deal_id: str, approved_only: bool = False):
    from fastapi.responses import Response
    leads = await db.get_leads_for_deal(deal_id, approved_only=approved_only)
    csv   = leads_to_csv(leads)
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="leads_{deal_id}.csv"'},
    )


# ── Stats ───────────────────────────────────────────────────────────────────

@router.get("/stats")
async def get_stats():
    deals  = await db.list_deals()
    counts = {s: 0 for s in ["new","icp","pending_review","delivered","meeting","won"]}
    for d in deals:
        s = d.get("stage","new")
        if s in counts:
            counts[s] += 1
    return {"total": len(deals), **counts}


# ── Pipeline mode toggle ────────────────────────────────────────────────────

@router.get("/pipeline-mode")
async def get_pipeline_mode():
    """Returns current pipeline mode so the frontend can show the toggle state."""
    from app.config import settings
    return {"review_mode": settings.review_mode}


# ── Sequence endpoints ──────────────────────────────────────────────────────

@router.post("/deals/{deal_id}/sequence/start")
async def start_sequence(deal_id: str, body: dict):
    """
    Enrol a deal in a sequence.
    Body: {"seq_id": "abc123", "step": 1}
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    seq_id = body.get("seq_id", "")
    step   = int(body.get("step", 1))
    await db.start_sequence(deal_id, seq_id, step)
    return {"started": True, "deal_id": deal_id, "seq_id": seq_id, "step": step}


@router.post("/deals/{deal_id}/sequence/stop")
async def stop_sequence(deal_id: str, body: dict = {}):
    """
    Stop the sequence for a deal.
    Body: {"reason": "manual"} — optional
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    reason = body.get("reason", "manual")
    await db.stop_sequence(deal_id, reason=reason)
    return {"stopped": True, "deal_id": deal_id, "reason": reason}


@router.post("/deals/{deal_id}/sequence/next")
async def advance_sequence(deal_id: str):
    """Advance a deal to the next sequence step (after sending email)."""
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")
    new_step = await db.advance_sequence_step(deal_id)
    return {"deal_id": deal_id, "new_step": new_step}


@router.get("/sequences/active")
async def get_active_sequences():
    """Return all deals with an active sequence — for the Active Sequences tab."""
    enrolled = await db.get_active_sequences()
    return {"enrolled": enrolled, "count": len(enrolled)}


# ── Email scheduling endpoints ──────────────────────────────────────────────

@router.post("/deals/{deal_id}/sequence/schedule")
async def schedule_sequence_emails(deal_id: str, body: dict):
    """
    Schedule all remaining emails in a sequence for a deal.
    
    Body: {
        "seq_steps": [{"subject":"...","body":"...","delay":0}, ...],
        "seq_id": "abc",
        "send_time": "09:00",
        "timezone": "Europe/London",
        "allowed_days": ["mon","tue","wed","thu","fri"],
        "current_step": 0
    }
    """
    from app.services.scheduler import calculate_send_at
    from app.services.database import schedule_email, cancel_scheduled_emails
    import pytz
    from datetime import datetime

    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    steps       = body.get("seq_steps", [])
    seq_id      = body.get("seq_id", "")
    send_time   = body.get("send_time", "09:00")
    tz_str      = body.get("timezone", "Europe/London")
    allowed     = body.get("allowed_days", ["mon","tue","wed","thu","fri"])
    start_step  = int(body.get("current_step", 0))

    # Cancel any existing scheduled emails for this deal
    await cancel_scheduled_emails(deal_id)

    now_utc = datetime.now(pytz.UTC)
    scheduled = []

    for i, step in enumerate(steps):
        if i < start_step:
            continue  # already sent
        delay = int(step.get("delay", 0))
        send_at = calculate_send_at(now_utc, delay, send_time, tz_str, allowed)
        email_id = await schedule_email(
            deal_id     = deal_id,
            seq_id      = seq_id,
            step_index  = i,
            subject     = step.get("subject", ""),
            body        = step.get("body", ""),
            send_at_utc = send_at.isoformat(),
            timezone    = tz_str,
        )
        scheduled.append({"step": i+1, "subject": step.get("subject",""), "send_at": send_at.isoformat(), "id": email_id})

    return {"scheduled": scheduled, "count": len(scheduled)}


@router.get("/deals/{deal_id}/scheduled-emails")
async def get_scheduled_emails(deal_id: str):
    """Return all scheduled emails for a deal."""
    from app.services.database import get_scheduled_emails_for_deal
    emails = await get_scheduled_emails_for_deal(deal_id)
    return {"deal_id": deal_id, "emails": emails, "count": len(emails)}


@router.delete("/deals/{deal_id}/scheduled-emails")
async def cancel_all_scheduled(deal_id: str):
    """Cancel all pending scheduled emails for a deal."""
    from app.services.database import cancel_scheduled_emails
    await cancel_scheduled_emails(deal_id)
    return {"cancelled": True, "deal_id": deal_id}


# ── CSV Upload & Lead Delivery ───────────────────────────────────────────────

@router.post("/deals/{deal_id}/upload-leads")
async def upload_leads_csv(deal_id: str, body: dict):
    """
    Accept a CSV (as text) uploaded from the frontend.
    Parses it into lead rows, saves to DB, returns parsed count.
    Body: { "csv_text": "...", "filename": "leads.csv" }
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    csv_text = body.get("csv_text", "")
    filename = body.get("filename", "leads.csv")
    if not csv_text.strip():
        raise HTTPException(status_code=400, detail="csv_text is empty")

    # Parse CSV into lead dicts
    import csv, io
    reader = csv.DictReader(io.StringIO(csv_text))
    leads = []
    for row in reader:
        # Normalise common column name variants
        def g(*keys):
            for k in keys:
                for rk in row:
                    if rk.strip().lower() == k.lower():
                        return row[rk].strip()
            return ""
        leads.append({
            "first_name":   g("first name", "firstname", "first_name"),
            "last_name":    g("last name", "lastname", "last_name"),
            "full_name":    g("full name", "fullname", "full_name", "name"),
            "title":        g("title", "job title", "jobtitle", "position"),
            "email":        g("email", "email address", "work email"),
            "company":      g("company", "company name", "organisation", "organization"),
            "city":         g("city", "location"),
            "country":      g("country"),
            "linkedin_url": g("linkedin", "linkedin url", "linkedin_url", "profile url"),
        })

    if not leads:
        raise HTTPException(status_code=400, detail="No rows parsed from CSV")

    run_id = await db.start_pipeline_run(deal_id)
    await db.save_leads(deal_id, run_id, leads)
    await db.finish_pipeline_run(run_id, status="complete")

    logger.info("Uploaded %d leads for deal %s from %s", len(leads), deal_id, filename)
    return {
        "uploaded":  True,
        "deal_id":   deal_id,
        "count":     len(leads),
        "filename":  filename,
    }


@router.post("/deals/{deal_id}/draft-email")
async def draft_delivery_email(deal_id: str, body: dict):
    """
    Ask Claude to draft the lead delivery email for this deal.
    Body: { "template": "warm|direct|ai", "custom_note": "..." }
    Returns: { "subject": "...", "body": "..." }
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    from app.models.schemas import ProspectData, ICPData
    from app.services.composer import compose_email_body
    from app.config import settings

    template    = body.get("template", "ai")
    custom_note = body.get("custom_note", "")

    prospect = ProspectData(
        name    = deal["name"],
        email   = deal["email"],
        company = deal["company"],
        domain  = deal.get("domain", ""),
    )

    icp_dict = deal.get("icp") or {}
    try:
        icp = ICPData(**icp_dict)
    except Exception:
        icp = None

    body_text = await compose_email_body(
        prospect     = prospect,
        icp          = icp,
        from_name    = settings.default_from_name or "Kayode",
        sender_email = settings.ms_sender_email or "",
        template     = template,
    )

    # Append custom note if provided
    if custom_note.strip():
        body_text += f"\n\n{custom_note.strip()}"

    first_name = deal["name"].split()[0] if deal["name"] else "there"
    subject = f"Your targeted lead list — {deal.get('company', '')}"

    return {"subject": subject, "body": body_text}


@router.post("/deals/{deal_id}/send-leads")
async def send_leads_to_prospect(deal_id: str, background_tasks: BackgroundTasks, body: dict):
    """
    Send the uploaded CSV + email body to the prospect.
    Body: { "subject": "...", "body": "...", "filename": "leads.csv" }
    The CSV is pulled from the deal's stored leads.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    subject  = body.get("subject", f"Your leads — {deal.get('company','')}")
    body_txt = body.get("body", "")
    filename = body.get("filename", "leads.csv")

    if not body_txt.strip():
        raise HTTPException(status_code=400, detail="Email body is empty")

    background_tasks.add_task(
        _send_leads_to_prospect_bg,
        deal_id, deal, subject, body_txt, filename
    )
    return {"queued": True, "deal_id": deal_id, "to": deal["email"]}


async def _send_leads_to_prospect_bg(
    deal_id: str, deal: dict,
    subject: str, body_txt: str, filename: str
):
    from app.services.outlook import send_email_via_outlook
    from app.services.apollo import leads_to_csv
    from app.config import settings

    # Get leads from DB
    leads = await db.get_leads_for_deal(deal_id, approved_only=False)
    csv_data = leads_to_csv(leads) if leads else None

    result = await send_email_via_outlook(
        to_email     = deal["email"],
        to_name      = deal["name"],
        from_name    = settings.default_from_name or "Kayode",
        subject      = subject,
        body         = body_txt,
        csv_data     = csv_data,
        csv_filename = filename,
    )

    if result.get("success"):
        await db.advance_deal_stage(deal_id, "delivered")
        logger.info("Leads sent to prospect %s — deal advanced to delivered", deal["email"])
    else:
        logger.error("Failed to send leads to %s: %s", deal["email"], result.get("error"))


@router.post("/draft-email-quick")
async def draft_email_quick(body: dict):
    """
    Draft a lead delivery email using Claude AI.
    No database lookup needed — accepts context directly.
    Body: { "name": "...", "company": "...", "email": "...",
            "pain_point": "...", "industry": "...", "lead_count": 100,
            "sender_name": "...", "calendly_link": "...",
            "template": "warm|direct|ai", "custom_note": "..." }
    """
    from app.config import settings
    import httpx, json as _json

    name        = body.get("name", "there")
    company     = body.get("company", "your company")
    pain_point  = body.get("pain_point", "")
    industry    = body.get("industry", "consulting")
    lead_count  = body.get("lead_count", 100)
    sender_name = body.get("sender_name") or settings.default_from_name or "Kayode"
    calendly    = body.get("calendly_link", "")
    template    = body.get("template", "ai")
    custom_note = body.get("custom_note", "")
    first_name  = name.split()[0] if name else "there"

    # Warm and Direct are instant — no Claude call needed
    if template == "warm":
        email_body = f"""Hi {first_name},

As promised, please find your targeted lead list attached — {lead_count} verified contacts matched specifically to {company}'s ideal client profile.

Each contact has been filtered for {industry} firms most likely to need your services right now{(' — particularly those facing ' + pain_point.lower()) if pain_point else ''}.

To get the most from this list, I'd recommend prioritising the first 20–30 contacts and reaching out within the next 48 hours while the list is fresh.

If you'd like to jump on a quick call to walk through how to work these leads most effectively, you can grab a slot here:
{calendly or '[your-calendly-link]'}

Looking forward to hearing how it goes.

{sender_name}"""
        return {"subject": f"Your targeted lead list — {company}", "body": email_body}

    if template == "direct":
        email_body = f"""Hi {first_name},

Attached is your {lead_count}-contact lead list for {company}.

These are verified, targeted contacts in {industry}. Work through the list systematically — aim for 20 outreach attempts per day for best results.

Questions? Reply to this email or book a call: {calendly or '[your-calendly-link]'}

{sender_name}"""
        return {"subject": f"Your targeted lead list — {company}", "body": email_body}

    # AI template — call Claude
    prompt = f"""You are writing a professional lead delivery email on behalf of {sender_name}, who runs a B2B lead generation service.

The email is being sent to {name} at {company}.
They replied positively to a cold email and are now receiving a targeted list of {lead_count} leads in the {industry} space.
{f"Their main pain point: {pain_point}" if pain_point else ""}
{f"Include this Calendly link naturally: {calendly}" if calendly else ""}

Write a warm, professional email delivering the lead list. 3 short paragraphs. No subject line. No placeholder text. Sign off as {sender_name}. Make it feel personal and excited about their results.{(chr(10) + chr(10) + "Also incorporate this personal note from the sender: " + custom_note) if custom_note else ""}"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        data = resp.json()
        email_body = data["content"][0]["text"].strip()
        return {"subject": f"Your targeted lead list — {company}", "body": email_body}
    except Exception as exc:
        logger.error("AI draft failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"AI draft failed: {str(exc)}")
