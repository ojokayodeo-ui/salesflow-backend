"""
Full Automated Pipeline Service

Chains: Website extraction → Apollo search (across all ICP segments) → Lead list
        → Delivery email → 4-email follow-up sequence generation → optional auto-send

Called either:
  - Automatically after ICP generation in the webhook pipeline
  - Manually via POST /api/pipeline/full-run/{deal_id} from the frontend
"""

import json
import logging
import httpx
from datetime import datetime
import pytz

from app.config import settings
from app.services import database as db
from app.services.apollo import search_leads, leads_to_csv
from app.services.outlook import send_email_via_outlook
from app.models.schemas import ProspectData, ICPData

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


# ── Pipeline status helpers ───────────────────────────────────────────────────

async def _set_status(deal_id: str, step: str, status: str, detail: str = ""):
    try:
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT pipeline_status FROM deals WHERE id=$1", deal_id)
            if not row:
                return
            current = {}
            if row["pipeline_status"]:
                try:
                    current = json.loads(row["pipeline_status"])
                except Exception:
                    pass
            current[step] = {
                "status": status,
                "detail": detail,
                "ts": datetime.now(pytz.UTC).isoformat(),
            }
            await conn.execute(
                "UPDATE deals SET pipeline_status=$1 WHERE id=$2",
                json.dumps(current), deal_id,
            )
    except Exception as exc:
        logger.warning("pipeline_status update failed: %s", exc)


# ── Lead search across ICP segments ──────────────────────────────────────────

async def search_leads_across_segments(
    segments: list[dict], total_limit: int = 100
) -> list[dict]:
    """
    Run Apollo search for up to 3 ICP segments, merge and deduplicate by email.
    Returns at most total_limit leads.
    """
    per_seg = max(30, total_limit // min(len(segments), 3))
    all_leads: list[dict] = []
    seen_emails: set[str] = set()

    for seg in segments[:3]:
        try:
            icp = ICPData(
                industry            = seg.get("industry", ""),
                sub_niche           = seg.get("sub_niche", ""),
                company_size        = seg.get("company_size", ""),
                hq_country          = seg.get("hq_country", "United Kingdom"),
                target_titles       = seg.get("target_titles", []),
                pain_point          = seg.get("pain_point", ""),
                keywords            = seg.get("keywords", []),
                apollo_employee_min = int(seg.get("employee_min") or 10),
                apollo_employee_max = int(seg.get("employee_max") or 500),
                company_age_years   = "2-10",
                buying_signal       = seg.get("buying_signal", ""),
            )
            leads = await search_leads(icp, limit=per_seg)
            for lead in leads:
                key = (lead.get("email") or "").lower().strip()
                if key and key not in seen_emails:
                    seen_emails.add(key)
                    lead["icp_segment"] = seg.get("segment_name", "")
                    all_leads.append(lead)
            logger.info(
                "Apollo segment '%s': %d leads (running total: %d)",
                seg.get("segment_name", "?"), len(leads), len(all_leads),
            )
        except Exception as exc:
            logger.warning(
                "Apollo search failed for segment '%s': %s",
                seg.get("segment_name", "?"), exc,
            )

    return all_leads[:total_limit]


# ── Delivery email generation ─────────────────────────────────────────────────

async def generate_delivery_email(
    prospect: ProspectData,
    segments: list[dict],
    reply: str,
    lead_count: int,
    website_intel: dict | None = None,
) -> dict:
    """
    Generate a personalised lead delivery email using Claude.
    Optionally uses structured website intel for a more targeted opening.
    """
    first_name = (prospect.name or "there").split()[0]
    sender     = settings.default_from_name or "Kayode"

    seg_summary = "\n".join([
        f"- {s.get('segment_name', '')}: {s.get('industry', '')} "
        f"({s.get('company_size', '')} firms) targeting "
        f"{', '.join((s.get('target_titles') or [])[:2])}"
        for s in segments
    ])

    # Build website intel insight block
    wi_insight = ""
    if website_intel and website_intel.get("status") == "success":
        vp  = website_intel.get("value_proposition", "NOT FOUND")
        cus = website_intel.get("target_customers",  "NOT FOUND")
        svc = website_intel.get("services",          "NOT FOUND")
        parts = []
        if vp  != "NOT FOUND": parts.append(f"Value proposition: {vp}")
        if cus != "NOT FOUND": parts.append(f"Target customers: {cus}")
        if svc != "NOT FOUND": parts.append(f"Services: {svc}")
        if parts:
            wi_insight = "Website intelligence extracted:\n" + "\n".join(parts)

    prompt = f"""Write a professional lead delivery email from {sender} to {first_name} at {prospect.company}.

Context (use ONLY this data - no hallucination):
- Their reply to our cold email: "{reply[:300]}"
- Company: {prospect.company}
- Industry: {prospect.industry or 'not specified'}
- Job title: {prospect.job_title or 'not specified'}
- We analysed their business and built {len(segments)} ICP segments
- We found {lead_count} verified contacts via Apollo.io matching those ICPs
- The CSV is attached to this email
{wi_insight}

ICP segments built:
{seg_summary}

Write an email that:
1. Opens with one specific, genuine insight from their reply or business (not generic)
2. Briefly explains we built {len(segments)} ICP segments for their outreach
3. States the {lead_count}-contact list is attached, filtered for those ICPs
4. Clear CTA: book a 15-minute call to walk through the strategy
5. Professional, warm, concise - 4 short paragraphs max
6. Sign off as {sender}
7. NEVER use em dashes. Use a hyphen (-) or rewrite the sentence.
8. No placeholder text. No square brackets. Everything must be complete and real.

Return ONLY the email body. No subject line."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-6",
                    "max_tokens": 600,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            body_text = resp.json()["content"][0]["text"].strip()
    except Exception as exc:
        logger.warning("Delivery email generation failed: %s", exc)
        body_text = (
            f"Hi {first_name},\n\n"
            f"As promised, please find your targeted lead list attached - {lead_count} verified contacts "
            f"matched specifically to {prospect.company}'s ideal client profile.\n\n"
            f"Based on your business context, we identified {len(segments)} distinct audience segments "
            f"most likely to need your services right now. The list is filtered for those ICPs, "
            f"giving you a focused starting point.\n\n"
            f"I'd love to jump on a quick call to walk through the outreach strategy - "
            f"happy to share what's working best in your sector right now.\n\n"
            f"{sender}"
        )

    subject = (
        f"Your targeted lead list - {lead_count} contacts matched to "
        f"{prospect.company}'s ICPs"
    )
    return {"subject": subject, "body": body_text}


# ── Follow-up sequence generation ────────────────────────────────────────────

async def generate_followup_sequence(
    prospect: ProspectData,
    segments: list[dict],
    reply: str,
) -> list[dict]:
    """
    Generate a 4-email follow-up sequence using Claude.
    Returns list of {delay_days, subject, body} dicts.
    """
    first_name = (prospect.name or "there").split()[0]
    sender     = settings.default_from_name or "Kayode"
    seg_names  = ", ".join([s.get("segment_name", "") for s in segments[:3]])

    prompt = f"""Generate a 4-email follow-up sequence from {sender} to {first_name} at {prospect.company}.

Context:
- Initial delivery email already sent with their targeted lead list + ICP analysis
- Their original reply: "{reply[:200]}"
- Company: {prospect.company} | Industry: {prospect.industry or 'professional services'}
- ICP segments built: {seg_names}
- Goal: book a strategy call or nurture toward a meeting

Sequence:
1. delay_days: 3  - Reminder + value add (reference the list, give one practical tip)
2. delay_days: 7  - New angle (focus on one specific ICP segment, why it is the strongest)
3. delay_days: 14 - Social proof style (what works for similar businesses - no made-up names/numbers)
4. delay_days: 21 - Soft close / break-up email (no pressure, leave door open)

Rules:
- NEVER use em dashes. Use hyphens or rewrite.
- No placeholder text, no [insert X], nothing fake
- Each email under 130 words and self-contained
- Professional but direct
- Sign off every email as {sender}
- Sequence stops automatically if they reply (mention this naturally in email 1)

Return ONLY valid JSON, no markdown fences:
{{"sequence":[{{"delay_days":3,"subject":"...","body":"..."}},{{"delay_days":7,"subject":"...","body":"..."}},{{"delay_days":14,"subject":"...","body":"..."}},{{"delay_days":21,"subject":"...","body":"..."}}]}}"""

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-6",
                    "max_tokens": 2000,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            raw  = resp.json()["content"][0]["text"].strip()
            raw  = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
            seq  = data.get("sequence", [])
            if seq:
                return seq
    except Exception as exc:
        logger.warning("Follow-up sequence generation failed: %s", exc)

    return _fallback_followups(first_name, prospect.company, sender)


def _fallback_followups(first_name: str, company: str, sender: str) -> list[dict]:
    return [
        {
            "delay_days": 3,
            "subject":    "Re: Your lead list - quick tip",
            "body": (
                f"Hi {first_name},\n\n"
                f"Just checking in to see if you had a chance to look through the list.\n\n"
                f"One tip: start with the first 20-30 contacts and reach out within 48 hours "
                f"while the data is fresh - that is when response rates are highest.\n\n"
                f"Worth a 15-minute call to map out the approach?\n\n"
                f"(If you reply to this email the sequence stops - no more follow-ups.)\n\n"
                f"{sender}"
            ),
        },
        {
            "delay_days": 7,
            "subject":    f"One angle worth prioritising for {company}",
            "body": (
                f"Hi {first_name},\n\n"
                f"A thought on the ICP segments we built for {company}:\n\n"
                f"The quickest wins usually come from the segment closest to your existing "
                f"clients - the ones you already have results or case studies for. "
                f"Leading with familiar territory converts faster than a cold new segment.\n\n"
                f"Happy to walk through which segment fits that description for you on a quick call.\n\n"
                f"{sender}"
            ),
        },
        {
            "delay_days": 14,
            "subject":    "What is landing best right now",
            "body": (
                f"Hi {first_name},\n\n"
                f"Businesses that get the most from targeted lists tend to do one thing differently: "
                f"they build outreach around a single specific pain point rather than a general pitch.\n\n"
                f"It sounds simple but it is the difference between a 5% and a 20% reply rate.\n\n"
                f"If useful, I can share the messaging angles working best for your ICP segments right now.\n\n"
                f"{sender}"
            ),
        },
        {
            "delay_days": 21,
            "subject":    "Leaving the door open",
            "body": (
                f"Hi {first_name},\n\n"
                f"I know timing is not always right, and I do not want to keep nudging if now is not the moment.\n\n"
                f"The lead list and ICP analysis are yours whenever you are ready. "
                f"If you ever want to build a second list or revisit the strategy, just reply here.\n\n"
                f"Wishing you and the team at {company} the best.\n\n"
                f"{sender}"
            ),
        },
    ]


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def run_auto_pipeline(
    deal_id:   str,
    deal:      dict,
    auto_send: bool = False,
) -> dict:
    """
    Full pipeline for a deal that has ICP segments.

    Steps:
      0. Extract website intelligence (if not already done)
      1. Search Apollo across all ICP segments - merge leads
      2. Save leads to DB
      3. Generate personalised delivery email
      4. Generate 4-email follow-up sequence
      5. If auto_send=True: send email + CSV + schedule follow-ups

    Returns a results dict the frontend can use to pre-fill the Send Leads modal.
    """
    result: dict = {
        "deal_id":           deal_id,
        "steps":             {},
        "leads":             [],
        "csv_data":          "",
        "lead_count":        0,
        "delivery_email":    {},
        "followup_sequence": [],
        "sent":              False,
        "error":             None,
    }

    icp_data = deal.get("icp") or {}
    segments = (icp_data.get("segments", []) if isinstance(icp_data, dict) else [])
    if not segments:
        result["error"] = "No ICP segments found. Run ICP generation first."
        return result

    prospect = ProspectData(
        name         = deal.get("name", ""),
        email        = deal.get("email", ""),
        company      = deal.get("company", ""),
        domain       = deal.get("domain", ""),
        website      = deal.get("company_website", ""),
        job_title    = deal.get("job_title", ""),
        job_level    = deal.get("job_level", ""),
        location     = deal.get("location", ""),
        headcount    = deal.get("headcount", ""),
        industry     = deal.get("industry", ""),
        sub_industry = deal.get("sub_industry", ""),
        description  = deal.get("company_desc", ""),
        headline     = deal.get("headline", ""),
        department   = deal.get("department", ""),
    )
    reply = deal.get("reply_body") or ""

    # ── Step 0: Website extraction (if not already done) ──────────────────────
    website_intel = deal.get("website_intel")
    if not website_intel and (deal.get("company_website") or deal.get("domain")):
        await _set_status(deal_id, "website_extraction", "running")
        try:
            from app.services.website_extractor import extract_website_intel
            website_url = deal.get("company_website") or (
                "https://" + deal["domain"] if deal.get("domain") else ""
            )
            wi = await extract_website_intel(website_url, deal.get("company") or "")
            if wi:
                pool = await db.get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE deals SET website_intel=$1 WHERE id=$2",
                        json.dumps(wi), deal_id,
                    )
                website_intel = wi
                pages_crawled = len(wi.get("source_pages", []))
                result["steps"]["website_extraction"] = {
                    "status": "ok",
                    "pages":  wi.get("source_pages", []),
                }
                await _set_status(
                    deal_id, "website_extraction", "done",
                    f"{pages_crawled} page(s) crawled",
                )
                logger.info("Website extraction complete for deal %s: %d pages", deal_id, pages_crawled)
        except Exception as exc:
            logger.warning("Website extraction failed (non-critical): %s", exc)
            result["steps"]["website_extraction"] = {"status": "error", "error": str(exc)}
            await _set_status(deal_id, "website_extraction", "error", str(exc))
    else:
        if website_intel:
            result["steps"]["website_extraction"] = {"status": "ok", "reason": "already extracted"}
        else:
            result["steps"]["website_extraction"] = {"status": "skipped", "reason": "no website URL"}
        await _set_status(deal_id, "website_extraction", "done", "already extracted")

    # ── Step 1: Apollo search ─────────────────────────────────────────────────
    await _set_status(deal_id, "apollo_search", "running")
    if settings.apollo_api_key:
        try:
            leads = await search_leads_across_segments(segments, total_limit=100)
            result["leads"]      = leads
            result["lead_count"] = len(leads)

            if leads:
                run_id = await db.start_pipeline_run(deal_id)
                await db.save_leads(deal_id, run_id, leads)
                await db.finish_pipeline_run(run_id, status="complete")
                result["csv_data"] = leads_to_csv(leads)

            result["steps"]["apollo_search"] = {"status": "ok", "count": len(leads)}
            await _set_status(deal_id, "apollo_search", "done", f"{len(leads)} leads found")
            logger.info("Apollo search complete for deal %s: %d leads", deal_id, len(leads))
        except Exception as exc:
            logger.exception("Apollo search error for deal %s", deal_id)
            result["steps"]["apollo_search"] = {"status": "error", "error": str(exc)}
            await _set_status(deal_id, "apollo_search", "error", str(exc))
    else:
        result["steps"]["apollo_search"] = {"status": "skipped", "reason": "APOLLO_API_KEY not configured"}
        await _set_status(deal_id, "apollo_search", "skipped", "APOLLO_API_KEY not set")

    # ── Step 2: Generate delivery email ───────────────────────────────────────
    await _set_status(deal_id, "email_draft", "running")
    try:
        delivery = await generate_delivery_email(
            prospect, segments, reply, result["lead_count"],
            website_intel=website_intel if isinstance(website_intel, dict) else None,
        )
        result["delivery_email"] = delivery
        result["steps"]["email_draft"] = {"status": "ok"}
        await _set_status(deal_id, "email_draft", "done")
    except Exception as exc:
        logger.exception("Delivery email generation failed for deal %s", deal_id)
        result["steps"]["email_draft"] = {"status": "error", "error": str(exc)}
        await _set_status(deal_id, "email_draft", "error", str(exc))

    # ── Step 3: Generate follow-up sequence ───────────────────────────────────
    await _set_status(deal_id, "followup_gen", "running")
    try:
        followups = await generate_followup_sequence(prospect, segments, reply)
        result["followup_sequence"] = followups

        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE deals SET followup_draft=$1 WHERE id=$2",
                json.dumps(followups), deal_id,
            )

        result["steps"]["followup_gen"] = {"status": "ok", "count": len(followups)}
        await _set_status(deal_id, "followup_gen", "done", f"{len(followups)} emails generated")
        logger.info("Follow-up sequence generated for deal %s: %d emails", deal_id, len(followups))
    except Exception as exc:
        logger.exception("Follow-up generation failed for deal %s", deal_id)
        result["steps"]["followup_gen"] = {"status": "error", "error": str(exc)}
        await _set_status(deal_id, "followup_gen", "error", str(exc))

    # ── Step 4: Auto-send (only if explicitly requested) ──────────────────────
    if auto_send and result["delivery_email"] and deal.get("email"):
        await _set_status(deal_id, "email_send", "running")
        try:
            csv_filename = (
                f"{(deal.get('company') or 'leads').lower().replace(' ', '_')}_leads.csv"
            )
            send_result = await send_email_via_outlook(
                to_email     = deal["email"],
                to_name      = deal["name"],
                from_name    = settings.default_from_name or "Kayode",
                subject      = result["delivery_email"]["subject"],
                body         = result["delivery_email"]["body"],
                csv_data     = result["csv_data"] or None,
                csv_filename = csv_filename,
                deal_id      = deal_id,
            )
            if send_result.get("success"):
                result["sent"] = True
                result["steps"]["email_send"] = {"status": "ok"}
                await db.advance_deal_stage(deal_id, "delivered")
                await _set_status(deal_id, "email_send", "done")
                logger.info("Auto-pipeline email sent for deal %s", deal_id)
            else:
                err = send_result.get("error", "Send failed")
                result["steps"]["email_send"] = {"status": "error", "error": err}
                await _set_status(deal_id, "email_send", "error", err)
        except Exception as exc:
            logger.exception("Auto-send failed for deal %s", deal_id)
            result["steps"]["email_send"] = {"status": "error", "error": str(exc)}
            await _set_status(deal_id, "email_send", "error", str(exc))

        # ── Step 5: Schedule follow-ups ───────────────────────────────────────
        if result["sent"] and result["followup_sequence"]:
            await _set_status(deal_id, "followup_schedule", "running")
            try:
                from app.services.scheduler import calculate_send_at
                from app.services.database import schedule_email
                now_utc   = datetime.now(pytz.UTC)
                scheduled = []
                for i, step in enumerate(result["followup_sequence"]):
                    delay_days = int(step.get("delay_days") or ((i + 1) * 3))
                    send_at    = calculate_send_at(
                        now_utc, delay_days, "09:00", "Europe/London",
                        ["mon", "tue", "wed", "thu", "fri"],
                    )
                    eid = await schedule_email(
                        deal_id     = deal_id,
                        seq_id      = "auto_followup",
                        step_index  = i,
                        subject     = step.get("subject", ""),
                        body        = step.get("body", ""),
                        send_at_utc = send_at.isoformat(),
                        timezone    = "Europe/London",
                    )
                    scheduled.append({"step": i + 1, "send_at": send_at.isoformat(), "id": eid})
                result["steps"]["followup_schedule"] = {"status": "ok", "count": len(scheduled)}
                await _set_status(deal_id, "followup_schedule", "done", f"{len(scheduled)} scheduled")
                logger.info("Auto follow-ups scheduled for deal %s: %d emails", deal_id, len(scheduled))
            except Exception as exc:
                logger.exception("Follow-up scheduling failed for deal %s", deal_id)
                result["steps"]["followup_schedule"] = {"status": "error", "error": str(exc)}
                await _set_status(deal_id, "followup_schedule", "error", str(exc))

    return result
