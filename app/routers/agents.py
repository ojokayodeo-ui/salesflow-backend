"""
Agents Router — exposes all 6 PALM AI agents as named endpoints.

Agents:
  1. Website Intel Agent   POST /api/agents/website-intel/{deal_id}
  2. ICP Generation Agent  POST /api/agents/icp/{deal_id}
  3. Lead List Agent       POST /api/agents/leads/{deal_id}      ← full agentic loop
  4. Email Draft Agent     POST /api/agents/email/{deal_id}
  5. Follow-up Agent       POST /api/agents/followups/{deal_id}
  6. Pipeline Agent        POST /api/agents/pipeline/{deal_id}   (runs 1-5 in sequence)

GET /api/agents/                                returns agent registry + deal status
GET /api/agents/status/{deal_id}               returns per-agent status for a deal
"""

import json
import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.services import database as db
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Registry ──────────────────────────────────────────────────────────────────

AGENT_REGISTRY = [
    {
        "id":          "website_intel",
        "name":        "Website Intel Agent",
        "description": "Crawls the prospect's website (up to 6 page types) and extracts structured intelligence using Claude — industry, services, value proposition, target customers, positioning, keywords.",
        "trigger":     "Runs automatically on webhook receipt. Can also be triggered manually.",
        "endpoint":    "POST /api/agents/website-intel/{deal_id}",
    },
    {
        "id":          "icp_generation",
        "name":        "ICP Generation Agent",
        "description": "Generates exactly 5 Ideal Customer Profile segments grounded in real website data, Instantly.ai lead profile, and Apify market intelligence. Strict no-hallucination rule.",
        "trigger":     "Runs automatically after website intel. Can be re-triggered manually.",
        "endpoint":    "POST /api/agents/icp/{deal_id}",
    },
    {
        "id":          "lead_list",
        "name":        "Lead List Generation Agent",
        "description": "Agentic loop: Claude reasons about ICP segments, calls Apollo.io search tools, assesses result quality, refines filters if needed, and exports a verified lead list as CSV.",
        "trigger":     "Runs after ICP generation. Requires APOLLO_API_KEY.",
        "endpoint":    "POST /api/agents/leads/{deal_id}",
    },
    {
        "id":          "email_draft",
        "name":        "Email Draft Agent",
        "description": "Generates a personalised delivery email using website intel, ICP context, and lead count. Opens with a specific insight from the prospect's own website — no generic copy.",
        "trigger":     "Runs after lead list generation.",
        "endpoint":    "POST /api/agents/email/{deal_id}",
    },
    {
        "id":          "followup_sequence",
        "name":        "Follow-up & Nurture Agent",
        "description": "Generates a 4-email follow-up sequence (days 3, 7, 14, 21). Stops automatically on reply. Sequences: reminder + tip, strongest ICP angle, social proof, soft close.",
        "trigger":     "Runs after email draft generation.",
        "endpoint":    "POST /api/agents/followups/{deal_id}",
    },
    {
        "id":          "pipeline",
        "name":        "Pipeline Orchestrator",
        "description": "Runs all agents in sequence: Website Intel → ICP → Leads → Email → Follow-ups. Each step updates pipeline_status so the frontend can poll progress.",
        "trigger":     "Manual trigger or automatic after webhook ICP generation.",
        "endpoint":    "POST /api/agents/pipeline/{deal_id}",
    },
]


@router.get("/")
async def list_agents():
    """Return the full agent registry."""
    return {"agents": AGENT_REGISTRY, "count": len(AGENT_REGISTRY)}


@router.get("/status/{deal_id}")
async def agent_status(deal_id: str):
    """Return per-agent completion status for a deal."""
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    raw = deal.get("pipeline_status")
    steps = {}
    if raw:
        try:
            steps = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            pass

    icp_step_status = steps.get("icp_generation", {}).get("status")
    icp_status = icp_step_status if icp_step_status else ("done" if deal.get("icp") else "idle")

    status_map = {
        "website_intel":     steps.get("website_extraction", {}).get("status", "idle"),
        "icp_generation":    icp_status,
        "lead_list":         steps.get("apollo_search", {}).get("status", "idle"),
        "email_draft":       steps.get("email_draft", {}).get("status", "idle"),
        "followup_sequence": steps.get("followup_gen", {}).get("status", "idle"),
        "pipeline":          (
            "done" if steps.get("email_send", {}).get("status") in ("done", "ok")
            else "running" if any(
                steps.get(k, {}).get("status") == "running"
                for k in ("website_extraction", "icp_generation", "apollo_search",
                          "email_draft", "followup_gen", "email_send")
            ) and steps
            else "done" if all(
                steps.get(k, {}).get("status") in ("done", "ok", "skipped")
                for k in ("apollo_search", "email_draft", "followup_gen")
            ) and steps
            else "idle"
        ),
    }

    return {
        "deal_id":    deal_id,
        "company":    deal.get("company"),
        "agents":     status_map,
        "lead_count": len(await db.get_leads_for_deal(deal_id, approved_only=False)),
        "icp_ready":  bool(deal.get("icp")),
        "website_intel_ready": bool(deal.get("website_intel")),
        "followup_ready": bool(deal.get("followup_draft")),
    }


# ── Agent 1: Website Intel ────────────────────────────────────────────────────

@router.post("/website-intel/{deal_id}")
async def run_website_intel_agent(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Agent 1 — Website Intel Agent.
    Uses Perplexity.ai (if configured) as primary research source, falls back to web crawl.
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
            detail="No website URL on this deal. Add company_website first.",
        )

    background_tasks.add_task(_website_intel_bg, deal_id, website_url, deal.get("company") or "")
    return {
        "agent":   "website_intel",
        "queued":  True,
        "deal_id": deal_id,
        "website": website_url,
        "message": "Website Intel Agent started. Poll /api/agents/status/" + deal_id,
    }


async def _website_intel_bg(deal_id: str, website_url: str, company_name: str):
    from app.services.website_extractor import extract_website_intel
    try:
        intel = await extract_website_intel(website_url, company_name)
        pool  = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE deals SET website_intel=$1, updated_at=$2 WHERE id=$3",
                json.dumps(intel), db.now_iso(), deal_id,
            )
        logger.info("Agent 1 (website intel) complete for deal %s: status=%s pages=%s",
                    deal_id, intel.get("status"), intel.get("source_pages", []))
    except Exception:
        logger.exception("Agent 1 (website intel) failed for deal %s", deal_id)


# ── Agent 2: ICP Generation ───────────────────────────────────────────────────

@router.post("/icp/{deal_id}")
async def run_icp_agent(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Agent 2 — ICP Generation Agent.
    Generates 5 ICP segments from website intel, Instantly data, and reply context.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    training_notes = body.get("training_notes", "")
    background_tasks.add_task(_icp_bg, deal_id, deal, training_notes)
    return {
        "agent":   "icp_generation",
        "queued":  True,
        "deal_id": deal_id,
        "message": "ICP Generation Agent started.",
    }


async def _icp_bg(deal_id: str, deal: dict, training_notes: str = ""):
    from app.services.icp import generate_icp_segments
    from app.models.schemas import ProspectData
    try:
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

        # Use already-stored website intel — avoids redundant crawl and ensures
        # ICP is grounded in the same real data the Website Intel Agent extracted
        stored_intel = deal.get("website_intel")
        existing_wi  = stored_intel if isinstance(stored_intel, dict) else None

        segments = await generate_icp_segments(
            prospect,
            reply,
            deal_id                = deal_id,
            existing_website_intel = existing_wi,
            training_notes         = training_notes,
        )
        await db.set_deal_icp(deal_id, {"segments": segments})
        await db.advance_deal_stage(deal_id, "icp")
        logger.info("Agent 2 (ICP) complete for deal %s: %d segments", deal_id, len(segments))
    except Exception:
        logger.exception("Agent 2 (ICP) failed for deal %s", deal_id)


# ── Agent 3: Lead List (full agentic loop) ────────────────────────────────────

@router.post("/leads/{deal_id}")
async def run_lead_list_agent(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Agent 3 — Lead List Generation Agent.

    Full agentic loop: Claude uses tool-calling to search Apollo.io across ICP
    segments, assesses result quality, refines filters if needed, and produces
    a curated, deduplicated lead list exported as CSV.

    Requires: ICP segments on the deal + APOLLO_API_KEY configured.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    icp_data = deal.get("icp") or {}
    segments = icp_data.get("segments", []) if isinstance(icp_data, dict) else []
    if not segments:
        raise HTTPException(
            status_code=400,
            detail="No ICP segments found. Run the ICP Generation Agent first.",
        )

    if not settings.apollo_api_key:
        raise HTTPException(
            status_code=503,
            detail="APOLLO_API_KEY not configured. Add it to your environment variables.",
        )

    training_notes      = body.get("training_notes", "")
    lead_count_override = body.get("lead_count")

    background_tasks.add_task(_lead_list_bg, deal_id, deal, segments, training_notes, lead_count_override)
    return {
        "agent":     "lead_list",
        "queued":    True,
        "deal_id":   deal_id,
        "segments":  len(segments),
        "message":   (
            f"Lead List Agent started — searching Apollo.io across {len(segments)} ICP segments. "
            "Poll /api/agents/status/" + deal_id + " for progress."
        ),
    }


async def _lead_list_bg(
    deal_id: str,
    deal: dict,
    segments: list[dict],
    training_notes: str = "",
    lead_count_override=None,
):
    from app.services.lead_list_agent import run_lead_list_agent
    from app.services.auto_pipeline import search_leads_across_segments
    from app.services.apollo import leads_to_csv

    await db._set_pipeline_step(deal_id, "apollo_search", "running")
    cfg = await db.get_pipeline_config()
    target_count = int(lead_count_override) if lead_count_override else cfg["lead_count"]
    leads: list[dict] = []
    detail = ""

    # ── Try agentic approach first ────────────────────────────────────────────
    try:
        result = await run_lead_list_agent(
            segments         = segments,
            prospect_company = deal.get("company") or "",
            deal_id          = deal_id,
            target_count     = target_count,
            training_notes   = training_notes,
        )
        leads  = result["leads"]
        detail = (
            f"{result['lead_count']} leads from {result['searches_performed']} searches "
            f"({', '.join(result['segments_searched'])})"
        )
        logger.info("Agent 3 (agentic) complete for deal %s: %d leads", deal_id, len(leads))
    except Exception as exc:
        logger.warning("Agent 3 agentic approach failed for deal %s: %s — falling back to direct search", deal_id, exc)

    # ── Fallback: direct segment search if agentic returned nothing ───────────
    if not leads:
        try:
            logger.info("Agent 3 (direct fallback): searching %d segments for deal %s", len(segments), deal_id)
            leads = await search_leads_across_segments(segments, total_limit=target_count)
            detail = f"{len(leads)} leads via direct segment search"
            logger.info("Agent 3 (direct fallback) complete for deal %s: %d leads", deal_id, len(leads))
        except Exception as exc:
            logger.exception("Agent 3 direct fallback also failed for deal %s: %s", deal_id, exc)
            await db._set_pipeline_step(deal_id, "apollo_search", "error",
                                        f"Apollo search failed: {exc}")
            return

    # ── Save results ──────────────────────────────────────────────────────────
    try:
        if leads:
            run_id = await db.start_pipeline_run(deal_id)
            await db.save_leads(deal_id, run_id, leads)
            await db.finish_pipeline_run(run_id, status="complete")

        await db._set_pipeline_step(
            deal_id, "apollo_search", "done",
            detail or f"{len(leads)} leads found",
        )
    except Exception as exc:
        logger.exception("Agent 3 save failed for deal %s", deal_id)
        await db._set_pipeline_step(deal_id, "apollo_search", "error", f"Save failed: {exc}")


# ── Agent 4: Email Draft ──────────────────────────────────────────────────────

@router.post("/email/{deal_id}")
async def run_email_draft_agent(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Agent 4 — Email Draft Agent.
    Generates a personalised delivery email using website intel, ICP context,
    and lead count. Must be personalized only on extracted real data.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    icp_data = deal.get("icp") or {}
    segments = icp_data.get("segments", []) if isinstance(icp_data, dict) else []
    if not segments:
        raise HTTPException(status_code=400, detail="No ICP segments. Run ICP Agent first.")

    training_notes = body.get("training_notes", "")
    background_tasks.add_task(_email_draft_bg, deal_id, deal, segments, training_notes)
    return {
        "agent":   "email_draft",
        "queued":  True,
        "deal_id": deal_id,
        "message": "Email Draft Agent started.",
    }


async def _email_draft_bg(deal_id: str, deal: dict, segments: list[dict], training_notes: str = ""):
    from app.services.auto_pipeline import generate_delivery_email
    from app.models.schemas import ProspectData
    try:
        await db._set_pipeline_step(deal_id, "email_draft", "running")
        prospect = ProspectData(
            name     = deal.get("name", ""),
            email    = deal.get("email", ""),
            company  = deal.get("company", ""),
            industry = deal.get("industry", ""),
            job_title= deal.get("job_title", ""),
        )
        lead_count   = len(await db.get_leads_for_deal(deal_id, approved_only=False))
        website_intel = deal.get("website_intel")
        wi = website_intel if isinstance(website_intel, dict) else None

        draft = await generate_delivery_email(
            prospect, segments, deal.get("reply_body") or "", lead_count,
            website_intel=wi, training_notes=training_notes,
        )

        pool = await db.get_pool()
        async with pool.acquire() as conn:
            current_status = {}
            row = await conn.fetchrow("SELECT pipeline_status FROM deals WHERE id=$1", deal_id)
            if row and row["pipeline_status"]:
                try:
                    current_status = json.loads(row["pipeline_status"])
                except Exception:
                    pass
            current_status["email_draft"] = {
                "status": "done",
                "subject": draft.get("subject", ""),
                "body":    draft.get("body", ""),
                "ts":      db.now_iso(),
            }
            await conn.execute(
                "UPDATE deals SET pipeline_status=$1 WHERE id=$2",
                json.dumps(current_status), deal_id,
            )

        logger.info("Agent 4 (email draft) complete for deal %s", deal_id)
    except Exception:
        logger.exception("Agent 4 (email draft) failed for deal %s", deal_id)
        await db._set_pipeline_step(deal_id, "email_draft", "error", "Agent failed — check logs")


# ── Agent 5: Follow-up Sequence ───────────────────────────────────────────────

@router.post("/followups/{deal_id}")
async def run_followup_agent(deal_id: str, background_tasks: BackgroundTasks):
    """
    Agent 5 — Follow-up & Nurture Agent.
    Generates a 4-email follow-up sequence (days 3, 7, 14, 21).
    Stores to followup_draft on the deal for frontend review before sending.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    icp_data = deal.get("icp") or {}
    segments = icp_data.get("segments", []) if isinstance(icp_data, dict) else []
    if not segments:
        raise HTTPException(status_code=400, detail="No ICP segments. Run ICP Agent first.")

    background_tasks.add_task(_followup_bg, deal_id, deal, segments)
    return {
        "agent":   "followup_sequence",
        "queued":  True,
        "deal_id": deal_id,
        "message": "Follow-up Agent started — generating 4-email sequence.",
    }


async def _followup_bg(deal_id: str, deal: dict, segments: list[dict]):
    from app.services.auto_pipeline import generate_followup_sequence
    from app.models.schemas import ProspectData
    try:
        await db._set_pipeline_step(deal_id, "followup_gen", "running")
        prospect = ProspectData(
            name     = deal.get("name", ""),
            email    = deal.get("email", ""),
            company  = deal.get("company", ""),
            industry = deal.get("industry", ""),
        )
        followups = await generate_followup_sequence(prospect, segments, deal.get("reply_body") or "")

        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE deals SET followup_draft=$1 WHERE id=$2",
                json.dumps(followups), deal_id,
            )

        await db._set_pipeline_step(
            deal_id, "followup_gen", "done", f"{len(followups)} emails generated"
        )
        logger.info("Agent 5 (followups) complete for deal %s: %d emails", deal_id, len(followups))
    except Exception:
        logger.exception("Agent 5 (followups) failed for deal %s", deal_id)
        await db._set_pipeline_step(deal_id, "followup_gen", "error", "Agent failed — check logs")


# ── Agent 6: Pipeline Orchestrator ───────────────────────────────────────────

@router.post("/pipeline/{deal_id}")
async def run_pipeline_agent(deal_id: str, background_tasks: BackgroundTasks, body: dict = {}):
    """
    Agent 6 — Pipeline Orchestrator.
    Runs the FULL chain from scratch: Website Intel → ICP → Leads → Email → Follow-ups.
    Works on any deal — ICP segments do not need to exist beforehand.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    auto_send           = bool(body.get("auto_send", True))
    training_notes      = body.get("training_notes", "")
    lead_count_override = body.get("lead_count")
    background_tasks.add_task(_pipeline_bg, deal_id, deal, auto_send, training_notes, lead_count_override)
    return {
        "agent":     "pipeline",
        "queued":    True,
        "deal_id":   deal_id,
        "auto_send": auto_send,
        "message":   "Pipeline Orchestrator started — Website Intel → ICP → Leads → Email → Follow-ups. Poll /api/agents/status/" + deal_id,
    }


async def _pipeline_bg(
    deal_id: str,
    deal: dict,
    auto_send: bool,
    training_notes: str = "",
    lead_count_override=None,
):
    """
    Full pipeline from scratch.
    Step 1: Website Intel (if not already successfully extracted)
    Step 2: ICP Generation (always runs fresh, uses stored intel if available)
    Step 3: Leads → Email → Follow-ups (via run_auto_pipeline)
    """
    from app.services.icp import generate_icp_segments
    from app.services.auto_pipeline import run_auto_pipeline
    from app.models.schemas import ProspectData

    try:
        # ── Step 1: Website Intel ─────────────────────────────────────────────
        stored_intel = deal.get("website_intel")
        has_intel    = isinstance(stored_intel, dict) and stored_intel.get("status") == "success"

        if not has_intel:
            website_url = deal.get("company_website") or (
                "https://" + deal["domain"] if deal.get("domain") else ""
            )
            if website_url:
                await db._set_pipeline_step(deal_id, "website_extraction", "running")
                await _website_intel_bg(deal_id, website_url, deal.get("company") or "")
                deal = await db.get_deal(deal_id) or deal
                stored_intel = deal.get("website_intel")
                has_intel    = isinstance(stored_intel, dict) and stored_intel.get("status") == "success"
            else:
                await db._set_pipeline_step(deal_id, "website_extraction", "skipped", "no website URL")

        # ── Step 2: ICP Generation ────────────────────────────────────────────
        await db._set_pipeline_step(deal_id, "icp_generation", "running")
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
        existing_wi = stored_intel if isinstance(stored_intel, dict) else None
        segments    = await generate_icp_segments(
            prospect,
            deal.get("reply_body") or "",
            deal_id                = deal_id,
            existing_website_intel = existing_wi,
            training_notes         = training_notes,
        )
        await db.set_deal_icp(deal_id, {"segments": segments})
        await db.advance_deal_stage(deal_id, "icp")
        await db._set_pipeline_step(deal_id, "icp_generation", "done", f"{len(segments)} segments")
        logger.info("Agent 6 step 2 (ICP) complete for deal %s: %d segments", deal_id, len(segments))

        # ── Step 3: Leads → Email → Follow-ups ───────────────────────────────
        deal = await db.get_deal(deal_id) or deal
        result = await run_auto_pipeline(
            deal_id,
            deal,
            auto_send      = auto_send,
            training_notes = training_notes,
            lead_count_override = lead_count_override,
        )
        logger.info(
            "Agent 6 (pipeline) complete for deal %s — leads=%d sent=%s",
            deal_id, result.get("lead_count", 0), result.get("sent"),
        )

    except Exception:
        logger.exception("Agent 6 (pipeline) failed for deal %s", deal_id)
        await db._set_pipeline_step(deal_id, "icp_generation", "error", "Pipeline failed — check logs")


# ── Agents Lab helpers ────────────────────────────────────────────────────────

@router.post("/test-deal")
async def create_test_deal(body: dict):
    """
    Create a quick test deal for the Agents Lab without needing Instantly.
    Returns existing deal if the email already has one.
    """
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=422, detail="email is required")

    existing = await db.get_deal_by_email(email)
    if existing:
        force_reset = body.get("force_reset", False)
        if force_reset:
            # Wipe all agent outputs so the deal can be run fresh
            pool = await db.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """UPDATE deals
                       SET website_intel=NULL, icp_data=NULL, leads=NULL,
                           delivery_email=NULL, follow_ups=NULL,
                           pipeline_status=NULL, pipeline_steps=NULL,
                           stage='new', updated_at=$1
                       WHERE id=$2""",
                    db.now_iso(), existing["id"],
                )
            logger.info("Test deal %s reset for fresh run", existing["id"])
            return {
                "deal_id": existing["id"],
                "created": False,
                "message": f"Deal reset — ready for a fresh run ({existing.get('company') or email})",
            }
        return {
            "deal_id": existing["id"],
            "created": False,
            "message": f"Deal already exists for {existing.get('company') or email}",
        }

    raw_name    = (body.get("name") or "").strip()
    raw_company = (body.get("company") or "").strip()
    website     = (body.get("website") or "").strip()
    domain      = email.split("@")[1] if "@" in email else ""
    name        = raw_name    or email.split("@")[0].replace(".", " ").title()
    company     = raw_company or domain.split(".")[0].title()

    deal = await db.create_deal(
        name            = name,
        email           = email,
        company         = company,
        domain          = domain,
        campaign        = "Agents Lab Test",
        reply_body      = body.get("reply") or "Yes, sounds interesting — tell me more about what you offer.",
        job_title       = body.get("job_title") or "",
        job_level       = "",
        department      = "",
        linkedin        = "",
        location        = body.get("location") or "",
        headcount       = "",
        industry        = body.get("industry") or "",
        sub_industry    = "",
        company_website = website,
        company_desc    = "",
        headline        = "",
        reply_subject   = "Re: Quick question",
    )
    deal_id = deal["id"]

    # Auto-kick website extraction in background if URL provided —
    # so intel is ready by the time the user hits Run on any agent
    if website:
        import asyncio as _asyncio
        _asyncio.create_task(_website_intel_bg(deal_id, website, company))
        logger.info("test-deal: website extraction queued for %s (%s)", company, website)

    return {
        "deal_id": deal_id,
        "created": True,
        "message": f"Test deal created for {company}" + (" — extracting website intel..." if website else ""),
        "website_extraction_queued": bool(website),
    }


@router.get("/outputs/{deal_id}")
async def get_agent_outputs(deal_id: str):
    """
    Return all current agent outputs for a deal in one call.
    Used by the Agents Lab to populate output previews after each run.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    pipeline_steps: dict = {}
    raw = deal.get("pipeline_status")
    if raw:
        try:
            pipeline_steps = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            pass

    icp_data = deal.get("icp") or {}
    segments = icp_data.get("segments", []) if isinstance(icp_data, dict) else []

    leads = await db.get_leads_for_deal(deal_id, approved_only=False)

    followups: list = []
    raw_fu = deal.get("followup_draft")
    if raw_fu:
        try:
            followups = json.loads(raw_fu) if isinstance(raw_fu, str) else raw_fu
        except Exception:
            pass

    email_step  = pipeline_steps.get("email_draft", {})
    send_step   = pipeline_steps.get("email_send", {})
    email_sent  = send_step.get("status") in ("done", "ok")

    from app.services.apollo import leads_to_csv
    csv_data = leads_to_csv(leads) if leads else ""

    return {
        "deal_id":           deal_id,
        "company":           deal.get("company"),
        "name":              deal.get("name"),
        "email":             deal.get("email"),
        "website_intel":     deal.get("website_intel"),
        "icp_segments":      segments,
        "lead_count":        len(leads),
        "leads_sample":      leads[:5],
        "csv_data":          csv_data,
        "email_subject":     email_step.get("subject", ""),
        "email_body":        email_step.get("body", ""),
        "email_sent":        email_sent,
        "followup_sequence": followups,
        "pipeline_steps":    pipeline_steps,
    }


# ── Apollo connection test ────────────────────────────────────────────────────

@router.get("/debug/apollo")
async def debug_apollo_connection():
    """
    Quick Apollo.io connection test — runs a minimal search and returns raw results.
    Use this to verify the API key works and see what fields Apollo actually returns.
    """
    if not settings.apollo_api_key:
        return {"ok": False, "error": "APOLLO_API_KEY is not set in environment"}

    import httpx as _httpx
    APOLLO_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
    payload = {
        "per_page": 5,
        "page":     1,
        "person_titles[]":        ["Managing Director", "CEO", "Founder"],
        "person_locations[]":     ["United Kingdom"],
        "contact_email_status[]": ["verified", "likely_to_engage"],
    }
    headers = {
        "Content-Type":  "application/json",
        "X-Api-Key":     settings.apollo_api_key,
        "Cache-Control": "no-cache",
    }
    try:
        async with _httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(APOLLO_URL, json=payload, headers=headers)
        if resp.status_code != 200:
            return {"ok": False, "http_status": resp.status_code, "error": resp.text[:300]}
        raw = resp.json()
        people = raw.get("people", [])
        before_email = sum(1 for p in people if p.get("email"))

        # Run the reveal step so the Test button shows post-reveal results
        from app.services.apollo import _make_headers, _reveal_emails_batch, _build_lead
        if people:
            people = await _reveal_emails_batch(people, headers, max_reveal=3)

        after_email = sum(1 for p in people if p.get("email"))
        sample = []
        for p in people[:5]:
            sample.append({
                "name":         p.get("name"),
                "title":        p.get("title"),
                "email":        p.get("email") or None,
                "email_status": p.get("email_status") or "—",
                "city":         p.get("city"),
                "country":      p.get("country"),
            })
        return {
            "ok":                    True,
            "http_status":           resp.status_code,
            "total_entries":         raw.get("pagination", {}).get("total_entries", 0),
            "people_returned":       len(people),
            "people_with_email_before_reveal": before_email,
            "people_with_email":     after_email,
            "reveal_step_working":   after_email > before_email,
            "sample":                sample,
            "api_key_prefix":        settings.apollo_api_key[:6] + "..." if settings.apollo_api_key else "",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.get("/debug/perplexity")
async def debug_perplexity_connection():
    """Quick test of Perplexity.ai connection."""
    if not settings.perplexity_api_key:
        return {"ok": False, "error": "PERPLEXITY_API_KEY is not set in environment"}
    try:
        from app.services.perplexity_intel import research_company_with_perplexity
        result = await research_company_with_perplexity(
            "https://anthropic.com", "Anthropic"
        )
        if result and result.get("status") == "success":
            return {
                "ok": True,
                "api_key_prefix": settings.perplexity_api_key[:8] + "...",
                "industry":       result.get("industry", ""),
                "services":       str(result.get("services", ""))[:100],
            }
        return {"ok": False, "error": "No result returned from Perplexity"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ── Agent Configs (persistent training notes) ─────────────────────────────────

@router.get("/configs")
async def get_agent_configs():
    """Return all saved agent training notes."""
    configs = await db.get_all_agent_configs()
    return {"configs": configs}


@router.post("/configs/{agent_id}")
async def save_agent_config(agent_id: str, body: dict = {}):
    """Save training notes for a specific agent."""
    valid_ids = {"website-intel", "icp", "leads", "email", "followups", "pipeline"}
    if agent_id not in valid_ids:
        raise HTTPException(status_code=400, detail=f"Unknown agent: {agent_id}")
    training_notes = body.get("training_notes", "")
    result = await db.save_agent_config(agent_id, training_notes)
    return result
