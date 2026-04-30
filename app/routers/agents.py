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

    status_map = {
        "website_intel":    steps.get("website_extraction", {}).get("status", "idle"),
        "icp_generation":   "done" if deal.get("icp") else "idle",
        "lead_list":        steps.get("apollo_search", {}).get("status", "idle"),
        "email_draft":      steps.get("email_draft", {}).get("status", "idle"),
        "followup_sequence":steps.get("followup_gen", {}).get("status", "idle"),
        "pipeline":         "done" if all(
            steps.get(k, {}).get("status") in ("done", "ok", "skipped")
            for k in ("apollo_search", "email_draft", "followup_gen")
        ) and steps else "idle",
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
async def run_website_intel_agent(deal_id: str, background_tasks: BackgroundTasks):
    """
    Agent 1 — Website Intel Agent.
    Crawls up to 6 page types and extracts structured intelligence via Claude.
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
async def run_icp_agent(deal_id: str, background_tasks: BackgroundTasks):
    """
    Agent 2 — ICP Generation Agent.
    Generates 5 ICP segments from website intel, Instantly data, and reply context.
    """
    deal = await db.get_deal(deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    background_tasks.add_task(_icp_bg, deal_id, deal)
    return {
        "agent":   "icp_generation",
        "queued":  True,
        "deal_id": deal_id,
        "message": "ICP Generation Agent started.",
    }


async def _icp_bg(deal_id: str, deal: dict):
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
        reply    = deal.get("reply_body") or ""
        segments = await generate_icp_segments(prospect, reply, deal_id=deal_id)
        await db.set_deal_icp(deal_id, {"segments": segments})
        await db.advance_deal_stage(deal_id, "icp")
        logger.info("Agent 2 (ICP) complete for deal %s: %d segments", deal_id, len(segments))
    except Exception:
        logger.exception("Agent 2 (ICP) failed for deal %s", deal_id)


# ── Agent 3: Lead List (full agentic loop) ────────────────────────────────────

@router.post("/leads/{deal_id}")
async def run_lead_list_agent(deal_id: str, background_tasks: BackgroundTasks):
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

    background_tasks.add_task(_lead_list_bg, deal_id, deal, segments)
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


async def _lead_list_bg(deal_id: str, deal: dict, segments: list[dict]):
    from app.services.lead_list_agent import run_lead_list_agent
    try:
        await db._set_pipeline_step(deal_id, "apollo_search", "running")

        result = await run_lead_list_agent(
            segments         = segments,
            prospect_company = deal.get("company") or "",
            deal_id          = deal_id,
        )

        leads    = result["leads"]
        csv_data = result["csv_data"]

        if leads:
            run_id = await db.start_pipeline_run(deal_id)
            await db.save_leads(deal_id, run_id, leads)
            await db.finish_pipeline_run(run_id, status="complete")

        await db._set_pipeline_step(
            deal_id, "apollo_search", "done",
            f"{result['lead_count']} leads from {result['searches_performed']} searches "
            f"({', '.join(result['segments_searched'])})",
        )

        logger.info(
            "Agent 3 (lead list) complete for deal %s: %d leads, %d searches",
            deal_id, result["lead_count"], result["searches_performed"],
        )
    except Exception:
        logger.exception("Agent 3 (lead list) failed for deal %s", deal_id)
        await db._set_pipeline_step(deal_id, "apollo_search", "error", "Agent failed — check logs")


# ── Agent 4: Email Draft ──────────────────────────────────────────────────────

@router.post("/email/{deal_id}")
async def run_email_draft_agent(deal_id: str, background_tasks: BackgroundTasks):
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

    background_tasks.add_task(_email_draft_bg, deal_id, deal, segments)
    return {
        "agent":   "email_draft",
        "queued":  True,
        "deal_id": deal_id,
        "message": "Email Draft Agent started.",
    }


async def _email_draft_bg(deal_id: str, deal: dict, segments: list[dict]):
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
            prospect, segments, deal.get("reply_body") or "", lead_count, website_intel=wi
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
                "status": "done", "subject": draft.get("subject", ""), "ts": db.now_iso()
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
    Runs all agents in sequence: Website Intel → ICP → Leads → Email → Follow-ups.
    Delegates to the existing auto_pipeline service.
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

    auto_send = bool(body.get("auto_send", False))
    background_tasks.add_task(_pipeline_bg, deal_id, deal, auto_send)
    return {
        "agent":     "pipeline",
        "queued":    True,
        "deal_id":   deal_id,
        "auto_send": auto_send,
        "message":   "Pipeline Orchestrator started. Poll /api/agents/status/" + deal_id,
    }


async def _pipeline_bg(deal_id: str, deal: dict, auto_send: bool):
    from app.services.auto_pipeline import run_auto_pipeline
    try:
        result = await run_auto_pipeline(deal_id, deal, auto_send=auto_send)
        logger.info(
            "Agent 6 (pipeline) complete for deal %s — leads=%d sent=%s",
            deal_id, result.get("lead_count", 0), result.get("sent"),
        )
    except Exception:
        logger.exception("Agent 6 (pipeline) failed for deal %s", deal_id)
