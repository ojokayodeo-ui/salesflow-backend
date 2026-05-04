"""
Lead List Generation Agent

A proper Claude agent that uses tool-calling to intelligently search Apollo.io.
Unlike a direct API call, this agent:
  - Reasons about which ICP segments to prioritise
  - Decides Apollo filter combinations for each segment
  - Reads results, assesses quality, and adapts filters if needed
  - Deduplicates across segments
  - Produces a curated lead list with a strategic outreach summary

Agentic loop: Claude decides → tool executes Apollo search → Claude sees results
              → refines if poor → calls finalize when satisfied (max 10 turns)
"""

import json
import logging
import httpx

from app.config import settings
from app.services.apollo import leads_to_csv

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL   = "claude-sonnet-4-6"
MAX_TURNS      = 10

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"

TOOLS = [
    {
        "name": "search_apollo",
        "description": (
            "Search Apollo.io for B2B contacts matching the given criteria. "
            "Call once per ICP segment. The export will contain only verified emails. "
            "Each call is an API credit — be specific to maximise quality. "
            "If results are 0, retry with fewer filters (drop industries or keywords first, then employee range)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "segment_name": {
                    "type": "string",
                    "description": "ICP segment label (used for tracking which segment each lead came from)"
                },
                "job_titles": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Exact Apollo-style job titles e.g. ['Managing Director', 'Head of Sales', 'Founder']. Use 3-5 titles."
                },
                "seniority_levels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Apollo seniority values — use ONLY: 'owner', 'founder', 'c_suite', 'partner', 'vp', 'head', 'director', 'manager', 'senior', 'entry', 'intern'. Pick 2-4."
                },
                "industries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Industry names as Apollo labels them e.g. ['Management Consulting', 'Staffing and Recruiting', 'Financial Services']. Use 1-3. Drop this filter first if results are 0."
                },
                "company_size": {
                    "type": "object",
                    "description": "Employee count range for target companies",
                    "properties": {
                        "min": {"type": "integer", "description": "Minimum employees"},
                        "max": {"type": "integer", "description": "Maximum employees"}
                    }
                },
                "locations": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Country names e.g. ['United Kingdom', 'Ireland']. Use exact country names Apollo expects."
                },
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Additional keyword tags for company matching. Use 1-3 max — over-filtering reduces results. Drop this if results are low."
                },
                "limit": {
                    "type": "integer",
                    "description": "Max contacts to fetch (10-50). Default 30."
                }
            },
            "required": ["segment_name", "job_titles", "locations"]
        }
    },
    {
        "name": "finalize_leads",
        "description": (
            "Call this when you have finished all Apollo searches and are satisfied with the results. "
            "The system will compile, deduplicate, and export the final lead list. "
            "Provide a quality summary and outreach recommendation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "What was searched, how many leads found per segment, and overall quality assessment"
                },
                "recommended_approach": {
                    "type": "string",
                    "description": "Strategic recommendation for how to approach outreach to this specific list — tone, hook, prioritisation"
                },
                "strongest_segment": {
                    "type": "string",
                    "description": "Name of the ICP segment that produced the best quality leads"
                }
            },
            "required": ["summary"]
        }
    }
]


async def _execute_apollo_search(
    job_titles: list[str],
    locations: list[str],
    seniority_levels: list[str] | None = None,
    industries: list[str] | None = None,
    employee_min: int | None = None,
    employee_max: int | None = None,
    keywords: list[str] | None = None,
    limit: int = 30,
) -> tuple[list[dict], int]:
    """
    Execute a real Apollo.io API search using the 6 core filter dimensions.
    No email-status filter on search — contacts are revealed via people/match.
    Only verified emails survive into the final export.
    Returns (leads_with_verified_email, total_available).
    """
    from app.services.apollo import _make_headers, _apollo_search

    headers = _make_headers()

    # Build keyword tag list: industries + extra keywords
    keyword_tags = []
    if industries:
        keyword_tags.extend(industries[:3])
    if keywords:
        keyword_tags.extend(keywords[:3])

    payload: dict = {
        "per_page":           min(limit, 50),
        "page":               1,
        "person_titles[]":    job_titles[:5],
        "person_locations[]": locations,
    }

    if seniority_levels:
        payload["person_seniorities[]"] = seniority_levels[:4]

    if employee_min and employee_max and 0 < employee_min < employee_max <= 50000:
        payload["organization_num_employees_ranges[]"] = [f"{employee_min},{employee_max}"]

    if keyword_tags:
        payload["q_organization_keyword_tags[]"] = keyword_tags[:6]

    leads, total = await _apollo_search(payload, headers, max_reveal=limit)

    # Retry 1: drop industry/keyword tags
    if not leads and keyword_tags:
        logger.info("Apollo: retrying without keyword/industry tags")
        payload.pop("q_organization_keyword_tags[]", None)
        leads, total = await _apollo_search(payload, headers, max_reveal=limit)

    # Retry 2: drop employee range too
    if not leads and (employee_min or employee_max):
        logger.info("Apollo: retrying without employee range")
        payload.pop("organization_num_employees_ranges[]", None)
        leads, total = await _apollo_search(payload, headers, max_reveal=limit)

    # Retry 3: drop seniority too (titles + location only)
    if not leads and seniority_levels:
        logger.info("Apollo: retrying with titles + location only")
        payload.pop("person_seniorities[]", None)
        leads, total = await _apollo_search(payload, headers, max_reveal=limit)

    logger.info("Apollo search complete: %d verified leads (total available: %s)", len(leads), total)
    return leads, total


async def run_lead_list_agent(
    segments: list[dict],
    prospect_company: str = "",
    deal_id: str = "",
    target_count: int = 100,
    training_notes: str = "",
) -> dict:
    """
    Run the Lead List Generation Agent.

    Claude uses tool-calling to search Apollo.io across ICP segments,
    reason about result quality, adapt filters, and produce a curated list.

    Returns:
      {
        "leads": [...],
        "lead_count": int,
        "csv_data": str,
        "summary": str,
        "recommended_approach": str,
        "strongest_segment": str,
        "searches_performed": int,
        "segments_searched": [str],
      }
    """
    if not settings.anthropic_api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not configured")
    if not settings.apollo_api_key:
        raise RuntimeError("APOLLO_API_KEY not configured — cannot search Apollo.io")
    if not segments:
        raise ValueError("No ICP segments provided")

    # Build segment context for the agent
    seg_lines = []
    for i, seg in enumerate(segments[:5], 1):
        titles     = seg.get("job_titles") or seg.get("target_titles") or []
        seniority  = seg.get("seniority_levels") or []
        industries = seg.get("industries") or ([seg["industry"]] if seg.get("industry") else [])
        locations  = seg.get("locations") or ([seg["hq_country"]] if seg.get("hq_country") else ["United Kingdom"])
        seg_lines.append(
            f"Segment {i} — {seg.get('segment_name', f'Segment {i}')}\n"
            f"  Job titles:      {', '.join(titles)}\n"
            f"  Seniority:       {', '.join(seniority) or 'not specified'}\n"
            f"  Industries:      {', '.join(industries)}\n"
            f"  Company size:    {seg.get('company_size', 'any')} "
            f"(min={seg.get('employee_min', '')}, max={seg.get('employee_max', '')})\n"
            f"  Locations:       {', '.join(locations)}\n"
            f"  Keywords:        {', '.join(seg.get('keywords') or [])}\n"
            f"  Pain point:      {seg.get('pain_point', '')}\n"
            f"  Buying signal:   {seg.get('buying_signal', '')}"
        )
    seg_context = "\n\n".join(seg_lines)

    per_seg_target = max(25, target_count // min(len(segments), 3))
    training_block = (
        f"\n\nADDITIONAL INSTRUCTIONS FROM USER:\n{training_notes.strip()}"
        if training_notes and training_notes.strip() else ""
    )

    # Load swipe files as targeting reference
    swipe_block = ""
    try:
        from app.services.swipe_context import build_swipe_context
        swipe_ctx = await build_swipe_context(limit=5, chars_per_file=600)
        if swipe_ctx:
            swipe_block = "\n\n" + swipe_ctx
    except Exception:
        pass

    system = f"""You are the Lead List Generation Agent for PALM — a B2B outbound system.

Your job: search Apollo.io to build a high-quality lead list for {prospect_company or 'the prospect'} using the ICP segments below.

STRATEGY:
- You have {len(segments)} ICP segments. Choose the 3 with the clearest, most searchable targeting data.
- For each chosen segment, call search_apollo using all 6 filter dimensions from the segment:
    job_titles       → exact Apollo-style titles (3-5 titles)
    seniority_levels → Apollo seniority values: owner, founder, c_suite, partner, vp, head, director, manager, senior
    industries       → industry names as Apollo labels them (1-3)
    company_size     → {{ "min": X, "max": Y }} from employee range
    locations        → country names (default: United Kingdom)
    keywords         → extra keyword tags (1-3, use sparingly)
- Target {per_seg_target} leads per segment. Total goal: {target_count} unique verified leads.
- After searching your chosen segments, call finalize_leads with your summary.

QUALITY RULES:
- The search is broad — email reveal runs automatically. Only VERIFIED emails make the final export.
- If a search returns 0 results, retry with fewer filters in this order:
    1. Drop keywords
    2. Drop industries
    3. Drop company_size
    4. Drop seniority_levels (titles + location only as last resort)
- If a segment still returns 0 after all retries, skip it and note why.
- Prioritise relevance over volume. A tight list of 80 well-targeted leads beats 200 generic ones.

ICP SEGMENTS:
{seg_context}{swipe_block}{training_block}

Begin by choosing which 3 segments to search, then execute the searches."""

    messages = [
        {
            "role": "user",
            "content": (
                f"Please build the Apollo.io lead list for {prospect_company or 'this prospect'}. "
                f"Analyse the {len(segments)} ICP segments, choose the 3 best to search, "
                f"run the Apollo searches, and finalize when you have a quality list."
            )
        }
    ]

    all_leads:       list[dict] = []
    seen_emails:     set[str]   = set()
    segments_searched: list[str] = []
    searches_performed           = 0
    agent_summary                = ""
    recommended_approach         = ""
    strongest_segment            = ""

    # ── Agentic loop ────────────────────────────────────────────────────────────
    for turn in range(MAX_TURNS):
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key":         settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      CLAUDE_MODEL,
                    "max_tokens": 1024,
                    "system":     system,
                    "tools":      TOOLS,
                    "messages":   messages,
                },
            )
            resp.raise_for_status()

        data        = resp.json()
        stop_reason = data.get("stop_reason")
        content     = data.get("content", [])

        messages.append({"role": "assistant", "content": content})

        tool_results: list[dict] = []
        finished = False

        for block in content:
            if block.get("type") != "tool_use":
                continue

            tool_name     = block["name"]
            tool_input    = block.get("input", {})
            tool_use_id   = block["id"]

            # ── search_apollo ────────────────────────────────────────────────
            if tool_name == "search_apollo":
                seg_name    = tool_input.get("segment_name", f"Segment {searches_performed + 1}")
                titles      = tool_input.get("job_titles", tool_input.get("person_titles", []))
                locs        = tool_input.get("locations", tool_input.get("person_locations", ["United Kingdom"]))
                seniorities = tool_input.get("seniority_levels", [])
                industries  = tool_input.get("industries", [])
                size        = tool_input.get("company_size", {})
                emp_min     = size.get("min") if isinstance(size, dict) else tool_input.get("employee_min")
                emp_max     = size.get("max") if isinstance(size, dict) else tool_input.get("employee_max")
                kws         = tool_input.get("keywords", [])
                limit       = min(int(tool_input.get("limit") or 30), 50)

                logger.info(
                    "Lead agent [turn %d]: searching '%s' — titles=%s seniority=%s industries=%s loc=%s emp=%s-%s kw=%s limit=%d",
                    turn + 1, seg_name, titles, seniorities, industries, locs, emp_min, emp_max, kws, limit,
                )

                try:
                    leads, total_available = await _execute_apollo_search(
                        job_titles       = titles,
                        locations        = locs,
                        seniority_levels = seniorities,
                        industries       = industries,
                        employee_min     = emp_min,
                        employee_max     = emp_max,
                        keywords         = kws,
                        limit            = limit,
                    )
                    searches_performed += 1

                    new_count = 0
                    for lead in leads:
                        key = (lead.get("email") or "").lower().strip()
                        if key and key not in seen_emails:
                            seen_emails.add(key)
                            lead["icp_segment"] = seg_name
                            all_leads.append(lead)
                            new_count += 1

                    if seg_name not in segments_searched:
                        segments_searched.append(seg_name)

                    # Give Claude a meaningful result summary + sample
                    sample_lines = "\n".join(
                        f"  {l.get('full_name','?')} | {l.get('title','?')} @ "
                        f"{l.get('company','?')} ({l.get('city','')}, {l.get('country','')})"
                        for l in leads[:4]
                    )
                    result_msg = (
                        f"Apollo search complete for '{seg_name}'.\n"
                        f"  Returned: {len(leads)} contacts ({total_available} total available in Apollo)\n"
                        f"  New unique leads added: {new_count}\n"
                        f"  Running total: {len(all_leads)} unique leads\n"
                    )
                    if sample_lines:
                        result_msg += f"\nSample contacts:\n{sample_lines}"
                    if len(leads) < 10:
                        result_msg += (
                            "\n\nNOTE: Low result count. Consider retrying with fewer filters "
                            "(remove keywords or employee range) or move to the next segment."
                        )

                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": tool_use_id,
                        "content":     result_msg,
                    })

                except Exception as exc:
                    logger.warning("Apollo search failed in agent [turn %d]: %s", turn + 1, exc)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": tool_use_id,
                        "content":     f"Search failed: {exc}. Try adjusting filters or skip this segment.",
                        "is_error":    True,
                    })

            # ── finalize_leads ───────────────────────────────────────────────
            elif tool_name == "finalize_leads":
                agent_summary        = tool_input.get("summary", "")
                recommended_approach = tool_input.get("recommended_approach", "")
                strongest_segment    = tool_input.get("strongest_segment", "")
                finished = True

                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": tool_use_id,
                    "content":     (
                        f"Lead list finalized. "
                        f"{len(all_leads)} unique verified leads from {searches_performed} Apollo searches."
                    ),
                })

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

        if finished or stop_reason == "end_turn":
            break

    csv_data = leads_to_csv(all_leads) if all_leads else ""

    logger.info(
        "Lead list agent complete for '%s': %d leads, %d searches, segments=%s",
        prospect_company, len(all_leads), searches_performed, segments_searched,
    )

    return {
        "leads":              all_leads,
        "lead_count":         len(all_leads),
        "csv_data":           csv_data,
        "summary":            agent_summary,
        "recommended_approach": recommended_approach,
        "strongest_segment":  strongest_segment,
        "searches_performed": searches_performed,
        "segments_searched":  segments_searched,
    }
