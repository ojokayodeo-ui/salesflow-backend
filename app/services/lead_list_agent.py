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
            "Call once per ICP segment. Returns real contacts with verified emails. "
            "Each call is an API credit — be specific to maximise quality. "
            "You can retry a segment with looser filters if results are poor."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "segment_name": {
                    "type": "string",
                    "description": "ICP segment label (used for tracking which segment each lead came from)"
                },
                "person_titles": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Job titles to target. Use exact Apollo-style titles e.g. 'Managing Director', 'Head of Sales', 'Founder'"
                },
                "person_locations": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Countries or cities e.g. ['United Kingdom'] or ['London', 'Manchester', 'Birmingham']"
                },
                "employee_min": {
                    "type": "integer",
                    "description": "Minimum employee count at target company (omit if not relevant)"
                },
                "employee_max": {
                    "type": "integer",
                    "description": "Maximum employee count at target company (omit if not relevant)"
                },
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Industry/sector keyword tags e.g. ['consulting', 'recruitment', 'SaaS']. Use sparingly — over-filtering reduces results."
                },
                "limit": {
                    "type": "integer",
                    "description": "Max contacts to fetch (10-50). Default 30. Use 50 only for the strongest segment."
                }
            },
            "required": ["segment_name", "person_titles", "person_locations"]
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
    person_titles: list[str],
    person_locations: list[str],
    employee_min: int | None,
    employee_max: int | None,
    keywords: list[str] | None,
    limit: int,
) -> tuple[list[dict], int]:
    """
    Execute a real Apollo.io API search.
    Returns (leads_with_email, total_available).
    """
    payload: dict = {
        "per_page": min(limit, 50),
        "page": 1,
        "person_titles[]":        person_titles[:5],
        "person_locations[]":     person_locations,
        "contact_email_status[]": ["verified"],
    }

    if employee_min and employee_max and 0 < employee_min < employee_max <= 50000:
        payload["organization_num_employees_ranges[]"] = [f"{employee_min},{employee_max}"]

    if keywords:
        payload["q_organization_keyword_tags[]"] = keywords[:5]

    headers = {
        "Content-Type":  "application/json",
        "X-Api-Key":     settings.apollo_api_key,
        "Cache-Control": "no-cache",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(APOLLO_SEARCH_URL, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    people = data.get("people", [])
    total  = data.get("pagination", {}).get("total_entries", 0)

    leads = []
    for p in people:
        email = (p.get("email") or "").strip()
        if not email:
            continue
        leads.append({
            "first_name":   p.get("first_name", ""),
            "last_name":    p.get("last_name", ""),
            "full_name":    p.get("name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
            "title":        p.get("title", ""),
            "email":        email,
            "company":      (p.get("organization") or {}).get("name", ""),
            "city":         p.get("city", ""),
            "country":      p.get("country", ""),
            "linkedin_url": p.get("linkedin_url", ""),
        })

    # Retry without employee range if zero results
    if not leads and (employee_min or employee_max):
        logger.info("Apollo: zero results with employee filter — retrying without")
        payload.pop("organization_num_employees_ranges[]", None)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(APOLLO_SEARCH_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        people = data.get("people", [])
        total  = data.get("pagination", {}).get("total_entries", 0)
        for p in people:
            email = (p.get("email") or "").strip()
            if not email:
                continue
            leads.append({
                "first_name":   p.get("first_name", ""),
                "last_name":    p.get("last_name", ""),
                "full_name":    p.get("name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "title":        p.get("title", ""),
                "email":        email,
                "company":      (p.get("organization") or {}).get("name", ""),
                "city":         p.get("city", ""),
                "country":      p.get("country", ""),
                "linkedin_url": p.get("linkedin_url", ""),
            })

    return leads, total


async def run_lead_list_agent(
    segments: list[dict],
    prospect_company: str = "",
    deal_id: str = "",
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
        seg_lines.append(
            f"Segment {i} — {seg.get('segment_name', f'Segment {i}')}\n"
            f"  Industry:      {seg.get('industry', 'not specified')}\n"
            f"  Sub-niche:     {seg.get('sub_niche', '')}\n"
            f"  Titles:        {', '.join(seg.get('target_titles') or [])}\n"
            f"  Alt titles:    {', '.join(seg.get('secondary_titles') or [])}\n"
            f"  Company size:  {seg.get('company_size', 'any')} "
            f"(min={seg.get('employee_min', '')}, max={seg.get('employee_max', '')})\n"
            f"  Location:      {seg.get('hq_country', 'United Kingdom')}\n"
            f"  Keywords:      {', '.join(seg.get('keywords') or [])}\n"
            f"  Pain point:    {seg.get('pain_point', '')}\n"
            f"  Buying signal: {seg.get('buying_signal', '')}"
        )
    seg_context = "\n\n".join(seg_lines)

    system = f"""You are the Lead List Generation Agent for PALM — a B2B outbound system.

Your job: search Apollo.io to build a high-quality lead list for {prospect_company or 'the prospect'} using the ICP segments below.

STRATEGY:
- You have {len(segments)} ICP segments. Choose the 3 with the clearest, most searchable targeting data.
- For each chosen segment, call search_apollo with filters derived directly from the segment.
- Map segment data to Apollo parameters: titles → person_titles, location → person_locations, employee range → employee_min/max, keywords → keywords (use sparingly).
- If a search returns fewer than 10 leads, consider relaxing filters (remove keywords or employee range) and try again.
- Target 25-40 leads per segment. Total goal: 75-120 unique verified leads.
- After searching your chosen segments, call finalize_leads with your summary.

QUALITY RULES:
- Only verified email contacts count (Apollo handles this — confirmed=verified in the API).
- Prioritise relevance over volume. A tight list of 80 well-targeted leads beats 200 generic ones.
- If results for a segment are poor (wrong titles, irrelevant companies) after retry, skip it and note why.

ICP SEGMENTS:
{seg_context}

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
                seg_name = tool_input.get("segment_name", f"Segment {searches_performed + 1}")
                titles   = tool_input.get("person_titles", [])
                locs     = tool_input.get("person_locations", ["United Kingdom"])
                emp_min  = tool_input.get("employee_min")
                emp_max  = tool_input.get("employee_max")
                kws      = tool_input.get("keywords", [])
                limit    = min(int(tool_input.get("limit") or 30), 50)

                logger.info(
                    "Lead agent [turn %d]: searching '%s' — titles=%s loc=%s emp=%s-%s kw=%s limit=%d",
                    turn + 1, seg_name, titles, locs, emp_min, emp_max, kws, limit,
                )

                try:
                    leads, total_available = await _execute_apollo_search(
                        person_titles   = titles,
                        person_locations= locs,
                        employee_min    = emp_min,
                        employee_max    = emp_max,
                        keywords        = kws,
                        limit           = limit,
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
