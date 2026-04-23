"""
Database Layer — PostgreSQL via asyncpg

Tables:
  deals         — one CRM card per prospect, includes full respondent profile
  pipeline_runs — audit log of every pipeline execution
  leads         — Apollo leads per run
  scheduled_emails — email send queue
"""

import json
import logging
import os
import asyncpg
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Connection ──────────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            raise RuntimeError("DATABASE_URL environment variable not set")
        # asyncpg requires postgresql:// not postgres://
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        _pool = await asyncpg.create_pool(db_url, min_size=1, max_size=10)
        logger.info("PostgreSQL connection pool created")
    return _pool


# ── Schema ──────────────────────────────────────────────────────────────────

CREATE_DEALS = """
CREATE TABLE IF NOT EXISTS deals (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    stage           TEXT NOT NULL DEFAULT 'new',
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    company         TEXT NOT NULL,
    domain          TEXT,
    campaign        TEXT,
    reply_body      TEXT,
    job_title       TEXT,
    job_level       TEXT,
    department      TEXT,
    linkedin        TEXT,
    location        TEXT,
    headcount       TEXT,
    industry        TEXT,
    sub_industry    TEXT,
    company_website TEXT,
    company_desc    TEXT,
    headline        TEXT,
    reply_subject   TEXT,
    icp_json        TEXT,
    history_json    TEXT NOT NULL DEFAULT '[]',
    seq_active      INTEGER NOT NULL DEFAULT 0,
    seq_id          TEXT,
    seq_step        INTEGER DEFAULT 1,
    seq_started     TEXT,
    seq_stopped     TEXT,
    seq_stop_reason TEXT,
    sentiment       TEXT DEFAULT 'warm',
    sentiment_reason TEXT,
    sentiment_emoji TEXT DEFAULT '☀️',
    last_activity   TEXT
)
"""

CREATE_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL DEFAULT 'running',
    stage       TEXT,
    error       TEXT
)
"""

CREATE_LEADS = """
CREATE TABLE IF NOT EXISTS leads (
    id           SERIAL PRIMARY KEY,
    deal_id      TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    approved     INTEGER NOT NULL DEFAULT 1,
    first_name   TEXT,
    last_name    TEXT,
    full_name    TEXT,
    title        TEXT,
    email        TEXT,
    company      TEXT,
    city         TEXT,
    country      TEXT,
    linkedin_url TEXT,
    raw_json     TEXT
)
"""

CREATE_SCHEDULED_EMAILS = """
CREATE TABLE IF NOT EXISTS scheduled_emails (
    id           TEXT PRIMARY KEY,
    deal_id      TEXT NOT NULL,
    seq_id       TEXT NOT NULL,
    step_index   INTEGER NOT NULL,
    step_subject TEXT NOT NULL,
    step_body    TEXT NOT NULL,
    send_at      TEXT NOT NULL,
    timezone     TEXT NOT NULL DEFAULT 'Europe/London',
    status       TEXT NOT NULL DEFAULT 'pending',
    sent_at      TEXT,
    error        TEXT,
    attachments  TEXT,
    created_at   TEXT NOT NULL
)
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def new_id() -> str:
    import time, random, string
    return time.strftime("%Y%m%d%H%M%S") + "".join(random.choices(string.ascii_lowercase, k=4))


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DEALS)
        await conn.execute(CREATE_PIPELINE_RUNS)
        await conn.execute(CREATE_LEADS)
        await conn.execute(CREATE_SCHEDULED_EMAILS)
        # Add columns that may be missing in older schemas
        new_cols = {
            # Profile fields added after initial deploy
            "domain":          "TEXT",
            "job_title":       "TEXT",
            "job_level":       "TEXT",
            "department":      "TEXT",
            "linkedin":        "TEXT",
            "location":        "TEXT",
            "headcount":       "TEXT",
            "industry":        "TEXT",
            "sub_industry":    "TEXT",
            "company_website": "TEXT",
            "company_desc":    "TEXT",
            "headline":        "TEXT",
            "reply_subject":   "TEXT",
            # Sentiment & activity
            "sentiment":       "TEXT DEFAULT 'warm'",
            "sentiment_reason":"TEXT",
            "sentiment_emoji": "TEXT DEFAULT '☀️'",
            "last_activity":   "TEXT",
            # Sequence management
            "seq_active":      "INTEGER NOT NULL DEFAULT 0",
            "seq_id":          "TEXT",
            "seq_step":        "INTEGER DEFAULT 1",
            "seq_started":     "TEXT",
            "seq_stopped":     "TEXT",
            "seq_stop_reason": "TEXT",
        }
        for col, defn in new_cols.items():
            try:
                await conn.execute(f"ALTER TABLE deals ADD COLUMN IF NOT EXISTS {col} {defn}")
            except Exception:
                pass
        # Add raw_json to leads table (preserves all original CSV columns)
        try:
            await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS raw_json TEXT")
        except Exception:
            pass
        # Add attachments to scheduled_emails (base64-encoded file attachments)
        try:
            await conn.execute("ALTER TABLE scheduled_emails ADD COLUMN IF NOT EXISTS attachments TEXT")
        except Exception:
            pass
    logger.info("PostgreSQL database ready")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _deal_row(row) -> dict:
    d = dict(row)
    d["history"]    = json.loads(d.pop("history_json", "[]") or "[]")
    d["icp"]        = json.loads(d["icp_json"]) if d.get("icp_json") else None
    d["seq_active"] = bool(d.get("seq_active", 0))
    # Keep CRM stage in sync with sequence state so the frontend board is correct
    if d.get("seq_active"):
        # Actively running → Sequence Active column
        if d.get("stage") not in ("meeting", "won"):
            d["stage"] = "sequence"
    elif d.get("seq_id") and d.get("stage") not in ("meeting", "won", "delivered", "lost"):
        # Had a sequence but it's stopped/completed → Seq Enrolled column
        d["stage"] = "seq_enrolled"
    return d


# ── Deals ──────────────────────────────────────────────────────────────────

async def get_deal_by_email(email: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM deals WHERE LOWER(email)=LOWER($1) ORDER BY created_at DESC LIMIT 1",
            email,
        )
    return _deal_row(row) if row else None


async def create_deal(
    name: str, email: str, company: str,
    domain: str = "", campaign: str = "", reply_body: str = "",
    job_title: str = "", job_level: str = "", department: str = "",
    linkedin: str = "", location: str = "", headcount: str = "",
    industry: str = "", sub_industry: str = "", company_website: str = "",
    company_desc: str = "", headline: str = "", reply_subject: str = "",
) -> dict:
    deal_id = new_id()
    ts = now_iso()
    history = json.dumps([{"from": None, "to": "new", "ts": ts}])
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO deals
               (id,created_at,updated_at,stage,name,email,company,domain,campaign,
                reply_body,job_title,job_level,department,linkedin,location,headcount,
                industry,sub_industry,company_website,company_desc,headline,reply_subject,
                history_json,sentiment,sentiment_emoji,last_activity)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                       $17,$18,$19,$20,$21,$22,$23,$24,$25,$26)""",
            deal_id, ts, ts, "new", name, email, company, domain, campaign,
            reply_body, job_title, job_level, department, linkedin, location, headcount,
            industry, sub_industry, company_website, company_desc, headline, reply_subject,
            history, "warm", "☀️", ts,
        )
    return await get_deal(deal_id)


async def get_deal(deal_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM deals WHERE id=$1", deal_id)
    return _deal_row(row) if row else None


async def list_deals() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM deals ORDER BY created_at DESC")
    return [_deal_row(r) for r in rows]


async def advance_deal_stage(deal_id: str, new_stage: str) -> dict | None:
    deal = await get_deal(deal_id)
    if not deal:
        return None
    ts = now_iso()
    history = deal["history"]
    history.append({"from": deal["stage"], "to": new_stage, "ts": ts})
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET stage=$1, updated_at=$2, history_json=$3 WHERE id=$4",
            new_stage, ts, json.dumps(history), deal_id,
        )
    return await get_deal(deal_id)


async def update_deal_fields(deal_id: str, fields: dict) -> dict | None:
    """
    Update one or more profile fields on an existing deal.
    Accepted keys: name, company, domain, job_title, job_level, department,
    linkedin, location, headcount, industry, sub_industry, company_website,
    company_desc, headline, reply_subject, reply_body, campaign.
    """
    ALLOWED = {
        "name", "company", "domain", "job_title", "job_level", "department",
        "linkedin", "location", "headcount", "industry", "sub_industry",
        "company_website", "company_desc", "headline", "reply_subject",
        "reply_body", "campaign",
    }
    updates = {k: v for k, v in fields.items() if k in ALLOWED and v is not None}
    if not updates:
        return await get_deal(deal_id)
    ts = now_iso()
    set_clauses = ", ".join(f"{col}=${i + 1}" for i, col in enumerate(updates.keys()))
    values = list(updates.values()) + [ts, deal_id]
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE deals SET {set_clauses}, updated_at=${len(updates) + 1} WHERE id=${len(updates) + 2}",
            *values,
        )
    return await get_deal(deal_id)


async def set_deal_icp(deal_id: str, icp: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET icp_json=$1, updated_at=$2 WHERE id=$3",
            json.dumps(icp), now_iso(), deal_id,
        )


async def set_deal_sentiment(deal_id: str, sentiment: str, reason: str = "", emoji: str = "☀️"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET sentiment=$1, sentiment_reason=$2, sentiment_emoji=$3, updated_at=$4 WHERE id=$5",
            sentiment, reason, emoji, now_iso(), deal_id,
        )
    logger.info("Sentiment set for deal %s: %s %s", deal_id, emoji, sentiment)


async def update_last_activity(deal_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET last_activity=$1, updated_at=$2 WHERE id=$3",
            now_iso(), now_iso(), deal_id,
        )


# ── Analytics ──────────────────────────────────────────────────────────────

async def get_analytics() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        stage_rows = await conn.fetch("SELECT stage, COUNT(*) as cnt FROM deals GROUP BY stage")
        stages = {r["stage"]: r["cnt"] for r in stage_rows}
        total = sum(stages.values())

        sent_rows = await conn.fetch("SELECT sentiment, COUNT(*) as cnt FROM deals GROUP BY sentiment")
        sent = {r["sentiment"]: r["cnt"] for r in sent_rows}

        meetings = stages.get("meeting", 0) + stages.get("won", 0)
        conversion = round(meetings / total * 100) if total > 0 else 0

        camp_rows = await conn.fetch(
            "SELECT campaign, sentiment, COUNT(*) as cnt FROM deals WHERE campaign IS NOT NULL AND campaign != '' GROUP BY campaign, sentiment"
        )
        camp_map = {}
        for r in camp_rows:
            c = r["campaign"]
            if c not in camp_map:
                camp_map[c] = {"name": c, "count": 0, "hot": 0}
            camp_map[c]["count"] += r["cnt"]
            if r["sentiment"] == "hot":
                camp_map[c]["hot"] += r["cnt"]
        campaigns = sorted(camp_map.values(), key=lambda x: x["count"], reverse=True)

        timing_rows = await conn.fetch(
            "SELECT created_at, updated_at FROM deals WHERE stage IN ('meeting','won')"
        )
        avg_days = 0
        if timing_rows:
            deltas = []
            for r in timing_rows:
                try:
                    c = datetime.fromisoformat(r["created_at"])
                    u = datetime.fromisoformat(r["updated_at"])
                    deltas.append((u - c).days)
                except Exception:
                    pass
            avg_days = round(sum(deltas) / len(deltas)) if deltas else 0

    return {
        "total_replies":    total,
        "meetings_booked":  meetings,
        "conversion_rate":  conversion,
        "avg_days_to_call": avg_days,
        "hot_leads":        sent.get("hot", 0),
        "warm_leads":       sent.get("warm", 0),
        "cold_leads":       sent.get("cold", 0),
        "stages":           stages,
        "campaigns":        campaigns,
    }


async def get_idle_deals(days: int = 7) -> list[dict]:
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM deals
               WHERE stage NOT IN ('won','meeting','delivered')
               AND (last_activity IS NULL OR last_activity < $1)
               AND created_at < $1
               ORDER BY created_at ASC""",
            cutoff,
        )
    return [_deal_row(r) for r in rows]


# ── Sequence management ─────────────────────────────────────────────────────

async def start_sequence(deal_id: str, seq_id: str, step: int = 1):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE deals
               SET seq_active=1, seq_id=$1, seq_step=$2, seq_started=$3,
                   seq_stopped=NULL, seq_stop_reason=NULL, updated_at=$4
               WHERE id=$5""",
            seq_id, step, now_iso(), now_iso(), deal_id,
        )


async def advance_sequence_step(deal_id: str) -> int:
    deal = await get_deal(deal_id)
    if not deal:
        return 0
    new_step = (deal.get("seq_step") or 1) + 1
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET seq_step=$1, updated_at=$2 WHERE id=$3",
            new_step, now_iso(), deal_id,
        )
    return new_step


async def stop_sequence(deal_id: str, reason: str = "manual"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE deals
               SET seq_active=0, seq_stopped=$1, seq_stop_reason=$2, updated_at=$3
               WHERE id=$4""",
            now_iso(), reason, now_iso(), deal_id,
        )


async def get_active_sequences() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM deals WHERE seq_active=1 ORDER BY seq_started DESC"
        )
    return [_deal_row(r) for r in rows]


# ── Pipeline runs ───────────────────────────────────────────────────────────

async def start_pipeline_run(deal_id: str) -> str:
    run_id = new_id()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO pipeline_runs (id,deal_id,started_at,status,stage) VALUES ($1,$2,$3,$4,$5)",
            run_id, deal_id, now_iso(), "running", "started",
        )
    return run_id


async def update_pipeline_run(run_id: str, stage: str, status: str = "running", error: str = ""):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pipeline_runs SET stage=$1, status=$2, error=$3 WHERE id=$4",
            stage, status, error, run_id,
        )


async def finish_pipeline_run(run_id: str, status: str = "complete", error: str = ""):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pipeline_runs SET status=$1, finished_at=$2, error=$3 WHERE id=$4",
            status, now_iso(), error, run_id,
        )


# ── Leads ───────────────────────────────────────────────────────────────────

async def save_leads(deal_id: str, run_id: str, leads: list[dict]):
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """INSERT INTO leads
               (deal_id,run_id,created_at,approved,first_name,last_name,
                full_name,title,email,company,city,country,linkedin_url,raw_json)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
            [
                (deal_id, run_id, ts, 1,
                 l.get("first_name",""), l.get("last_name",""), l.get("full_name",""),
                 l.get("title",""), l.get("email",""), l.get("company",""),
                 l.get("city",""), l.get("country",""), l.get("linkedin_url",""),
                 json.dumps(l["_raw"]) if l.get("_raw") else None)
                for l in leads
            ],
        )


async def get_leads_for_deal(deal_id: str, approved_only: bool = False) -> list[dict]:
    query = "SELECT * FROM leads WHERE deal_id=$1" + (" AND approved=1" if approved_only else "") + " ORDER BY id"
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, deal_id)
    return [dict(r) for r in rows]


async def set_lead_approval(lead_id: int, approved: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE leads SET approved=$1 WHERE id=$2", 1 if approved else 0, lead_id)


async def bulk_set_lead_approval(deal_id: str, approved_ids: list[int]):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE leads SET approved=0 WHERE deal_id=$1", deal_id)
        if approved_ids:
            await conn.execute(
                f"UPDATE leads SET approved=1 WHERE deal_id=$1 AND id = ANY($2::int[])",
                deal_id, approved_ids,
            )


# ── Scheduled emails ─────────────────────────────────────────────────────────

async def schedule_email(
    deal_id: str, seq_id: str, step_index: int,
    subject: str, body: str, send_at_utc: str,
    timezone: str = "Europe/London",
    attachments: str | None = None,
) -> str:
    email_id = new_id()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO scheduled_emails
               (id,deal_id,seq_id,step_index,step_subject,step_body,
                send_at,timezone,status,attachments,created_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)""",
            email_id, deal_id, seq_id, step_index, subject, body,
            send_at_utc, timezone, "pending", attachments, now_iso(),
        )
    logger.info("Scheduled email %s for deal %s at %s", email_id, deal_id, send_at_utc)
    return email_id


async def get_due_emails() -> list[dict]:
    now = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM scheduled_emails WHERE status='pending' AND send_at <= $1 ORDER BY send_at",
            now,
        )
    return [dict(r) for r in rows]


async def mark_email_sent(email_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE scheduled_emails SET status='sent', sent_at=$1 WHERE id=$2",
            now_iso(), email_id,
        )


async def mark_email_failed(email_id: str, error: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE scheduled_emails SET status='failed', error=$1 WHERE id=$2",
            error, email_id,
        )


async def cancel_scheduled_emails(deal_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE scheduled_emails SET status='cancelled' WHERE deal_id=$1 AND status='pending'",
            deal_id,
        )


async def get_scheduled_emails_for_deal(deal_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM scheduled_emails WHERE deal_id=$1 ORDER BY send_at",
            deal_id,
        )
    return [dict(r) for r in rows]


# ── Extra Tables ────────────────────────────────────────────────────────────

CREATE_NOTES = """
CREATE TABLE IF NOT EXISTS deal_notes (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    content     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
)"""

CREATE_DRIVE_LINKS = """
CREATE TABLE IF NOT EXISTS deal_drive_links (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    label       TEXT NOT NULL,
    url         TEXT NOT NULL,
    file_type   TEXT DEFAULT 'document',
    created_at  TEXT NOT NULL
)"""

CREATE_SOCIAL_LINKS = """
CREATE TABLE IF NOT EXISTS deal_social_links (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    platform    TEXT NOT NULL,
    url         TEXT NOT NULL,
    notes       TEXT DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
)"""


CREATE_SWIPE_FILES = """
CREATE TABLE IF NOT EXISTS swipe_files (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    category    TEXT NOT NULL DEFAULT 'general',
    content     TEXT NOT NULL,
    source      TEXT DEFAULT '',
    tags_json   TEXT NOT NULL DEFAULT '[]',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
)"""

CREATE_NURTURE_SEQUENCES = """
CREATE TABLE IF NOT EXISTS nurture_sequences (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
)"""

CREATE_NURTURE_SEQ_STEPS = """
CREATE TABLE IF NOT EXISTS nurture_seq_steps (
    id          TEXT PRIMARY KEY,
    sequence_id TEXT NOT NULL,
    step_order  INTEGER NOT NULL,
    subject     TEXT NOT NULL DEFAULT '',
    body        TEXT NOT NULL DEFAULT '',
    delay_days  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
)"""

CREATE_NURTURE_ENROLLMENTS = """
CREATE TABLE IF NOT EXISTS nurture_enrollments (
    id           TEXT PRIMARY KEY,
    sequence_id  TEXT NOT NULL,
    deal_id      TEXT NOT NULL,
    current_step INTEGER NOT NULL DEFAULT 1,
    status       TEXT NOT NULL DEFAULT 'active',
    enrolled_at  TEXT NOT NULL,
    last_sent_at TEXT,
    next_send_at TEXT
)"""

CREATE_EMAIL_EVENTS = """
CREATE TABLE IF NOT EXISTS email_events (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    token       TEXT UNIQUE NOT NULL,
    direction   TEXT NOT NULL,
    subject     TEXT DEFAULT '',
    sent_at     TEXT NOT NULL,
    opened_at   TEXT,
    open_count  INTEGER DEFAULT 0,
    created_at  TEXT NOT NULL
)"""


async def ensure_extra_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_NOTES)
        await conn.execute(CREATE_DRIVE_LINKS)
        await conn.execute(CREATE_SOCIAL_LINKS)
        await conn.execute(CREATE_SWIPE_FILES)
        await conn.execute(CREATE_EMAIL_EVENTS)
        await conn.execute(CREATE_NURTURE_SEQUENCES)
        await conn.execute(CREATE_NURTURE_SEQ_STEPS)
        await conn.execute(CREATE_NURTURE_ENROLLMENTS)
    logger.info("Extra tables ready")


# ── Notes ────────────────────────────────────────────────────────────────────

async def add_note(deal_id: str, content: str) -> dict:
    note_id = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO deal_notes (id,deal_id,content,created_at,updated_at) VALUES ($1,$2,$3,$4,$5)",
            note_id, deal_id, content, ts, ts,
        )
    return {"id": note_id, "deal_id": deal_id, "content": content, "created_at": ts, "updated_at": ts}


async def get_notes(deal_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM deal_notes WHERE deal_id=$1 ORDER BY created_at DESC", deal_id)
    return [dict(r) for r in rows]


async def update_note(note_id: str, content: str) -> dict | None:
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE deal_notes SET content=$1, updated_at=$2 WHERE id=$3", content, ts, note_id)
        row = await conn.fetchrow("SELECT * FROM deal_notes WHERE id=$1", note_id)
    return dict(row) if row else None


async def delete_note(note_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM deal_notes WHERE id=$1", note_id)


# ── Drive Links ──────────────────────────────────────────────────────────────

async def add_drive_link(deal_id: str, label: str, url: str, file_type: str = "document") -> dict:
    link_id = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO deal_drive_links (id,deal_id,label,url,file_type,created_at) VALUES ($1,$2,$3,$4,$5,$6)",
            link_id, deal_id, label, url, file_type, ts,
        )
    return {"id": link_id, "deal_id": deal_id, "label": label, "url": url, "file_type": file_type, "created_at": ts}


async def get_drive_links(deal_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM deal_drive_links WHERE deal_id=$1 ORDER BY created_at DESC", deal_id)
    return [dict(r) for r in rows]


async def delete_drive_link(link_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM deal_drive_links WHERE id=$1", link_id)


# ── Social Links ─────────────────────────────────────────────────────────────

async def add_social_link(deal_id: str, platform: str, url: str, notes: str = "") -> dict:
    link_id = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO deal_social_links (id,deal_id,platform,url,notes,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            link_id, deal_id, platform, url, notes, ts, ts,
        )
    return {"id": link_id, "deal_id": deal_id, "platform": platform, "url": url, "notes": notes, "created_at": ts}


async def get_social_links(deal_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM deal_social_links WHERE deal_id=$1 ORDER BY platform", deal_id)
    return [dict(r) for r in rows]


async def update_social_link(link_id: str, url: str, notes: str) -> dict | None:
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE deal_social_links SET url=$1, notes=$2, updated_at=$3 WHERE id=$4", url, notes, ts, link_id)
        row = await conn.fetchrow("SELECT * FROM deal_social_links WHERE id=$1", link_id)
    return dict(row) if row else None


async def delete_social_link(link_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM deal_social_links WHERE id=$1", link_id)


# ── Pipeline Intelligence ─────────────────────────────────────────────────────

async def get_pipeline_intelligence() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        all_deals = await conn.fetch("SELECT * FROM deals ORDER BY created_at")
        deals = [dict(r) for r in all_deals]

    import json as _json
    from datetime import datetime

    def counter(items):
        c = {}
        for i in items:
            if i:
                c[i] = c.get(i, 0) + 1
        return sorted(c.items(), key=lambda x: x[1], reverse=True)

    job_titles  = counter([d.get("job_title","").strip() for d in deals if d.get("job_title")])
    seniority   = counter([d.get("job_level","").strip() for d in deals if d.get("job_level")])
    industries  = counter([d.get("industry","").strip() for d in deals if d.get("industry")])
    locations   = counter([d.get("location","").strip().split(",")[0] for d in deals if d.get("location")])
    headcounts  = counter([d.get("headcount","").strip() for d in deals if d.get("headcount")])
    campaigns   = counter([d.get("campaign","").strip() for d in deals if d.get("campaign")])

    pain_points = []
    desires = []
    buying_signals = []
    for d in deals:
        icp_raw = d.get("icp_json")
        if not icp_raw:
            continue
        try:
            icp = _json.loads(icp_raw)
            for s in icp.get("segments", []):
                if s.get("pain_point"): pain_points.append(s["pain_point"])
                if s.get("buying_signal"): buying_signals.append(s["buying_signal"])
                if s.get("segment_name"): desires.append(s["segment_name"])
        except Exception:
            pass

    velocity = {}
    for d in deals:
        history_raw = d.get("history_json", "[]")
        try:
            history = _json.loads(history_raw) if isinstance(history_raw, str) else history_raw
        except Exception:
            history = []
        for i in range(len(history) - 1):
            try:
                t1 = datetime.fromisoformat(history[i].get("ts", ""))
                t2 = datetime.fromisoformat(history[i+1].get("ts", ""))
                days = (t2 - t1).total_seconds() / 86400
                key = f"{history[i].get('to','')}→{history[i+1].get('to','')}"
                velocity.setdefault(key, []).append(round(days, 1))
            except Exception:
                pass
    avg_velocity = {k: round(sum(v)/len(v), 1) for k, v in velocity.items() if v}

    won_deals  = [d for d in deals if d.get("stage") in ("won", "meeting")]
    lost_deals = [d for d in deals if d.get("stage") == "lost"]
    funnel = {}
    for d in deals:
        s = d.get("stage", "new")
        funnel[s] = funnel.get(s, 0) + 1

    return {
        "total_deals": len(deals),
        "demographics": {
            "job_titles":  job_titles[:10],
            "seniority":   seniority[:8],
            "industries":  industries[:10],
            "locations":   locations[:10],
            "headcounts":  headcounts[:8],
            "campaigns":   campaigns[:10],
        },
        "psychographics": {
            "pain_points":    counter(pain_points)[:10],
            "desires":        counter(desires)[:10],
            "buying_signals": counter(buying_signals)[:8],
        },
        "velocity": avg_velocity,
        "win_loss": {
            "won_count":      len(won_deals),
            "lost_count":     len(lost_deals),
            "won_industries": counter([d.get("industry","") for d in won_deals if d.get("industry")])[:5],
            "lost_industries":counter([d.get("industry","") for d in lost_deals if d.get("industry")])[:5],
            "won_titles":     counter([d.get("job_title","") for d in won_deals if d.get("job_title")])[:5],
            "won_sentiments": counter([d.get("sentiment","") for d in won_deals]),
            "won_campaigns":  counter([d.get("campaign","") for d in won_deals if d.get("campaign")])[:5],
        },
        "funnel": funnel,
    }


# ── Swipe Files ───────────────────────────────────────────────────────────────

def _swipe_row(row) -> dict:
    d = dict(row)
    d["tags"] = json.loads(d.pop("tags_json", "[]") or "[]")
    return d


async def list_swipe_files() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM swipe_files ORDER BY created_at DESC")
    return [_swipe_row(r) for r in rows]


async def get_swipe_file(sf_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM swipe_files WHERE id=$1", sf_id)
    return _swipe_row(row) if row else None


async def create_swipe_file(
    title: str, content: str,
    category: str = "general", source: str = "", tags: list = []
) -> dict:
    sf_id = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO swipe_files
               (id,title,category,content,source,tags_json,created_at,updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
            sf_id, title, category, content, source, json.dumps(tags), ts, ts,
        )
    return await get_swipe_file(sf_id)


async def update_swipe_file(sf_id: str, fields: dict) -> dict | None:
    ALLOWED = {"title", "category", "content", "source"}
    updates = {}
    if "tags" in fields:
        updates["tags_json"] = json.dumps(fields["tags"])
    for k, v in fields.items():
        if k in ALLOWED:
            updates[k] = v
    if not updates:
        return await get_swipe_file(sf_id)
    ts = now_iso()
    set_clauses = ", ".join(f"{col}=${i+1}" for i, col in enumerate(updates.keys()))
    values = list(updates.values()) + [ts, sf_id]
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE swipe_files SET {set_clauses}, updated_at=${len(updates)+1} WHERE id=${len(updates)+2}",
            *values,
        )
    return await get_swipe_file(sf_id)


async def delete_swipe_file(sf_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM swipe_files WHERE id=$1", sf_id)


# ── Nurture Sequences ─────────────────────────────────────────────────────────

def _seq_row(row) -> dict:
    return {
        "id": row["id"], "name": row["name"], "description": row["description"],
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }

def _step_row(row) -> dict:
    return {
        "id": row["id"], "sequence_id": row["sequence_id"],
        "step_order": row["step_order"], "subject": row["subject"],
        "body": row["body"], "delay_days": row["delay_days"],
        "created_at": row["created_at"],
    }

async def list_nurture_sequences() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        seqs = await conn.fetch("SELECT * FROM nurture_sequences ORDER BY created_at DESC")
        steps = await conn.fetch("SELECT * FROM nurture_seq_steps ORDER BY sequence_id, step_order")
        counts = await conn.fetch(
            "SELECT sequence_id, COUNT(*) as total, "
            "COUNT(CASE WHEN status='active' THEN 1 END) as active "
            "FROM nurture_enrollments GROUP BY sequence_id"
        )
    count_map = {r["sequence_id"]: {"enrolled": r["total"], "active": r["active"]} for r in counts}
    step_map: dict[str, list] = {}
    for s in steps:
        step_map.setdefault(s["sequence_id"], []).append(_step_row(s))
    result = []
    for seq in seqs:
        d = _seq_row(seq)
        d["steps"] = step_map.get(seq["id"], [])
        d["enrolled_count"] = count_map.get(seq["id"], {}).get("enrolled", 0)
        d["active_count"] = count_map.get(seq["id"], {}).get("active", 0)
        result.append(d)
    return result

async def get_nurture_sequence(seq_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM nurture_sequences WHERE id=$1", seq_id)
        if not row:
            return None
        steps = await conn.fetch(
            "SELECT * FROM nurture_seq_steps WHERE sequence_id=$1 ORDER BY step_order", seq_id
        )
    d = _seq_row(row)
    d["steps"] = [_step_row(s) for s in steps]
    return d

async def create_nurture_sequence(name: str, description: str = "") -> dict:
    sid = new_id(); ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO nurture_sequences (id,name,description,created_at,updated_at) VALUES ($1,$2,$3,$4,$5)",
            sid, name, description, ts, ts,
        )
    return await get_nurture_sequence(sid)

async def update_nurture_sequence(seq_id: str, fields: dict) -> dict | None:
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE nurture_sequences SET name=$1, description=$2, updated_at=$3 WHERE id=$4",
            fields.get("name"), fields.get("description", ""), ts, seq_id,
        )
    return await get_nurture_sequence(seq_id)

async def delete_nurture_sequence(seq_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM nurture_seq_steps WHERE sequence_id=$1", seq_id)
        await conn.execute("DELETE FROM nurture_enrollments WHERE sequence_id=$1", seq_id)
        await conn.execute("DELETE FROM nurture_sequences WHERE id=$1", seq_id)

async def get_nurture_steps(sequence_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM nurture_seq_steps WHERE sequence_id=$1 ORDER BY step_order", sequence_id
        )
    return [_step_row(r) for r in rows]

async def add_nurture_step(sequence_id: str, step_order: int, subject: str, body: str, delay_days: int) -> dict:
    sid = new_id(); ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO nurture_seq_steps (id,sequence_id,step_order,subject,body,delay_days,created_at) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            sid, sequence_id, step_order, subject, body, delay_days, ts,
        )
        row = await conn.fetchrow("SELECT * FROM nurture_seq_steps WHERE id=$1", sid)
    return _step_row(row)

async def update_nurture_step(step_id: str, fields: dict) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE nurture_seq_steps SET subject=$1, body=$2, delay_days=$3, step_order=$4 WHERE id=$5",
            fields.get("subject"), fields.get("body"), fields.get("delay_days", 0),
            fields.get("step_order", 1), step_id,
        )
        row = await conn.fetchrow("SELECT * FROM nurture_seq_steps WHERE id=$1", step_id)
    return _step_row(row) if row else None

async def delete_nurture_step(step_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM nurture_seq_steps WHERE id=$1", step_id)

async def list_nurture_enrollments(status: str | None = None) -> list[dict]:
    pool = await get_pool()
    sql = (
        "SELECT ne.*, d.name as deal_name, d.email as deal_email, "
        "d.company as deal_company, ns.name as sequence_name "
        "FROM nurture_enrollments ne "
        "LEFT JOIN deals d ON d.id=ne.deal_id "
        "LEFT JOIN nurture_sequences ns ON ns.id=ne.sequence_id "
    )
    args = []
    if status:
        sql += " WHERE ne.status=$1"
        args.append(status)
    sql += " ORDER BY ne.enrolled_at DESC"
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [dict(r) for r in rows]

async def get_nurture_enrollment(enrollment_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ne.*, d.name as deal_name, d.email as deal_email, "
            "d.company as deal_company, ns.name as sequence_name "
            "FROM nurture_enrollments ne "
            "LEFT JOIN deals d ON d.id=ne.deal_id "
            "LEFT JOIN nurture_sequences ns ON ns.id=ne.sequence_id "
            "WHERE ne.id=$1",
            enrollment_id,
        )
    return dict(row) if row else None

async def create_nurture_enrollment(sequence_id: str, deal_id: str) -> dict:
    eid = new_id(); ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO nurture_enrollments "
            "(id,sequence_id,deal_id,current_step,status,enrolled_at) VALUES ($1,$2,$3,$4,$5,$6)",
            eid, sequence_id, deal_id, 1, "active", ts,
        )
    return await get_nurture_enrollment(eid)

async def update_enrollment_status(enrollment_id: str, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE nurture_enrollments SET status=$1 WHERE id=$2", status, enrollment_id
        )

async def advance_enrollment(enrollment_id: str, last_sent_at: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE nurture_enrollments SET current_step=current_step+1, last_sent_at=$1 WHERE id=$2",
            last_sent_at, enrollment_id,
        )

async def delete_nurture_enrollment(enrollment_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM nurture_enrollments WHERE id=$1", enrollment_id)

async def get_nurture_analytics() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM nurture_enrollments")
        status_rows = await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM nurture_enrollments GROUP BY status"
        )
        email_row = await conn.fetchrow(
            "SELECT COUNT(*) as sent, COALESCE(SUM(open_count),0) as total_opens, "
            "COUNT(CASE WHEN opened_at IS NOT NULL THEN 1 END) as unique_opens "
            "FROM email_events"
        )
        per_seq = await conn.fetch(
            "SELECT ne.sequence_id, ns.name, COUNT(*) as enrolled, "
            "COUNT(CASE WHEN ne.status='active' THEN 1 END) as active, "
            "COUNT(CASE WHEN ne.status='completed' THEN 1 END) as completed, "
            "COUNT(CASE WHEN ne.status='paused' THEN 1 END) as paused "
            "FROM nurture_enrollments ne "
            "LEFT JOIN nurture_sequences ns ON ns.id=ne.sequence_id "
            "GROUP BY ne.sequence_id, ns.name ORDER BY enrolled DESC"
        )
    status_map = {r["status"]: r["cnt"] for r in status_rows}
    return {
        "total_enrolled": total or 0,
        "active": status_map.get("active", 0),
        "paused": status_map.get("paused", 0),
        "completed": status_map.get("completed", 0),
        "cancelled": status_map.get("cancelled", 0),
        "emails_sent": email_row["sent"] if email_row else 0,
        "total_opens": email_row["total_opens"] if email_row else 0,
        "unique_opens": email_row["unique_opens"] if email_row else 0,
        "per_sequence": [dict(r) for r in per_seq],
    }


# ── Email Events (tracking) ──────────────────────────────────────────────────

async def save_email_event(
    deal_id: str, token: str, direction: str, subject: str
) -> str:
    eid = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO email_events
               (id, deal_id, token, direction, subject, sent_at, open_count, created_at)
               VALUES ($1,$2,$3,$4,$5,$6,0,$7)""",
            eid, deal_id, token, direction, subject, ts, ts,
        )
    return eid


async def record_email_open(token: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, opened_at FROM email_events WHERE token=$1", token
        )
        if not row:
            return False
        ts = now_iso()
        first_open = row["opened_at"] or ts
        await conn.execute(
            "UPDATE email_events SET open_count=open_count+1, opened_at=$1 WHERE token=$2",
            first_open, token,
        )
    return True


async def get_email_metrics(deal_id: str) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM email_events WHERE deal_id=$1 ORDER BY sent_at DESC", deal_id
        )
    events = [dict(r) for r in rows]
    sent     = [e for e in events if e["direction"] == "sent"]
    received = [e for e in events if e["direction"] == "received"]
    return {
        "sent_count":    len(sent),
        "received_count": len(received),
        "total_opens":   sum(e.get("open_count", 0) for e in sent),
        "opened_count":  sum(1 for e in sent if (e.get("open_count") or 0) > 0),
        "last_sent":     sent[0]["sent_at"] if sent else None,
        "last_received": received[0]["sent_at"] if received else None,
        "events":        events[:20],
    }
