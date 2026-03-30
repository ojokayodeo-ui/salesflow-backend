"""
Database Layer — SQLite via aiosqlite

Tables:
  deals         — one CRM card per prospect, includes full respondent profile
  pipeline_runs — audit log of every pipeline execution
  leads         — 100 Apollo leads per run
"""

import json
import logging
import aiosqlite
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)
DB_PATH = Path("salesflow.db")

CREATE_DEALS = """
CREATE TABLE IF NOT EXISTS deals (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    stage           TEXT NOT NULL DEFAULT 'new',
    -- Basic info
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    company         TEXT NOT NULL,
    domain          TEXT,
    campaign        TEXT,
    reply_body      TEXT,
    -- Full respondent profile from Instantly
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
    -- ICP and pipeline
    icp_json        TEXT,
    history_json    TEXT NOT NULL DEFAULT '[]',
    -- Sequence tracking
    seq_active      INTEGER NOT NULL DEFAULT 0,
    seq_id          TEXT,
    seq_step        INTEGER DEFAULT 1,
    seq_started     TEXT,
    seq_stopped     TEXT,
    seq_stop_reason TEXT,
    -- Sentiment scoring
    sentiment       TEXT DEFAULT 'warm',
    sentiment_reason TEXT,
    sentiment_emoji TEXT DEFAULT '☀️',
    last_activity   TEXT
)"""

CREATE_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL DEFAULT 'running',
    stage       TEXT,
    error       TEXT,
    FOREIGN KEY (deal_id) REFERENCES deals(id)
)"""

CREATE_LEADS = """
CREATE TABLE IF NOT EXISTS leads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
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
    FOREIGN KEY (deal_id) REFERENCES deals(id)
)"""

CREATE_SCHEDULED_EMAILS = """
CREATE TABLE IF NOT EXISTS scheduled_emails (
    id          TEXT PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    seq_id      TEXT NOT NULL,
    step_index  INTEGER NOT NULL,
    step_subject TEXT NOT NULL,
    step_body   TEXT NOT NULL,
    send_at     TEXT NOT NULL,
    timezone    TEXT NOT NULL DEFAULT 'Europe/London',
    status      TEXT NOT NULL DEFAULT 'pending',
    sent_at     TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (deal_id) REFERENCES deals(id)
)"""


PROSPECT_COLS = [
    "job_title","job_level","department","linkedin","location",
    "headcount","industry","sub_industry","company_website",
    "company_desc","headline","reply_subject",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def new_id() -> str:
    import time, random, string
    return time.strftime("%Y%m%d%H%M%S") + "".join(random.choices(string.ascii_lowercase, k=4))


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_DEALS)
        await db.execute(CREATE_PIPELINE_RUNS)
        await db.execute(CREATE_LEADS)
        await db.execute(CREATE_SCHEDULED_EMAILS)
        # Add new columns if upgrading from older schema
        all_new_cols = PROSPECT_COLS + [
            "seq_active","seq_id","seq_step","seq_started","seq_stopped","seq_stop_reason",
            "sentiment","sentiment_reason","sentiment_emoji","last_activity",
        ]
        col_defaults = {
            "seq_active": "INTEGER NOT NULL DEFAULT 0",
            "sentiment": "TEXT DEFAULT 'warm'",
            "sentiment_emoji": "TEXT DEFAULT '☀️'",
        }
        for col in all_new_cols:
            try:
                defn = col_defaults.get(col, "TEXT")
                await db.execute(f"ALTER TABLE deals ADD COLUMN {col} {defn}")
            except Exception:
                pass
        await db.commit()
    logger.info("Database ready at %s", DB_PATH.resolve())


# ── Deals ──────────────────────────────────────────────────────────────────

async def get_deal_by_email(email: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deals WHERE LOWER(email)=LOWER(?) ORDER BY created_at DESC LIMIT 1",
            (email,),
        ) as cur:
            row = await cur.fetchone()
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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO deals
               (id,created_at,updated_at,stage,name,email,company,domain,campaign,
                reply_body,job_title,job_level,department,linkedin,location,headcount,
                industry,sub_industry,company_website,company_desc,headline,reply_subject,
                history_json,sentiment,sentiment_emoji,last_activity)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (deal_id, ts, ts, "new", name, email, company, domain, campaign,
             reply_body, job_title, job_level, department, linkedin, location, headcount,
             industry, sub_industry, company_website, company_desc, headline, reply_subject,
             history, "warm", "☀️", ts),
        )
        await db.commit()
    return await get_deal(deal_id)


async def get_deal(deal_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM deals WHERE id=?", (deal_id,)) as cur:
            row = await cur.fetchone()
    return _deal_row(row) if row else None


async def list_deals() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM deals ORDER BY created_at DESC") as cur:
            rows = await cur.fetchall()
    return [_deal_row(r) for r in rows]


async def advance_deal_stage(deal_id: str, new_stage: str) -> dict | None:
    deal = await get_deal(deal_id)
    if not deal:
        return None
    ts = now_iso()
    history = deal["history"]
    history.append({"from": deal["stage"], "to": new_stage, "ts": ts})
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET stage=?, updated_at=?, history_json=? WHERE id=?",
            (new_stage, ts, json.dumps(history), deal_id),
        )
        await db.commit()
    return await get_deal(deal_id)


async def set_deal_icp(deal_id: str, icp: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET icp_json=?, updated_at=? WHERE id=?",
            (json.dumps(icp), now_iso(), deal_id),
        )
        await db.commit()


async def set_deal_sentiment(deal_id: str, sentiment: str, reason: str = "", emoji: str = "☀️"):
    """Set sentiment score for a deal: hot | warm | cold"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET sentiment=?, sentiment_reason=?, sentiment_emoji=?, updated_at=? WHERE id=?",
            (sentiment, reason, emoji, now_iso(), deal_id),
        )
        await db.commit()
    logger.info("Sentiment set for deal %s: %s %s", deal_id, emoji, sentiment)


async def update_last_activity(deal_id: str):
    """Stamp last_activity to now for re-engagement tracking."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET last_activity=?, updated_at=? WHERE id=?",
            (now_iso(), now_iso(), deal_id),
        )
        await db.commit()


def _deal_row(row) -> dict:
    d = dict(row)
    d["history"]    = json.loads(d.pop("history_json", "[]"))
    d["icp"]        = json.loads(d["icp_json"]) if d.get("icp_json") else None
    d["seq_active"] = bool(d.get("seq_active", 0))
    return d


# ── Analytics ──────────────────────────────────────────────────────────────

async def get_analytics() -> dict:
    """Return all key pipeline metrics for the dashboard."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Total deals and stage breakdown
        async with db.execute("SELECT stage, COUNT(*) as cnt FROM deals GROUP BY stage") as cur:
            stage_rows = await cur.fetchall()
        stages = {r["stage"]: r["cnt"] for r in stage_rows}
        total = sum(stages.values())

        # Sentiment breakdown
        async with db.execute("SELECT sentiment, COUNT(*) as cnt FROM deals GROUP BY sentiment") as cur:
            sent_rows = await cur.fetchall()
        sent = {r["sentiment"]: r["cnt"] for r in sent_rows}

        # Meetings / won
        meetings = stages.get("meeting", 0) + stages.get("won", 0)
        conversion = round(meetings / total * 100) if total > 0 else 0

        # Campaign breakdown with hot counts
        async with db.execute(
            "SELECT campaign, sentiment, COUNT(*) as cnt FROM deals WHERE campaign IS NOT NULL AND campaign != '' GROUP BY campaign, sentiment"
        ) as cur:
            camp_rows = await cur.fetchall()
        camp_map = {}
        for r in camp_rows:
            c = r["campaign"]
            if c not in camp_map:
                camp_map[c] = {"name": c, "count": 0, "hot": 0}
            camp_map[c]["count"] += r["cnt"]
            if r["sentiment"] == "hot":
                camp_map[c]["hot"] += r["cnt"]
        campaigns = sorted(camp_map.values(), key=lambda x: x["count"], reverse=True)

        # Avg days to call (deals that reached meeting stage)
        async with db.execute(
            "SELECT created_at, updated_at FROM deals WHERE stage IN ('meeting','won')"
        ) as cur:
            timing_rows = await cur.fetchall()
        if timing_rows:
            from datetime import datetime
            deltas = []
            for r in timing_rows:
                try:
                    c = datetime.fromisoformat(r["created_at"])
                    u = datetime.fromisoformat(r["updated_at"])
                    deltas.append((u - c).days)
                except Exception:
                    pass
            avg_days = round(sum(deltas) / len(deltas)) if deltas else 0
        else:
            avg_days = 0

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
    """Return deals with no activity for more than `days` days."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT * FROM deals
               WHERE stage NOT IN ('won','meeting','delivered')
               AND (last_activity IS NULL OR last_activity < ?)
               AND created_at < ?
               ORDER BY created_at ASC""",
            (cutoff, cutoff),
        ) as cur:
            rows = await cur.fetchall()
    return [_deal_row(r) for r in rows]


# ── Sequence management ─────────────────────────────────────────────────────

async def start_sequence(deal_id: str, seq_id: str, step: int = 1):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE deals
               SET seq_active=1, seq_id=?, seq_step=?, seq_started=?,
                   seq_stopped=NULL, seq_stop_reason=NULL, updated_at=?
               WHERE id=?""",
            (seq_id, step, now_iso(), now_iso(), deal_id),
        )
        await db.commit()


async def advance_sequence_step(deal_id: str) -> int:
    deal = await get_deal(deal_id)
    if not deal:
        return 0
    new_step = (deal.get("seq_step") or 1) + 1
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET seq_step=?, updated_at=? WHERE id=?",
            (new_step, now_iso(), deal_id),
        )
        await db.commit()
    return new_step


async def stop_sequence(deal_id: str, reason: str = "manual"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE deals
               SET seq_active=0, seq_stopped=?, seq_stop_reason=?, updated_at=?
               WHERE id=?""",
            (now_iso(), reason, now_iso(), deal_id),
        )
        await db.commit()


async def get_active_sequences() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deals WHERE seq_active=1 ORDER BY seq_started DESC"
        ) as cur:
            rows = await cur.fetchall()
    return [_deal_row(r) for r in rows]


# ── Pipeline runs ───────────────────────────────────────────────────────────

async def start_pipeline_run(deal_id: str) -> str:
    run_id = new_id()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO pipeline_runs (id,deal_id,started_at,status,stage) VALUES (?,?,?,?,?)",
            (run_id, deal_id, now_iso(), "running", "started"),
        )
        await db.commit()
    return run_id


async def update_pipeline_run(run_id: str, stage: str, status: str = "running", error: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE pipeline_runs SET stage=?, status=?, error=? WHERE id=?",
            (stage, status, error, run_id),
        )
        await db.commit()


async def finish_pipeline_run(run_id: str, status: str = "complete", error: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE pipeline_runs SET status=?, finished_at=?, error=? WHERE id=?",
            (status, now_iso(), error, run_id),
        )
        await db.commit()


# ── Leads ───────────────────────────────────────────────────────────────────

async def save_leads(deal_id: str, run_id: str, leads: list[dict]):
    ts = now_iso()
    rows = [
        (deal_id, run_id, ts, 1,
         l.get("first_name",""), l.get("last_name",""), l.get("full_name",""),
         l.get("title",""), l.get("email",""), l.get("company",""),
         l.get("city",""), l.get("country",""), l.get("linkedin_url",""))
        for l in leads
    ]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT INTO leads
               (deal_id,run_id,created_at,approved,first_name,last_name,
                full_name,title,email,company,city,country,linkedin_url)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        await db.commit()


async def get_leads_for_deal(deal_id: str, approved_only: bool = False) -> list[dict]:
    query = "SELECT * FROM leads WHERE deal_id=?" + (" AND approved=1" if approved_only else "") + " ORDER BY id"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (deal_id,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def set_lead_approval(lead_id: int, approved: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE leads SET approved=? WHERE id=?", (1 if approved else 0, lead_id))
        await db.commit()


async def bulk_set_lead_approval(deal_id: str, approved_ids: list[int]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE leads SET approved=0 WHERE deal_id=?", (deal_id,))
        if approved_ids:
            placeholders = ",".join("?" * len(approved_ids))
            await db.execute(
                f"UPDATE leads SET approved=1 WHERE deal_id=? AND id IN ({placeholders})",
                (deal_id, *approved_ids),
            )
        await db.commit()


# ── Scheduled emails ─────────────────────────────────────────────────────────

async def schedule_email(
    deal_id: str, seq_id: str, step_index: int,
    subject: str, body: str, send_at_utc: str,
    timezone: str = "Europe/London",
) -> str:
    email_id = new_id()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO scheduled_emails
               (id,deal_id,seq_id,step_index,step_subject,step_body,
                send_at,timezone,status,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (email_id, deal_id, seq_id, step_index, subject, body,
             send_at_utc, timezone, "pending", now_iso()),
        )
        await db.commit()
    logger.info("Scheduled email %s for deal %s at %s", email_id, deal_id, send_at_utc)
    return email_id


async def get_due_emails() -> list[dict]:
    now = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM scheduled_emails WHERE status='pending' AND send_at <= ? ORDER BY send_at",
            (now,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def mark_email_sent(email_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE scheduled_emails SET status='sent', sent_at=? WHERE id=?",
            (now_iso(), email_id),
        )
        await db.commit()


async def mark_email_failed(email_id: str, error: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE scheduled_emails SET status='failed', error=? WHERE id=?",
            (error, email_id),
        )
        await db.commit()


async def cancel_scheduled_emails(deal_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE scheduled_emails SET status='cancelled' WHERE deal_id=? AND status='pending'",
            (deal_id,),
        )
        await db.commit()


async def get_scheduled_emails_for_deal(deal_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM scheduled_emails WHERE deal_id=? ORDER BY send_at",
            (deal_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]
