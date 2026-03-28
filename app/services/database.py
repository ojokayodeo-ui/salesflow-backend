"""
Database Layer — SQLite via aiosqlite

Tables:
  deals         — one CRM card per prospect
  pipeline_runs — audit log of every pipeline execution
  leads         — 100 Apollo leads per run, with per-lead approval status

Sequence fields on deals:
  seq_active    — 1 if currently enrolled in a sequence
  seq_id        — which sequence (stored as text ID matching frontend)
  seq_step      — which step they're currently on (1-based)
  seq_started   — ISO timestamp when sequence started
  seq_stopped   — ISO timestamp when sequence was stopped (reply received)
  seq_stop_reason — why it stopped: 'prospect_replied' | 'completed' | 'manual'
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
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    company         TEXT NOT NULL,
    domain          TEXT,
    campaign        TEXT,
    reply_body      TEXT,
    icp_json        TEXT,
    history_json    TEXT NOT NULL DEFAULT '[]',
    seq_active      INTEGER NOT NULL DEFAULT 0,
    seq_id          TEXT,
    seq_step        INTEGER DEFAULT 1,
    seq_started     TEXT,
    seq_stopped     TEXT,
    seq_stop_reason TEXT
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
        # Add sequence columns if upgrading from older schema
        for col, defn in [
            ("seq_active",      "INTEGER NOT NULL DEFAULT 0"),
            ("seq_id",          "TEXT"),
            ("seq_step",        "INTEGER DEFAULT 1"),
            ("seq_started",     "TEXT"),
            ("seq_stopped",     "TEXT"),
            ("seq_stop_reason", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE deals ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
        await db.commit()
    logger.info("Database ready at %s", DB_PATH.resolve())


# ── Deals ──────────────────────────────────────────────────────────────────

async def create_deal(name, email, company, domain="", campaign="", reply_body="") -> dict:
    deal_id = new_id()
    ts = now_iso()
    history = json.dumps([{"from": None, "to": "new", "ts": ts}])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO deals
               (id,created_at,updated_at,stage,name,email,company,
                domain,campaign,reply_body,history_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (deal_id, ts, ts, "new", name, email, company,
             domain, campaign, reply_body, history),
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


def _deal_row(row) -> dict:
    d = dict(row)
    d["history"]   = json.loads(d.pop("history_json", "[]"))
    d["icp"]       = json.loads(d["icp_json"]) if d.get("icp_json") else None
    d["seq_active"] = bool(d.get("seq_active", 0))
    return d


# ── Sequence management ────────────────────────────────────────────────────

async def start_sequence(deal_id: str, seq_id: str, step: int = 1):
    """Enrol a deal in a sequence."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE deals
               SET seq_active=1, seq_id=?, seq_step=?, seq_started=?,
                   seq_stopped=NULL, seq_stop_reason=NULL, updated_at=?
               WHERE id=?""",
            (seq_id, step, now_iso(), now_iso(), deal_id),
        )
        await db.commit()
    logger.info("Deal %s enrolled in sequence %s at step %d", deal_id, seq_id, step)


async def advance_sequence_step(deal_id: str) -> int:
    """Move deal to next sequence step. Returns new step number."""
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
    """
    Stop the sequence for a deal.

    reason options:
      'prospect_replied' — they replied to an email (automatic)
      'completed'        — all steps sent
      'manual'           — you stopped it manually
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE deals
               SET seq_active=0, seq_stopped=?, seq_stop_reason=?, updated_at=?
               WHERE id=?""",
            (now_iso(), reason, now_iso(), deal_id),
        )
        await db.commit()
    logger.info("Sequence stopped for deal %s — reason: %s", deal_id, reason)


async def get_active_sequences() -> list[dict]:
    """Return all deals with an active sequence."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deals WHERE seq_active=1 ORDER BY seq_started DESC"
        ) as cur:
            rows = await cur.fetchall()
    return [_deal_row(r) for r in rows]


# ── Pipeline runs ──────────────────────────────────────────────────────────

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


# ── Leads ──────────────────────────────────────────────────────────────────

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
