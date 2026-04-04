"""
Prompt Management Database Layer
PostgreSQL via asyncpg — all prompt storage, versioning, and category operations.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from app.services.database import get_pool

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

def new_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_prompt(row: dict) -> dict:
    """Convert asyncpg row to clean dict, parsing JSON fields."""
    d = dict(row)
    # Parse tags (stored as JSON array string)
    if isinstance(d.get("tags"), str):
        try:
            d["tags"] = json.loads(d["tags"])
        except Exception:
            d["tags"] = []
    elif d.get("tags") is None:
        d["tags"] = []

    # Parse variables (stored as JSON array string)
    if isinstance(d.get("variables"), str):
        try:
            d["variables"] = json.loads(d["variables"])
        except Exception:
            d["variables"] = []
    elif d.get("variables") is None:
        d["variables"] = []

    # Parse boolean
    d["is_favorite"] = bool(d.get("is_favorite", False))

    return d


# ── DDL ──────────────────────────────────────────────────────────────────────

CREATE_PROMPTS = """
CREATE TABLE IF NOT EXISTS prompts (
    id           TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    title        TEXT NOT NULL,
    description  TEXT,
    content      TEXT NOT NULL,
    tags         TEXT NOT NULL DEFAULT '[]',
    category     TEXT,
    is_favorite  INTEGER NOT NULL DEFAULT 0,
    usage_count  INTEGER NOT NULL DEFAULT 0,
    variables    TEXT NOT NULL DEFAULT '[]'
)
"""

CREATE_PROMPT_VERSIONS = """
CREATE TABLE IF NOT EXISTS prompt_versions (
    id             TEXT PRIMARY KEY,
    prompt_id      TEXT NOT NULL REFERENCES prompts(id) ON DELETE CASCADE,
    content        TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    change_note    TEXT,
    created_at     TEXT NOT NULL
)
"""

CREATE_PROMPT_CATEGORIES = """
CREATE TABLE IF NOT EXISTS prompt_categories (
    id         TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    name       TEXT UNIQUE NOT NULL,
    color      TEXT NOT NULL DEFAULT '#6366f1',
    icon       TEXT
)
"""


async def ensure_prompt_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_PROMPTS)
        await conn.execute(CREATE_PROMPT_VERSIONS)
        await conn.execute(CREATE_PROMPT_CATEGORIES)
    logger.info("Prompt tables ready")


# ── Prompts ──────────────────────────────────────────────────────────────────

async def create_prompt(
    title: str,
    content: str,
    description: Optional[str] = None,
    tags: list = None,
    category: Optional[str] = None,
    is_favorite: bool = False,
    variables: list = None,
) -> dict:
    pid = new_id()
    ts = now_iso()
    tags_json = json.dumps(tags or [])
    vars_json = json.dumps(variables or [])
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO prompts
               (id, created_at, updated_at, title, description, content, tags,
                category, is_favorite, usage_count, variables)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10)""",
            pid, ts, ts, title, description, content,
            tags_json, category, 1 if is_favorite else 0, vars_json,
        )
    return _row_to_prompt({
        "id": pid, "created_at": ts, "updated_at": ts,
        "title": title, "description": description, "content": content,
        "tags": tags or [], "category": category, "is_favorite": is_favorite,
        "usage_count": 0, "variables": variables or [],
    })


async def get_prompt(prompt_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM prompts WHERE id=$1", prompt_id)
    return _row_to_prompt(dict(row)) if row else None


async def list_prompts(
    search: Optional[str] = None,
    tags: Optional[list] = None,
    category: Optional[str] = None,
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    conditions = []
    params = []
    idx = 1

    if search:
        conditions.append(
            f"(LOWER(title) LIKE $%d OR LOWER(content) LIKE $%d OR LOWER(description) LIKE $%d)"
            % (idx, idx + 1, idx + 2)
        )
        like = f"%{search.lower()}%"
        params.extend([like, like, like])
        idx += 3

    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1

    if tags:
        # Check each tag is present in the JSON array string
        for tag in tags:
            conditions.append(f"tags LIKE ${idx}")
            params.append(f'%"{tag}"%')
            idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    order = {
        "recent": "created_at DESC",
        "most_used": "usage_count DESC, created_at DESC",
        "az": "LOWER(title) ASC",
        "favorites": "is_favorite DESC, created_at DESC",
    }.get(sort, "created_at DESC")

    params.extend([limit, offset])
    query = f"SELECT * FROM prompts {where} ORDER BY {order} LIMIT ${idx} OFFSET ${idx+1}"

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    return [_row_to_prompt(dict(r)) for r in rows]


async def count_prompts(
    search: Optional[str] = None,
    tags: Optional[list] = None,
    category: Optional[str] = None,
) -> int:
    conditions = []
    params = []
    idx = 1

    if search:
        conditions.append(
            f"(LOWER(title) LIKE $%d OR LOWER(content) LIKE $%d OR LOWER(description) LIKE $%d)"
            % (idx, idx + 1, idx + 2)
        )
        like = f"%{search.lower()}%"
        params.extend([like, like, like])
        idx += 3

    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1

    if tags:
        for tag in tags:
            conditions.append(f"tags LIKE ${idx}")
            params.append(f'%"{tag}"%')
            idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM prompts {where}", *params)
    return row["cnt"]


async def update_prompt(
    prompt_id: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
    content: Optional[str] = None,
    tags: Optional[list] = None,
    category: Optional[str] = None,
    is_favorite: Optional[bool] = None,
    variables: Optional[list] = None,
    change_note: Optional[str] = None,
    old_content: Optional[str] = None,
) -> dict:
    ts = now_iso()
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Save version snapshot if content changed
        if content is not None and old_content is not None and content != old_content:
            row = await conn.fetchrow(
                "SELECT COALESCE(MAX(version_number), 0) as max_ver FROM prompt_versions WHERE prompt_id=$1",
                prompt_id,
            )
            next_ver = row["max_ver"] + 1
            await conn.execute(
                "INSERT INTO prompt_versions (id,prompt_id,content,version_number,change_note,created_at) "
                "VALUES ($1,$2,$3,$4,$5,$6)",
                new_id(), prompt_id, old_content, next_ver, change_note, ts,
            )

        # Build SET clause for only provided fields
        sets = ["updated_at=$1"]
        params: list = [ts]
        idx = 2

        if title is not None:
            sets.append(f"title=${idx}"); params.append(title); idx += 1
        if description is not None:
            sets.append(f"description=${idx}"); params.append(description); idx += 1
        if content is not None:
            sets.append(f"content=${idx}"); params.append(content); idx += 1
        if tags is not None:
            sets.append(f"tags=${idx}"); params.append(json.dumps(tags)); idx += 1
        if category is not None:
            sets.append(f"category=${idx}"); params.append(category); idx += 1
        if is_favorite is not None:
            sets.append(f"is_favorite=${idx}"); params.append(1 if is_favorite else 0); idx += 1
        if variables is not None:
            sets.append(f"variables=${idx}"); params.append(json.dumps(variables)); idx += 1

        params.append(prompt_id)
        await conn.execute(
            f"UPDATE prompts SET {', '.join(sets)} WHERE id=${idx}", *params
        )

    return await get_prompt(prompt_id)


async def delete_prompt(prompt_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM prompts WHERE id=$1", prompt_id)


async def increment_usage(prompt_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE prompts SET usage_count=usage_count+1, updated_at=$1 WHERE id=$2",
            now_iso(), prompt_id,
        )


async def set_favorite(prompt_id: str, is_favorite: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE prompts SET is_favorite=$1, updated_at=$2 WHERE id=$3",
            1 if is_favorite else 0, now_iso(), prompt_id,
        )


# ── Versions ─────────────────────────────────────────────────────────────────

async def get_versions(prompt_id: str) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM prompt_versions WHERE prompt_id=$1 ORDER BY version_number DESC",
            prompt_id,
        )
    return [dict(r) for r in rows]


# ── Categories ───────────────────────────────────────────────────────────────

async def list_categories() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM prompt_categories ORDER BY name ASC")
    return [dict(r) for r in rows]


async def create_category(name: str, color: str = "#6366f1", icon: Optional[str] = None) -> dict:
    cid = new_id()
    ts = now_iso()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prompt_categories (id,created_at,name,color,icon) VALUES ($1,$2,$3,$4,$5)"
            " ON CONFLICT (name) DO NOTHING",
            cid, ts, name, color, icon,
        )
        row = await conn.fetchrow("SELECT * FROM prompt_categories WHERE name=$1", name)
    return dict(row)


async def delete_category(category_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM prompt_categories WHERE id=$1", category_id)
