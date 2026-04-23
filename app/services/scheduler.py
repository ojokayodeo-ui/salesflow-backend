"""
Email Sequence Scheduler

Runs as a background task inside FastAPI.
Every 10 minutes, checks for any sequence emails that are due
and sends them via Outlook automatically.

Schedule logic:
- Each sequence has a send_time (e.g. "09:00") and timezone
- Each sequence has allowed_days (e.g. ["mon","tue","wed","thu","fri"])
- When step N is sent, step N+1 is scheduled:
    base = sent_time + delay_days
    if base falls on a disallowed day, roll forward to next allowed day
    set time to send_time in the sequence timezone
    convert to UTC for storage
"""

import logging
import asyncio
from datetime import datetime, timedelta
import pytz
from app.services import database as db
from app.services.outlook import send_email_via_outlook
from app.config import settings

logger = logging.getLogger(__name__)

WEEKDAY_MAP = {"mon":0,"tue":1,"wed":2,"thu":3,"fri":4,"sat":5,"sun":6}


def calculate_send_at(
    from_dt: datetime,
    delay_days: int,
    send_time: str,        # "HH:MM"
    timezone_str: str,     # e.g. "Europe/London"
    allowed_days: list,    # e.g. ["mon","tue","wed","thu","fri"]
) -> datetime:
    """
    Calculate the UTC datetime when the next email should be sent.
    Respects delay_days, send_time, timezone, and allowed_days.
    """
    try:
        tz = pytz.timezone(timezone_str)
    except Exception:
        tz = pytz.UTC

    # Parse send_time
    try:
        hour, minute = [int(x) for x in send_time.split(":")]
    except Exception:
        hour, minute = 9, 0

    allowed = [WEEKDAY_MAP.get(d.lower(), -1) for d in (allowed_days or list(WEEKDAY_MAP.keys()))]
    allowed = [d for d in allowed if d >= 0]
    if not allowed:
        allowed = list(range(5))  # Mon-Fri default

    # Start from local time
    local_now = from_dt.astimezone(tz)
    candidate = local_now + timedelta(days=delay_days)
    candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # Roll forward if the candidate day isn't allowed
    for _ in range(14):  # max 2 weeks forward
        if candidate.weekday() in allowed:
            break
        candidate += timedelta(days=1)

    return candidate.astimezone(pytz.UTC)


async def process_due_emails():
    """Check for due emails and send them."""
    due = await db.get_due_emails()
    if not due:
        return

    logger.info("Scheduler: %d email(s) due", len(due))

    for email in due:
        deal = await db.get_deal(email["deal_id"])
        if not deal:
            await db.mark_email_failed(email["id"], "Deal not found")
            continue

        # If sequence was stopped, cancel remaining emails
        if not deal.get("seq_active") and deal.get("seq_stop_reason"):
            await db.mark_email_failed(email["id"], "Sequence stopped")
            continue

        try:
            import json as _json
            extra_attachments = []
            if email.get("attachments"):
                try:
                    extra_attachments = _json.loads(email["attachments"])
                except Exception:
                    pass

            result = await send_email_via_outlook(
                to_email          = deal["email"],
                to_name           = deal["name"].split(" ")[0],
                from_name         = settings.default_from_name,
                subject           = email["step_subject"],
                body              = email["step_body"],
                csv_data          = None,
                csv_filename      = "",
                extra_attachments = extra_attachments,
            )

            if result["success"]:
                await db.mark_email_sent(email["id"])
                # Advance the sequence step on the deal
                await db.advance_sequence_step(deal["deal_id"] if "deal_id" in email else email["deal_id"])
                logger.info(
                    "Scheduler: sent step %d to %s",
                    email["step_index"] + 1, deal["email"],
                )
            else:
                await db.mark_email_failed(email["id"], result.get("error", "Unknown error"))
                logger.error("Scheduler: send failed for %s: %s", deal["email"], result.get("error"))

        except Exception as exc:
            await db.mark_email_failed(email["id"], str(exc))
            logger.exception("Scheduler: exception sending to %s", deal["email"])


async def run_scheduler():
    """Background loop — checks every 10 minutes."""
    logger.info("Email scheduler started — checking every 10 minutes")
    while True:
        try:
            await process_due_emails()
        except Exception as exc:
            logger.exception("Scheduler loop error: %s", exc)
        await asyncio.sleep(600)  # 10 minutes
