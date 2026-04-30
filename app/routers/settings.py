"""
Pipeline Settings Router

GET  /api/settings/pipeline  — return current lead count + send delay config
POST /api/settings/pipeline  — save lead count + send delay config
"""

from fastapi import APIRouter, HTTPException
from app.services.database import get_pipeline_config, save_pipeline_config

router = APIRouter()


@router.get("/pipeline")
async def get_pipeline_settings():
    """Return current pipeline automation settings."""
    cfg = await get_pipeline_config()
    delay = cfg["send_delay_seconds"]
    return {
        "lead_count":         cfg["lead_count"],
        "send_delay_seconds": delay,
        "send_delay_display": {
            "hours":   delay // 3600,
            "minutes": (delay % 3600) // 60,
            "seconds": delay % 60,
        },
    }


@router.post("/pipeline")
async def save_pipeline_settings(body: dict):
    """
    Save pipeline automation settings.

    Body (any combination):
      { "lead_count": 150 }
      { "send_delay_seconds": 3600 }
      { "hours": 1, "minutes": 30, "seconds": 0 }
      { "lead_count": 200, "hours": 0, "minutes": 30, "seconds": 0 }
    """
    # Lead count
    lead_count = int(body.get("lead_count") or 100)
    if not 10 <= lead_count <= 500:
        raise HTTPException(status_code=422, detail="lead_count must be between 10 and 500")

    # Delay — accept either total seconds or H/M/S breakdown
    if "send_delay_seconds" in body:
        delay_seconds = int(body["send_delay_seconds"])
    else:
        h = int(body.get("hours", 0) or 0)
        m = int(body.get("minutes", 0) or 0)
        s = int(body.get("seconds", 0) or 0)
        delay_seconds = h * 3600 + m * 60 + s

    if delay_seconds < 0:
        raise HTTPException(status_code=422, detail="Delay cannot be negative")
    if delay_seconds > 86400:
        raise HTTPException(status_code=422, detail="Delay cannot exceed 24 hours")

    cfg = await save_pipeline_config(lead_count, delay_seconds)
    return {
        "success":            True,
        "lead_count":         cfg["lead_count"],
        "send_delay_seconds": cfg["send_delay_seconds"],
        "send_delay_display": {
            "hours":   cfg["send_delay_seconds"] // 3600,
            "minutes": (cfg["send_delay_seconds"] % 3600) // 60,
            "seconds": cfg["send_delay_seconds"] % 60,
        },
    }
