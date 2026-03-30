"""
Analytics Router
Returns pipeline metrics for the dashboard.
"""

import logging
from fastapi import APIRouter
from app.services import database as db

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/overview")
async def get_overview():
    """Main analytics endpoint — all key metrics in one call."""
    return await db.get_analytics()


@router.get("/idle-deals")
async def get_idle_deals(days: int = 7):
    """Return deals that have had no activity for more than `days` days."""
    deals = await db.get_idle_deals(days=days)
    return {
        "idle_deals": deals,
        "count":      len(deals),
        "threshold_days": days,
    }


@router.get("/campaigns")
async def get_campaigns():
    """Campaign performance breakdown."""
    data = await db.get_analytics()
    return {
        "campaigns":     data["campaigns"],
        "total_replies": data["total_replies"],
    }


@router.get("/sentiment")
async def get_sentiment():
    """Sentiment distribution across all deals."""
    data = await db.get_analytics()
    return {
        "hot":   data["hot_leads"],
        "warm":  data["warm_leads"],
        "cold":  data["cold_leads"],
        "total": data["total_replies"],
    }
