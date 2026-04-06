from fastapi import APIRouter, Query
from typing import Optional
from app.services.news_service import (
    get_news_feed,
    get_clustered_feed,
    run_fetch_cycle,
)

router = APIRouter(prefix="/news", tags=["news"])

SECTIONS    = ["indian_market", "global_market", "macro_impact", "swing_signals"]
EVENT_TYPES = ["Earnings", "Regulation", "Macro", "M&A", "Fraud/Negative", "General"]
ACTIONS     = ["Buy", "Sell", "Hold"]


@router.get("/feed")
async def news_feed(
    section:    Optional[str] = Query(None, description="Filter by section"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    action:     Optional[str] = Query(None, description="Filter by action (Buy/Sell/Hold)"),
    stock:      Optional[str] = Query(None, description="Filter by NSE stock symbol"),
    limit:      int           = Query(20, ge=1, le=100),
):
    """Returns latest enriched news articles with sentiment scores."""
    return await get_news_feed(
        section=section, limit=limit,
        event_type=event_type, action=action, stock=stock,
    )


@router.get("/clustered")
async def clustered_feed(
    section: Optional[str] = Query(None, description="Filter by section"),
    limit:   int           = Query(50, ge=1, le=200),
):
    """Returns news grouped by stock + event type (deduped clusters)."""
    return await get_clustered_feed(section=section, limit=limit)


@router.get("/sections")
async def list_sections():
    return {"sections": SECTIONS, "event_types": EVENT_TYPES, "actions": ACTIONS}


@router.post("/refresh")
async def force_refresh():
    """Manually trigger a fetch + sentiment cycle."""
    totals = await run_fetch_cycle()
    return {"status": "ok", "inserted": totals}


@router.post("/macro/update")
async def update_macro_signal(
    factor:     str   = Query(..., description="e.g. RBI_POLICY"),
    direction:  float = Query(..., ge=-1.0, le=1.0),
    weight:     float = Query(1.0, ge=0.1, le=3.0),
    confidence: float = Query(0.5, ge=0.0, le=1.0),
):
    """Upsert a structured macro signal into MongoDB."""
    from datetime import datetime, timezone
    from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
    from app.core.config import settings

    client = AsyncIOMotorClient(settings.MONGODB_URI, serverSelectionTimeoutMS=3000)
    col    = client["quantedge"]["macro_signals"]

    doc = {
        "factor": factor.upper(), "direction": direction,
        "weight": weight, "confidence": confidence,
        "updated_at": datetime.now(timezone.utc),
    }
    await col.update_one({"factor": factor.upper()}, {"$set": doc}, upsert=True)
    return {"status": "ok", "signal": doc}
