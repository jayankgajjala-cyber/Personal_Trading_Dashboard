from fastapi import APIRouter, Query
from typing import List, Optional
from app.services.news_service import get_news_feed, run_fetch_cycle

router = APIRouter(prefix="/news", tags=["news"])

SECTIONS = ["indian_market", "global_market", "macro_impact", "swing_signals"]


@router.get("/feed")
async def news_feed(
    section: Optional[str] = Query(None, description="Filter by section"),
    limit:   int           = Query(20,   ge=1, le=100),
):
    """Returns latest news articles. Omit section for all."""
    return await get_news_feed(section=section, limit=limit)


@router.get("/sections")
async def list_sections():
    return {"sections": SECTIONS}


@router.post("/refresh")
async def force_refresh():
    """Manually trigger a fetch cycle (admin/debug use)."""
    totals = await run_fetch_cycle()
    return {"status": "ok", "inserted": totals}
