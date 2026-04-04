from fastapi import APIRouter
from app.utils.global_data import get_macro_quotes, cache_age_seconds, MacroQuote
from typing import List

router = APIRouter(prefix="/global", tags=["global"])


@router.get("/macro", response_model=List[MacroQuote])
async def macro_quotes():
    """
    Returns global macro instrument prices (SPY, QQQ, VIX, Gold, Crude, etc.)
    Served from a 10-minute Finnhub cache to respect free-tier rate limits.
    """
    return await get_macro_quotes()


@router.get("/macro/cache-status")
async def macro_cache_status():
    age = cache_age_seconds()
    return {
        "cache_age_seconds": round(age, 1) if age != float("inf") else None,
        "is_fresh": age < 600,
    }
