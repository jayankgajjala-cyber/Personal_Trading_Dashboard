"""
global_data.py  —  Global Macro Ticker via Finnhub
====================================================
Instruments: SPY, QQQ, DIA, ^VIX, ^TNX, DXY, CL=F, GC=F
Cache: 10-minute TTL to stay within Finnhub free tier (60 req/min)

Usage:
  GET /global/macro  → List[MacroQuote]

MacroQuote = {
  "symbol":     str,   e.g. "SPY"
  "label":      str,   e.g. "S&P 500 ETF"
  "price":      float,
  "change":     float, absolute change
  "pct_change": float, percent change
  "cached":     bool,  True if served from cache
}
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, TypedDict

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = "d77cgspr01qp6afl4qjgd77cgspr01qp6afl4qk0"
CACHE_TTL_S     = 600   # 10 minutes

# ── Instruments ───────────────────────────────────────────────────────────────
# (finnhub_symbol, display_label)
MACRO_INSTRUMENTS: List[tuple[str, str]] = [
    ("SPY",   "S&P 500"),
    ("QQQ",   "NASDAQ"),
    ("DIA",   "Dow Jones"),
    ("^VIX",  "VIX"),
    ("^TNX",  "US 10Y"),
    ("DXY",   "Dollar"),
    ("CL=F",  "Crude Oil"),
    ("GC=F",  "Gold"),
]

# ── Types ─────────────────────────────────────────────────────────────────────

class MacroQuote(TypedDict):
    symbol:     str
    label:      str
    price:      float
    change:     float
    pct_change: float
    cached:     bool


_EMPTY_QUOTE: MacroQuote = {
    "symbol": "", "label": "", "price": 0.0,
    "change": 0.0, "pct_change": 0.0, "cached": False,
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache:          Optional[List[MacroQuote]] = None
_cache_ts:       float                      = 0.0
_cache_lock:     asyncio.Lock               = asyncio.Lock()  # created lazily
_executor:       ThreadPoolExecutor         = ThreadPoolExecutor(max_workers=2, thread_name_prefix="macro")


def _cache_valid() -> bool:
    return _cache is not None and (time.monotonic() - _cache_ts) < CACHE_TTL_S


# ── Finnhub fetch (sync, runs in executor) ────────────────────────────────────

def _fetch_macro_sync() -> List[MacroQuote]:
    """
    Fetches all macro instruments from Finnhub /quote in one loop.
    Runs in executor — Finnhub SDK is blocking.
    Rate: 8 instruments × 1 req = 8 req (well within 60/min free tier).
    """
    try:
        import finnhub  # type: ignore
        client = finnhub.Client(api_key=FINNHUB_API_KEY)
    except ImportError:
        logger.error("[macro] finnhub-python not installed")
        return []
    except Exception as e:
        logger.error("[macro] Finnhub client init: %s", e)
        return []

    results: List[MacroQuote] = []

    for fh_sym, label in MACRO_INSTRUMENTS:
        try:
            q      = client.quote(fh_sym)
            price  = float(q.get("c") or 0)
            prev   = float(q.get("pc") or 0)
            change = round(price - prev, 4) if price and prev else 0.0
            pct    = round((change / prev) * 100, 3) if prev else 0.0

            results.append({
                "symbol":     fh_sym,
                "label":      label,
                "price":      round(price, 4),
                "change":     change,
                "pct_change": pct,
                "cached":     False,
            })
            logger.debug("[macro] %s %s = %.4f (%.3f%%)", label, fh_sym, price, pct)

        except Exception as e:
            msg = str(e)
            # Silent on auth/rate errors — do not spam logs
            if "401" in msg:
                logger.error("[macro] Finnhub 401 INVALID KEY")
                break
            if "403" in msg:
                logger.debug("[macro] %s Finnhub 403 — permissions", fh_sym)
            elif "429" in msg:
                logger.debug("[macro] Finnhub 429 — rate limited")
            else:
                logger.debug("[macro] %s error: %s", fh_sym, e)

            results.append({**_EMPTY_QUOTE, "symbol": fh_sym, "label": label})

    return results


# ── Public async API ──────────────────────────────────────────────────────────

async def get_macro_quotes() -> List[MacroQuote]:
    """
    Returns cached macro quotes if fresh (< 10 min old).
    Otherwise fetches from Finnhub in executor and refreshes cache.
    Thread-safe via asyncio.Lock.
    """
    global _cache, _cache_ts, _cache_lock

    # Lazy-create lock (must be created inside a running event loop)
    if not isinstance(_cache_lock, asyncio.Lock):
        _cache_lock = asyncio.Lock()

    if _cache_valid():
        # Return copy with cached=True flag
        return [{**q, "cached": True} for q in _cache]  # type: ignore[misc]

    async with _cache_lock:
        # Double-check after acquiring lock
        if _cache_valid():
            return [{**q, "cached": True} for q in _cache]  # type: ignore[misc]

        loop = asyncio.get_event_loop()
        try:
            quotes = await asyncio.wait_for(
                loop.run_in_executor(_executor, _fetch_macro_sync),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            logger.warning("[macro] Finnhub fetch timed out")
            quotes = []
        except Exception as e:
            logger.warning("[macro] fetch error: %s", e)
            quotes = []

        if quotes:
            _cache    = quotes
            _cache_ts = time.monotonic()
            logger.info("[macro] Cache refreshed — %d instruments", len(quotes))
        elif _cache:
            logger.warning("[macro] Fetch failed — serving stale cache")
            return [{**q, "cached": True} for q in _cache]  # type: ignore[misc]

        return quotes


def cache_age_seconds() -> float:
    """How old is the current cache in seconds. Returns inf if no cache."""
    return (time.monotonic() - _cache_ts) if _cache is not None else float("inf")


def invalidate_cache() -> None:
    """Force next call to re-fetch from Finnhub."""
    global _cache, _cache_ts
    _cache    = None
    _cache_ts = 0.0
    logger.info("[macro] Cache invalidated")
