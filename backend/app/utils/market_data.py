"""
market_data.py
==============
LTP fetcher for NSE / BSE / US / CRYPTO symbols via yfinance.

ROOT CAUSE OF HANGING:
  yfinance makes blocking HTTP calls. Calling them directly inside an
  async FastAPI route blocks the uvicorn event loop, causing the request
  to stay 'Pending' forever. ALL yfinance work runs in a dedicated
  ThreadPoolExecutor and is awaited via asyncio.run_in_executor so the
  event loop is never blocked.

ARCHITECTURE:
  async fetch_ltp_batch_async()   ← called by FastAPI route (await)
    └─ asyncio.run_in_executor()  ← offloads to thread pool
         └─ _fetch_all_sync()     ← pure sync: runs all yfinance calls
              └─ _fetch_one_sync() per ticker (ThreadPoolExecutor, 8s cap)
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from functools import partial
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Hard cap on the entire batch so the API never hangs
BATCH_TIMEOUT_SECONDS = 8.0

# Dedicated thread pool — keeps yfinance threads separate from uvicorn workers
_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="yf_ltp")


# ── Symbol resolution ─────────────────────────────────────────────────────────

def _resolve(symbol: str, exchange: str = "NSE") -> str:
    """Map a stored symbol + exchange to a yfinance ticker string."""
    s = symbol.strip().upper()
    if "." in s:
        return s                       # already qualified (.NS / .BO / .L)
    ex = exchange.strip().upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s                       # raw: AAPL, BTC-USD
    return f"{s}.NS"                   # default → NSE


# ── Per-symbol sync fetcher ───────────────────────────────────────────────────

def _fetch_one_sync(yf_ticker: str, original: str) -> Tuple[str, Optional[float]]:
    """
    Blocking fetch for a single ticker. Runs inside a worker thread.
    Tries fast_info first, falls back to 1d/1m history tail.
    Returns (original_symbol, price_or_None).
    """
    try:
        import yfinance as yf

        t = yf.Ticker(yf_ticker)

        # ── Attempt 1: fast_info (single lightweight HTTP call) ───────────
        try:
            fi = t.fast_info
            for attr in ("last_price", "regularMarketPrice", "previous_close"):
                val = getattr(fi, attr, None)
                if val is not None:
                    try:
                        fval = float(val)
                        if fval > 0:
                            price = round(fval, 4)
                            print(f"[LTP] {yf_ticker} fast_info.{attr} = {price}")
                            return (original, price)
                    except (TypeError, ValueError):
                        pass
        except Exception as fi_err:
            print(f"[LTP] {yf_ticker} fast_info error: {fi_err}")

        # ── Attempt 2: 1-day 1-min history tail ──────────────────────────
        print(f"[LTP] {yf_ticker} falling back to history()")
        hist = t.history(period="1d", interval="1m", auto_adjust=True)
        if not hist.empty:
            close_col = next(
                (c for c in hist.columns if str(c).lower().strip("()' ") == "close"),
                None,
            )
            if close_col is not None:
                series = hist[close_col].dropna()
                if not series.empty:
                    price = round(float(series.iloc[-1]), 4)
                    print(f"[LTP] {yf_ticker} history tail = {price}")
                    return (original, price)

        print(f"[LTP] {yf_ticker} no price from any source")
        return (original, None)

    except Exception as e:
        print(f"[LTP] {yf_ticker} exception: {type(e).__name__}: {e}")
        return (original, None)


# ── Batch sync orchestrator ───────────────────────────────────────────────────

def _fetch_all_sync(
    unique_tickers: List[str],
    reverse_map: Dict[str, str],
    symbols: List[str],
) -> Dict[str, Optional[float]]:
    """
    Runs all per-ticker fetches in parallel worker threads.
    Hard-capped at BATCH_TIMEOUT_SECONDS total wall time.
    Pure sync — safe to call from run_in_executor.
    """
    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}

    futures: Dict = {}
    with ThreadPoolExecutor(max_workers=min(len(unique_tickers), 15),
                            thread_name_prefix="yf_sym") as pool:
        for yf_ticker in unique_tickers:
            original = reverse_map.get(yf_ticker.upper())
            if not original:
                print(f"[LTP] WARNING: no reverse_map entry for '{yf_ticker}'")
                continue
            futures[pool.submit(_fetch_one_sync, yf_ticker, original)] = yf_ticker

        try:
            for future in as_completed(futures, timeout=BATCH_TIMEOUT_SECONDS):
                try:
                    orig, price = future.result()
                    result[orig] = price
                except Exception as e:
                    print(f"[LTP] future result error: {e}")

        except FuturesTimeout:
            resolved = sum(1 for v in result.values() if v is not None)
            print(
                f"[LTP] TIMEOUT after {BATCH_TIMEOUT_SECONDS}s — "
                f"{resolved}/{len(symbols)} resolved before cutoff"
            )

    return result


# ── Public async API (called by FastAPI routes) ───────────────────────────────

async def fetch_ltp_batch_async(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Async wrapper — offloads all blocking yfinance work to _executor so
    the uvicorn event loop is never blocked.

    Returns {original_symbol: float | None}.
    Keys always match the exact symbols passed in (never .NS/.BO suffixed).
    """
    if not symbols:
        print("[LTP] fetch_ltp_batch_async called with empty list")
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }
    reverse_map: Dict[str, str] = {v.upper(): k for k, v in ticker_map.items()}
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))

    print(f"[LTP] Batch start — {len(unique_tickers)} tickers: {unique_tickers}")
    print(f"[LTP] Reverse map: {reverse_map}")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        _executor,
        partial(_fetch_all_sync, unique_tickers, reverse_map, symbols),
    )

    fetched = sum(1 for v in result.values() if v is not None)
    print(f"[LTP] Batch done — {fetched}/{len(symbols)} resolved: {result}")
    return result


# ── Backwards-compat sync shim (do not use in async routes) ──────────────────

def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Sync shim kept for import compatibility.
    FastAPI routes MUST use fetch_ltp_batch_async instead.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're already inside an event loop — caller should use the async version
            logger.warning(
                "fetch_ltp_batch() called from inside a running event loop. "
                "Use fetch_ltp_batch_async() in async contexts."
            )
            # Return empty rather than deadlock
            return {sym: None for sym in symbols}
        return loop.run_until_complete(
            fetch_ltp_batch_async(symbols, exchanges)
        )
    except Exception as e:
        logger.error("fetch_ltp_batch sync shim error: %s", e)
        return {sym: None for sym in (symbols or [])}
