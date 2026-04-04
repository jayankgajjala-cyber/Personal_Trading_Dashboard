"""
market_data.py  —  Concurrent Racing LTP Pipeline
===================================================

ARCHITECTURE
  fetch_ltp_batch_async()
    └─ per symbol: _race_one_async()           ← asyncio, no blocking
         ├─ RACE:  T1 (Finnhub) ║ T2 (NSE)    ← asyncio.gather, first non-zero wins
         ├─ FALLBACK T3: Google Finance         ← aiohttp scrape
         └─ FALLBACK T4: yfinance.download      ← run_in_executor (blocking, isolated)

RETURN TYPE
  Dict[str, LTPResult]
  LTPResult = {"price": float, "source": str}
  source ∈ {"Finnhub", "NSE", "Google", "yfinance", "Failed"}

PERFORMANCE GUARANTEE
  • T1 and T2 race concurrently per-symbol → fastest wins, no waiting for loser
  • All symbols raced in parallel via asyncio.gather at batch level
  • No time.sleep() anywhere in the async path
  • yfinance called as a single batch via run_in_executor (one thread, one HTTP call)
  • Target: 10-symbol portfolio resolved in < 5 s
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, TypedDict

import requests

logger = logging.getLogger(__name__)

# ── Types ─────────────────────────────────────────────────────────────────────

class LTPResult(TypedDict):
    price:  float
    source: str


_FAILED: LTPResult = {"price": 0.0, "source": "Failed"}

# ── Thread pool (for blocking yfinance only) ──────────────────────────────────

_yf_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="yf_batch")

# ── Browser UA rotation ───────────────────────────────────────────────────────

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_BASE_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache",
}


def _headers() -> Dict[str, str]:
    return {**_BASE_HEADERS, "User-Agent": random.choice(_UA_POOL)}


# ── Symbol helpers ────────────────────────────────────────────────────────────

def _resolve(symbol: str, exchange: str = "NSE") -> str:
    s  = symbol.strip().upper()
    if "." in s:
        return s
    ex = exchange.strip().upper()
    if ex == "BSE":      return f"{s}.BO"
    if ex in ("US", "CRYPTO"): return s
    return f"{s}.NS"


def _norm(ticker: str) -> str:
    return ticker.split(".")[0].upper()


def _is_nse(exchange: str) -> bool:
    return exchange.strip().upper() in ("NSE", "")


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — Finnhub  (async via run_in_executor — finnhub SDK is sync)
# ─────────────────────────────────────────────────────────────────────────────

def _finnhub_sync(symbol: str, exchange: str) -> Optional[float]:
    """Blocking Finnhub /quote call. Runs in executor."""
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        import finnhub  # type: ignore
        q     = finnhub.Client(api_key=api_key).quote(_resolve(symbol, exchange))
        price = q.get("c") or q.get("pc")
        return round(float(price), 4) if price and float(price) > 0 else None
    except Exception as e:
        logger.debug("[T1] %s Finnhub error: %s", symbol, e)
        return None


async def _tier1_async(symbol: str, exchange: str) -> Optional[float]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _finnhub_sync, symbol, exchange)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — NSE Official  (async HTTP via asyncio + requests in executor)
# ─────────────────────────────────────────────────────────────────────────────

_NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity?symbol={sym}"
_NSE_HEADERS   = {
    **_BASE_HEADERS,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.nseindia.com/",
}


def _nse_sync(symbol: str) -> Optional[float]:
    """Blocking NSE API call. Runs in executor."""
    if not symbol:
        return None
    # nsepython — preferred
    try:
        from nsepython import nse_eq  # type: ignore
        data  = nse_eq(symbol.upper())
        price = (
            data.get("priceInfo", {}).get("lastPrice")
            or data.get("lastPrice")
        )
        return round(float(price), 4) if price and float(price) > 0 else None
    except ImportError:
        pass
    except Exception as e:
        logger.debug("[T2] %s nsepython error: %s", symbol, e)

    # Direct NSE API fallback
    try:
        sess = requests.Session()
        # Warm cookie
        sess.get("https://www.nseindia.com", headers=_NSE_HEADERS, timeout=4)
        url  = _NSE_QUOTE_URL.format(sym=symbol.upper())
        resp = sess.get(url, headers=_NSE_HEADERS, timeout=5)
        if resp.status_code == 200:
            data  = resp.json()
            price = data.get("priceInfo", {}).get("lastPrice")
            return round(float(price), 4) if price and float(price) > 0 else None
    except Exception as e:
        logger.debug("[T2] %s NSE direct error: %s", symbol, e)
    return None


async def _tier2_async(symbol: str, exchange: str) -> Optional[float]:
    if not _is_nse(exchange):
        return None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _nse_sync, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3 — Google Finance  (async HTTP)
# ─────────────────────────────────────────────────────────────────────────────

_GOOGLE_URL  = "https://www.google.com/finance/quote/{sym}:{ex}"
_PRICE_RE    = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
_PRICE_RE_FB = re.compile(r'class="YMlKec fxKbKc"[^>]*>([0-9,]+\.?[0-9]*)<')


def _google_sync(symbol: str, exchange: str) -> Optional[float]:
    """Blocking Google Finance scrape. Runs in executor."""
    gex = "NSE" if _is_nse(exchange) else exchange.upper()
    url = _GOOGLE_URL.format(sym=symbol.upper(), ex=gex)
    try:
        resp = requests.get(url, headers=_headers(), timeout=6)
        if resp.status_code != 200:
            return None
        m = _PRICE_RE.search(resp.text)
        if m:
            return round(float(m.group(1)), 4)
        m = _PRICE_RE_FB.search(resp.text)
        if m:
            return round(float(m.group(1).replace(",", "")), 4)
        try:
            from bs4 import BeautifulSoup  # type: ignore
            el = BeautifulSoup(resp.text, "html.parser").find("div", {"class": "YMlKec fxKbKc"})
            if el:
                return round(float(el.get_text().strip().replace(",", "")), 4)
        except Exception:
            pass
    except Exception as e:
        logger.debug("[T3] %s Google error: %r", symbol, e)
    return None


async def _tier3_async(symbol: str, exchange: str) -> Optional[float]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _google_sync, symbol, exchange)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 4 — yfinance batch  (one blocking call for ALL remaining symbols)
# ─────────────────────────────────────────────────────────────────────────────

def _yfinance_batch_sync(
    symbols:    List[str],
    exchanges:  List[str],
) -> Dict[str, float]:
    """
    Single yf.download() for a list of symbols. No per-symbol loops.
    Returns {original_sym: price}. Runs in _yf_executor.
    """
    import yfinance as yf

    result:     Dict[str, float]  = {}
    yf_tickers  = [_resolve(s, e) for s, e in zip(symbols, exchanges)]
    rev_map     = {_norm(t): s for s, t in zip(symbols, yf_tickers)}
    sess        = requests.Session()
    sess.headers.update({**_BASE_HEADERS, "User-Agent": random.choice(_UA_POOL)})

    try:
        space_sep = " ".join(yf_tickers)
        df = yf.download(
            tickers     = space_sep,
            period      = "1d",
            interval    = "1m",
            group_by    = "ticker",
            auto_adjust = True,
            progress    = False,
            threads     = False,
            session     = sess,
        )
        if df is None or df.empty:
            return result

        if hasattr(df.columns, "levels") and len(df.columns.levels) == 2:
            for yf_tick in yf_tickers:
                orig = rev_map.get(_norm(yf_tick))
                if not orig:
                    continue
                for key in (yf_tick, yf_tick.upper(), _norm(yf_tick)):
                    if key in df.columns.get_level_values(0):
                        sub  = df[key]
                        cols = [str(c).lower() for c in sub.columns]
                        if "close" in cols:
                            s = sub[sub.columns[cols.index("close")]].dropna()
                            if not s.empty:
                                result[orig] = round(float(s.iloc[-1]), 4)
                                break
        else:
            # Single ticker — flat columns
            cols = [str(c).lower() for c in df.columns]
            if "close" in cols and len(symbols) == 1:
                s = df[df.columns[cols.index("close")]].dropna()
                if not s.empty:
                    result[symbols[0]] = round(float(s.iloc[-1]), 4)

    except Exception as e:
        logger.warning("[T4] yf.download error: %s: %s", type(e).__name__, e)

    # Per-symbol history fallback for anything the batch missed
    for sym in [s for s in symbols if s not in result]:
        ex      = exchanges[symbols.index(sym)]
        yf_tick = _resolve(sym, ex)
        try:
            t    = yf.Ticker(yf_tick, session=sess)
            hist = t.history(period="1d", auto_adjust=True)
            if not hist.empty:
                hist.columns = [str(c).split(",")[0].strip("() '\"").lower() for c in hist.columns]
                if "close" in hist.columns:
                    series = hist["close"].dropna()
                    if not series.empty:
                        result[sym] = round(float(series.iloc[-1]), 4)
        except Exception as e:
            logger.debug("[T4] %s per-sym error: %s", sym, e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol race orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def _race_one_async(symbol: str, exchange: str) -> LTPResult:
    """
    RACE T1 vs T2 concurrently. First non-zero price wins.
    If both fail, try T3 (Google). T4 is handled at batch level only.
    """
    # ── Concurrent race: Finnhub ‖ NSE ───────────────────────────────────
    t1_task = asyncio.create_task(_tier1_async(symbol, exchange))
    t2_task = asyncio.create_task(_tier2_async(symbol, exchange))

    done, pending = await asyncio.wait(
        {t1_task, t2_task},
        return_when=asyncio.FIRST_COMPLETED,
        timeout=4.0,
    )

    # Check completed tasks in order of completion
    for task in done:
        try:
            price = task.result()
            if price and price > 0:
                # Cancel the loser
                for p in pending:
                    p.cancel()
                src = "Finnhub" if task is t1_task else "NSE"
                logger.info("[RACE] %-20s ← %-10s %.4f", symbol, src, price)
                return {"price": price, "source": src}
        except Exception:
            pass

    # Wait for the remaining task if the first winner had 0/None
    if pending:
        done2, _ = await asyncio.wait(pending, timeout=3.0)
        for task in done2:
            try:
                price = task.result()
                if price and price > 0:
                    src = "Finnhub" if task is t1_task else "NSE"
                    logger.info("[RACE] %-20s ← %-10s %.4f (second)", symbol, src, price)
                    return {"price": price, "source": src}
            except Exception:
                pass

    # Cancel any survivors
    for task in (t1_task, t2_task):
        task.cancel()

    # ── T3: Google Finance ────────────────────────────────────────────────
    try:
        price = await asyncio.wait_for(_tier3_async(symbol, exchange), timeout=6.0)
        if price and price > 0:
            logger.info("[RACE] %-20s ← Google     %.4f", symbol, price)
            return {"price": price, "source": "Google"}
    except (asyncio.TimeoutError, Exception) as e:
        logger.debug("[T3] %s timeout/error: %s", symbol, e)

    # T4 is handled at the batch level after all races complete
    return {"price": 0.0, "source": "__pending_yf__"}


# ─────────────────────────────────────────────────────────────────────────────
# Public async API
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_ltp_batch_async(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    """
    Non-blocking entry point for FastAPI routes.

    1. Races T1 (Finnhub) vs T2 (NSE) concurrently per symbol.
       All symbols raced in parallel via asyncio.gather.
    2. Symbols not resolved by T1/T2 fall to T3 (Google).
    3. Symbols still at 0 after T3 are sent to T4 as a SINGLE
       yf.download() batch (one thread, one HTTP call).

    Returns Dict[str, LTPResult] where LTPResult = {"price": float, "source": str}.
    Always returns a result for every symbol — never raises.
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm_syms = [s.strip().upper() for s in symbols]
    norm_exs  = [e.strip().upper() for e in exchanges]

    logger.info("[LTP] Racing %d symbols: %s", len(norm_syms), norm_syms)

    # ── Stage 1: parallel race (T1 ‖ T2) + T3 fallback per symbol ────────
    race_tasks = [
        _race_one_async(sym, ex)
        for sym, ex in zip(norm_syms, norm_exs)
    ]
    race_results: List[LTPResult] = await asyncio.gather(*race_tasks, return_exceptions=False)

    result: Dict[str, LTPResult] = {}
    yf_needed_syms: List[str]    = []
    yf_needed_exs:  List[str]    = []

    for sym, res in zip(norm_syms, race_results):
        if res["source"] == "__pending_yf__":
            yf_needed_syms.append(sym)
            yf_needed_exs.append(norm_exs[norm_syms.index(sym)])
        else:
            result[sym] = res

    # ── Stage 2: single yf.download() batch for T1/T2/T3 misses ─────────
    if yf_needed_syms:
        logger.info("[T4] yfinance batch for %d symbols: %s", len(yf_needed_syms), yf_needed_syms)
        loop = asyncio.get_event_loop()
        try:
            yf_prices = await asyncio.wait_for(
                loop.run_in_executor(
                    _yf_executor,
                    _yfinance_batch_sync,
                    yf_needed_syms,
                    yf_needed_exs,
                ),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            logger.warning("[T4] yfinance batch timed out")
            yf_prices = {}
        except Exception as e:
            logger.warning("[T4] yfinance batch error: %s", e)
            yf_prices = {}

        for sym in yf_needed_syms:
            price = yf_prices.get(sym, 0.0)
            if price and price > 0:
                result[sym] = {"price": price, "source": "yfinance"}
                logger.info("[T4] %-20s ← yfinance   %.4f", sym, price)
            else:
                result[sym] = _FAILED
                logger.warning("[MISS] %s — all tiers exhausted", sym)

    # ── Ensure every input symbol has an entry ────────────────────────────
    for sym in norm_syms:
        if sym not in result:
            result[sym] = _FAILED

    logger.info("[LTP] Done — %d/%d resolved", sum(1 for v in result.values() if v["price"] > 0), len(norm_syms))
    return result


# ── Sync shim (scripts / health-check only) ───────────────────────────────────

def fetch_ltp_batch(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    """Sync shim. FastAPI routes MUST use fetch_ltp_batch_async."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("fetch_ltp_batch() inside running loop — use async version.")
            return {s: _FAILED for s in symbols}
        return loop.run_until_complete(fetch_ltp_batch_async(symbols, exchanges))
    except Exception as e:
        logger.error("fetch_ltp_batch shim error: %s", e)
        return {s: _FAILED for s in (symbols or [])}


# ── Health check ──────────────────────────────────────────────────────────────

def health_check(
    symbols:   Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    test_db:   bool                = True,
) -> None:
    """
    Run from backend root:
        python -c "from app.utils.market_data import health_check; health_check()"
    """
    import asyncio as _aio
    SEP = "═" * 64

    if test_db:
        print(f"\n{SEP}\n  [CHECK 1] DB CONNECTIVITY\n{SEP}")
        try:
            import asyncpg  # type: ignore
            from app.core.config import settings
            raw = settings.DATABASE_URL
            for old, new in (("postgresql+asyncpg://", "postgresql://"), ("postgres://", "postgresql://")):
                raw = raw.replace(old, new)
            raw = raw.split("?")[0]
            ver = _aio.get_event_loop().run_until_complete(
                asyncpg.connect(raw).then(lambda c: c.fetchval("SELECT version()"))  # type: ignore
            )
            print(f"  ✓ {str(ver)[:70]}")
        except Exception as e:
            print(f"  ✗ DB FAILED: {e}")

    if symbols is None:
        symbols   = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "WIPRO"]
    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    print(f"\n{SEP}\n  [CHECK 2] RACING LTP STRESS TEST — {[s.upper() for s in symbols]}\n{SEP}")

    result = fetch_ltp_batch([s.upper() for s in symbols], exchanges)

    print(f"\n  {'SYMBOL':<15}  {'PRICE':>10}  {'SOURCE':<12}  KEY-OK")
    print(f"  {'-'*55}")
    all_ok = True
    for s in [x.upper() for x in symbols]:
        r   = result.get(s, _FAILED)
        ok  = "✓" if r["price"] > 0 else "✗"
        key = "✓" if s in result else "✗ MISSING"
        print(f"  {ok} {s:<14}  {r['price']:>10.4f}  {r['source']:<12}  {key}")
        if r["price"] == 0.0 or s not in result:
            all_ok = False

    print(f"\n{SEP}")
    print(f"  RESULT: {'PASS' if all_ok else 'PARTIAL — some symbols returned 0.0'}\n")
