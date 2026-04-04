"""
market_data.py  —  Concurrent Racing LTP Pipeline  (T1-Preferred)
==================================================================

RACING ARCHITECTURE
  _race_one_async(symbol, exchange)
    ├─ T1 (Finnhub) started first, given T1_HEADSTART_MS grace window
    ├─ T2 (NSE)     started T1_HEADSTART_MS later — only wins if T1 silent/zero
    ├─ T3 (Google)  sequential fallback when both T1+T2 fail
    └─ T4 (yfinance) single batch call for all symbols T1-T3 could not resolve

SYMBOLOGY  (critical for Finnhub NSE tickers)
  DB "RELIANCE" + exchange "NSE"  →  Finnhub/yfinance "RELIANCE.NS"
  DB "RELIANCE" + exchange "BSE"  →  "RELIANCE.BO"
  DB "AAPL"     + exchange "US"   →  "AAPL"   (no suffix)

T1 HEADSTART
  Finnhub is the authoritative source. We give it T1_HEADSTART_MS (300 ms)
  before T2 is allowed to start. If Finnhub returns a valid price within
  the headstart window it wins outright. If it is still pending after
  T1_HEADSTART_MS, T2 is started concurrently and the faster non-zero
  result wins. T3/T4 are only entered if both T1 and T2 yield nothing.

RETURN TYPE
  Dict[str, LTPResult]
  LTPResult = {"price": float, "source": str}
  source ∈ {"Finnhub", "NSE", "Google", "yfinance", "Failed"}
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, TypedDict

import requests

logger = logging.getLogger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────────
# Give Finnhub this many ms head-start before NSE is allowed to enter the race.
# Raise if Finnhub is consistently slower on your Railway region.
T1_HEADSTART_MS: int = 300

_yf_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="yf_batch")

# ── Types ─────────────────────────────────────────────────────────────────────

class LTPResult(TypedDict):
    price:  float
    source: str


_FAILED: LTPResult = {"price": 0.0, "source": "Failed"}

# ── UA rotation ───────────────────────────────────────────────────────────────

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


def _ua_headers() -> Dict[str, str]:
    return {**_BASE_HEADERS, "User-Agent": random.choice(_UA_POOL)}


# ── Symbol resolution ─────────────────────────────────────────────────────────

def _resolve(symbol: str, exchange: str = "NSE") -> str:
    """
    Map a bare DB symbol to the fully-qualified ticker required by
    Finnhub and yfinance.  Critical: 'RELIANCE' alone returns 0 from
    Finnhub; it must be 'RELIANCE.NS' for NSE-listed equities.
    """
    s  = symbol.strip().upper()
    if "." in s:
        return s                        # already qualified
    ex = exchange.strip().upper()
    if ex == "BSE":                return f"{s}.BO"
    if ex in ("US", "CRYPTO"):    return s
    return f"{s}.NS"                    # default → NSE


def _norm(ticker: str) -> str:
    """'RELIANCE.NS' → 'RELIANCE'"""
    return ticker.split(".")[0].upper()


def _is_nse(exchange: str) -> bool:
    return exchange.strip().upper() in ("NSE", "")


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — Finnhub
# ─────────────────────────────────────────────────────────────────────────────

def _finnhub_sync(symbol: str, exchange: str) -> Optional[float]:
    """
    Blocking Finnhub /quote.  Always uses the suffixed ticker so NSE
    symbols are correctly resolved (bare 'RELIANCE' returns c=0).
    Runs in executor — never called directly from async code.
    """
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        logger.debug("[T1] FINNHUB_API_KEY not set")
        return None

    fh_ticker = _resolve(symbol, exchange)   # e.g. "RELIANCE.NS"
    try:
        import finnhub  # type: ignore
        client = finnhub.Client(api_key=api_key)
        q      = client.quote(fh_ticker)
        # 'c' = current; 'pc' = previous close (used when market is closed)
        price  = q.get("c") or q.get("pc")
        if price and float(price) > 0:
            logger.info("[T1] %-20s ← Finnhub (%s) = %.4f", symbol, fh_ticker, float(price))
            return round(float(price), 4)
        logger.debug("[T1] %s Finnhub zero (c=%s pc=%s) — symbol may be unsupported or market closed",
                     symbol, q.get("c"), q.get("pc"))
        return None
    except Exception as e:
        msg = str(e)
        if "401" in msg:
            logger.error("[T1] Finnhub 401 INVALID KEY — check FINNHUB_API_KEY")
        elif "403" in msg:
            logger.warning("[T1] %s Finnhub 403 — symbol requires paid plan", fh_ticker)
        elif "429" in msg:
            logger.warning("[T1] Finnhub 429 — rate limited (60 req/min free)")
        else:
            logger.debug("[T1] %s Finnhub error: %s", symbol, e)
        return None


async def _tier1_async(symbol: str, exchange: str) -> Optional[float]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _finnhub_sync, symbol, exchange)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — NSE Official
# ─────────────────────────────────────────────────────────────────────────────

_NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity?symbol={sym}"
_NSE_HEADERS   = {
    **_BASE_HEADERS,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.nseindia.com/",
}


def _nse_sync(symbol: str) -> Optional[float]:
    """Blocking NSE quote. Runs in executor."""
    # nsepython preferred
    try:
        from nsepython import nse_eq  # type: ignore
        data  = nse_eq(symbol.upper())
        price = data.get("priceInfo", {}).get("lastPrice") or data.get("lastPrice")
        if price and float(price) > 0:
            logger.info("[T2] %-20s ← NSE official = %.4f", symbol, float(price))
            return round(float(price), 4)
    except ImportError:
        pass
    except Exception as e:
        logger.debug("[T2] %s nsepython: %s", symbol, e)

    # Direct NSE API fallback
    try:
        sess = requests.Session()
        sess.get("https://www.nseindia.com", headers=_NSE_HEADERS, timeout=4)
        resp = sess.get(_NSE_QUOTE_URL.format(sym=symbol.upper()), headers=_NSE_HEADERS, timeout=5)
        if resp.status_code == 200:
            price = resp.json().get("priceInfo", {}).get("lastPrice")
            if price and float(price) > 0:
                return round(float(price), 4)
    except Exception as e:
        logger.debug("[T2] %s NSE direct: %s", symbol, e)
    return None


async def _tier2_async(symbol: str, exchange: str) -> Optional[float]:
    if not _is_nse(exchange):
        return None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _nse_sync, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3 — Google Finance scrape
# ─────────────────────────────────────────────────────────────────────────────

_GOOGLE_URL  = "https://www.google.com/finance/quote/{sym}:{ex}"
_PRICE_RE    = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
_PRICE_RE_FB = re.compile(r'class="YMlKec fxKbKc"[^>]*>([0-9,]+\.?[0-9]*)<')


def _google_sync(symbol: str, exchange: str) -> Optional[float]:
    gex = "NSE" if _is_nse(exchange) else exchange.upper()
    url = _GOOGLE_URL.format(sym=symbol.upper(), ex=gex)
    try:
        resp = requests.get(url, headers=_ua_headers(), timeout=6)
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
        logger.debug("[T3] %s Google: %r", symbol, e)
    return None


async def _tier3_async(symbol: str, exchange: str) -> Optional[float]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _google_sync, symbol, exchange)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 4 — yfinance batch
# ─────────────────────────────────────────────────────────────────────────────

def _yfinance_batch_sync(symbols: List[str], exchanges: List[str]) -> Dict[str, float]:
    """Single yf.download() for all remaining symbols. Runs in _yf_executor."""
    import yfinance as yf

    result     : Dict[str, float] = {}
    yf_tickers  = [_resolve(s, e) for s, e in zip(symbols, exchanges)]
    rev_map     = {_norm(t): s for s, t in zip(symbols, yf_tickers)}
    sess        = requests.Session()
    sess.headers.update({**_BASE_HEADERS, "User-Agent": random.choice(_UA_POOL)})

    try:
        df = yf.download(
            tickers=(" ".join(yf_tickers)), period="1d", interval="1m",
            group_by="ticker", auto_adjust=True, progress=False,
            threads=False, session=sess,
        )
        if df is not None and not df.empty:
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
                cols = [str(c).lower() for c in df.columns]
                if "close" in cols and len(symbols) == 1:
                    s = df[df.columns[cols.index("close")]].dropna()
                    if not s.empty:
                        result[symbols[0]] = round(float(s.iloc[-1]), 4)
    except Exception as e:
        logger.warning("[T4] yf.download %s: %s", type(e).__name__, e)

    for sym in [s for s in symbols if s not in result]:
        ex      = exchanges[symbols.index(sym)]
        yf_tick = _resolve(sym, ex)
        try:
            hist = yf.Ticker(yf_tick, session=sess).history(period="1d", auto_adjust=True)
            if not hist.empty:
                hist.columns = [str(c).split(",")[0].strip("() '\"").lower() for c in hist.columns]
                if "close" in hist.columns:
                    series = hist["close"].dropna()
                    if not series.empty:
                        result[sym] = round(float(series.iloc[-1]), 4)
        except Exception as e:
            logger.debug("[T4] %s per-sym: %s", sym, e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol race orchestrator  (T1-preferred with headstart)
# ─────────────────────────────────────────────────────────────────────────────

async def _race_one_async(symbol: str, exchange: str) -> LTPResult:
    """
    T1-PREFERRED RACE:
      1. Start Finnhub (T1).
      2. Wait up to T1_HEADSTART_MS for it to return a valid price.
         If it wins in the headstart window → return immediately (Finnhub).
      3. After the headstart, start NSE (T2) concurrently.
         Whichever of T1/T2 returns a valid non-zero price first wins.
      4. If both T1+T2 silent/zero → T3 Google Finance.
      5. Persistent failures → sentinel "__pending_yf__" (T4 batch).
    """
    headstart_s = T1_HEADSTART_MS / 1000.0

    # ── Step 1: T1 headstart window ───────────────────────────────────────
    t1_task = asyncio.create_task(_tier1_async(symbol, exchange))

    done, pending = await asyncio.wait(
        {t1_task},
        timeout=headstart_s,
    )

    if done:
        try:
            price = t1_task.result()
            if price and price > 0:
                logger.info("[RACE] %-20s ← Finnhub (headstart) %.4f", symbol, price)
                return {"price": price, "source": "Finnhub"}
        except Exception:
            pass

    # ── Step 2: T1 still running or returned 0 — start T2 concurrently ───
    t2_task = asyncio.create_task(_tier2_async(symbol, exchange))

    # If T1 is still pending, race T1 vs T2 with remaining budget
    remaining_tasks = {t for t in (t1_task, t2_task) if not t.done()}
    race_budget_s   = 5.0  # total budget from this point

    while remaining_tasks:
        done2, remaining_tasks = await asyncio.wait(
            remaining_tasks,
            timeout=race_budget_s,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done2:
            try:
                price = task.result()
                if price and price > 0:
                    # Determine source accurately by task identity
                    src = "Finnhub" if task is t1_task else "NSE"
                    # Cancel the loser
                    for t in remaining_tasks:
                        t.cancel()
                    logger.info("[RACE] %-20s ← %-10s %.4f", symbol, src, price)
                    return {"price": price, "source": src}
            except Exception:
                pass
        # All done tasks returned 0/None — check if anything left
        if not remaining_tasks:
            break

    # Cancel any lingering tasks
    for t in (t1_task, t2_task):
        if not t.done():
            t.cancel()

    # ── Step 3: T3 Google Finance ─────────────────────────────────────────
    try:
        price = await asyncio.wait_for(_tier3_async(symbol, exchange), timeout=6.0)
        if price and price > 0:
            logger.info("[RACE] %-20s ← Google     %.4f", symbol, price)
            return {"price": price, "source": "Google"}
    except (asyncio.TimeoutError, Exception) as e:
        logger.debug("[T3] %s: %s", symbol, e)

    # T4 handled at batch level
    return {"price": 0.0, "source": "__pending_yf__"}


# ─────────────────────────────────────────────────────────────────────────────
# Public async API
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_ltp_batch_async(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    """
    Entry point for FastAPI routes.  Never blocks the uvicorn event loop.
    Returns Dict[str, LTPResult] — every symbol always has an entry.
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm_syms = [s.strip().upper() for s in symbols]
    norm_exs  = [e.strip().upper() for e in exchanges]

    logger.info("[LTP] Racing %d symbols (T1 headstart=%dms): %s",
                len(norm_syms), T1_HEADSTART_MS, norm_syms)

    race_results: List[LTPResult] = await asyncio.gather(
        *[_race_one_async(sym, ex) for sym, ex in zip(norm_syms, norm_exs)],
        return_exceptions=False,
    )

    result:        Dict[str, LTPResult] = {}
    yf_needed_syms: List[str]           = []
    yf_needed_exs:  List[str]           = []

    for sym, res in zip(norm_syms, race_results):
        if res["source"] == "__pending_yf__":
            yf_needed_syms.append(sym)
            yf_needed_exs.append(norm_exs[norm_syms.index(sym)])
        else:
            result[sym] = res

    if yf_needed_syms:
        logger.info("[T4] yfinance batch for %d: %s", len(yf_needed_syms), yf_needed_syms)
        loop = asyncio.get_event_loop()
        try:
            yf_prices = await asyncio.wait_for(
                loop.run_in_executor(_yf_executor, _yfinance_batch_sync,
                                     yf_needed_syms, yf_needed_exs),
                timeout=8.0,
            )
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning("[T4] batch failed: %s", e)
            yf_prices = {}

        for sym in yf_needed_syms:
            price = yf_prices.get(sym, 0.0)
            if price and price > 0:
                result[sym] = {"price": price, "source": "yfinance"}
                logger.info("[T4] %-20s ← yfinance %.4f", sym, price)
            else:
                result[sym] = _FAILED
                logger.warning("[MISS] %s all tiers exhausted", sym)

    for sym in norm_syms:
        if sym not in result:
            result[sym] = _FAILED

    resolved = sum(1 for v in result.values() if v["price"] > 0)
    logger.info("[LTP] Done %d/%d  sources=%s",
                resolved, len(norm_syms),
                {s: result[s]["source"] for s in norm_syms})
    return result


# ── Sync shim ─────────────────────────────────────────────────────────────────

def fetch_ltp_batch(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    """Sync shim for scripts only. FastAPI routes use fetch_ltp_batch_async."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("fetch_ltp_batch() inside running loop — use async version.")
            return {s: _FAILED for s in symbols}
        return loop.run_until_complete(fetch_ltp_batch_async(symbols, exchanges))
    except Exception as e:
        logger.error("fetch_ltp_batch shim: %s", e)
        return {s: _FAILED for s in (symbols or [])}


# ── Health check ──────────────────────────────────────────────────────────────

def health_check(
    symbols:   Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    test_db:   bool                = True,
) -> None:
    """
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
            conn = _aio.get_event_loop().run_until_complete(asyncpg.connect(raw))
            ver  = _aio.get_event_loop().run_until_complete(conn.fetchval("SELECT version()"))
            _aio.get_event_loop().run_until_complete(conn.close())
            print(f"  ✓ {str(ver)[:70]}")
        except Exception as e:
            print(f"  ✗ DB FAILED: {e}")

    if symbols is None:
        symbols   = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "WIPRO"]
    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm = [s.upper() for s in symbols]
    print(f"\n{SEP}\n  [CHECK 2] RACING LTP — T1_HEADSTART={T1_HEADSTART_MS}ms\n  Symbols: {norm}\n{SEP}")

    result = fetch_ltp_batch(norm, exchanges)
    print(f"\n  {'SYMBOL':<15}  {'PRICE':>10}  {'SOURCE':<12}  KEY-OK")
    print(f"  {'-' * 55}")
    all_ok = True
    for s in norm:
        r   = result.get(s, _FAILED)
        ok  = "✓" if r["price"] > 0 else "✗"
        key = "✓" if s in result else "✗ MISSING"
        print(f"  {ok} {s:<14}  {r['price']:>10.4f}  {r['source']:<12}  {key}")
        if not r["price"]:
            all_ok = False

    print(f"\n{SEP}")
    print(f"  RESULT: {'PASS' if all_ok else 'PARTIAL — some symbols at 0.0'}\n")
