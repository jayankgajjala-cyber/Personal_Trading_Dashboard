"""
market_data.py  —  High-Speed Google-Primary LTP Pipeline
==========================================================

RACING ARCHITECTURE  (target < 2 s for 10-symbol portfolio)
  fetch_ltp_batch_async()
    └─ asyncio.gather over all symbols simultaneously
         └─ _race_one_async(symbol, exchange)
              ├─ T1 Google Finance  aiohttp async GET  ~200-650 ms
              ├─ T2 NSE Official    run_in_executor    ~400-900 ms
              │    Both launched concurrently; first valid non-zero wins
              └─ T3 yfinance        single batch download for T1+T2 misses

RETURN TYPE
  Dict[str, LTPResult]
  LTPResult = {"price": float, "source": str}
  source ∈ {"Google", "NSE", "yfinance", "Failed"}

PERFORMANCE
  • aiohttp for Google — true async, no thread needed
  • All symbols raced in parallel via asyncio.gather
  • yfinance called once as a batch (run_in_executor) for stragglers
  • No time.sleep() in the async path
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional
from typing_extensions import TypedDict

import requests

logger = logging.getLogger(__name__)

# ── Types ─────────────────────────────────────────────────────────────────────

class LTPResult(TypedDict):
    price:  float
    source: str


_FAILED: LTPResult = {"price": 0.0, "source": "Failed"}

# ── Thread pool (yfinance only — it's blocking) ───────────────────────────────

_yf_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="yf_batch")

# ── UA pool (rotated per request) ─────────────────────────────────────────────

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

_BASE_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache",
}


def _ua_headers() -> Dict[str, str]:
    return {**_BASE_HEADERS, "User-Agent": random.choice(_UA_POOL)}


# ── Symbol helpers ────────────────────────────────────────────────────────────

def _resolve(symbol: str, exchange: str = "NSE") -> str:
    s  = symbol.strip().upper()
    if "." in s:
        return s
    ex = exchange.strip().upper()
    if ex == "BSE":            return f"{s}.BO"
    if ex in ("US", "CRYPTO"): return s
    return f"{s}.NS"


def _norm(ticker: str) -> str:
    return ticker.split(".")[0].upper()


def _is_nse(exchange: str) -> bool:
    return exchange.strip().upper() in ("NSE", "")


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — Google Finance  (true async aiohttp)
# ─────────────────────────────────────────────────────────────────────────────

_GOOGLE_URL  = "https://www.google.com/finance/quote/{sym}:{ex}"
_PRICE_RE    = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
_PRICE_RE_FB = re.compile(r'class="YMlKec fxKbKc"[^>]*>([0-9,]+\.?[0-9]*)<')


async def _tier1_google_async(symbol: str, exchange: str) -> Optional[float]:
    """
    Pure async aiohttp GET to Google Finance.
    No thread pool needed — fully non-blocking.
    """
    try:
        import aiohttp  # type: ignore
    except ImportError:
        # aiohttp not installed — fall back to sync via executor
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _google_sync_fallback, symbol, exchange)

    gex = "NSE" if _is_nse(exchange) else exchange.upper()
    url = _GOOGLE_URL.format(sym=symbol.upper(), ex=gex)

    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(
            headers=_ua_headers(),
            timeout=timeout,
            connector=aiohttp.TCPConnector(ssl=False),
        ) as session:
            async with session.get(url) as resp:
                if resp.status in (429, 403):
                    logger.debug("[T1] %s Google HTTP %s — silent", symbol, resp.status)
                    return None
                if resp.status != 200:
                    return None
                text = await resp.text()
                return _parse_google_price(text, symbol)
    except asyncio.TimeoutError:
        logger.debug("[T1] %s Google timeout", symbol)
        return None
    except Exception as e:
        logger.debug("[T1] %s Google aiohttp error: %r", symbol, e)
        return None


def _google_sync_fallback(symbol: str, exchange: str) -> Optional[float]:
    """requests-based fallback when aiohttp unavailable."""
    gex = "NSE" if _is_nse(exchange) else exchange.upper()
    url = _GOOGLE_URL.format(sym=symbol.upper(), ex=gex)
    try:
        resp = requests.get(url, headers=_ua_headers(), timeout=6)
        if resp.status_code in (429, 403, 401):
            return None
        if resp.status_code != 200:
            return None
        return _parse_google_price(resp.text, symbol)
    except Exception as e:
        logger.debug("[T1] %s Google sync error: %r", symbol, e)
        return None


def _parse_google_price(html: str, symbol: str) -> Optional[float]:
    m = _PRICE_RE.search(html)
    if m:
        return round(float(m.group(1)), 4)
    m = _PRICE_RE_FB.search(html)
    if m:
        return round(float(m.group(1).replace(",", "")), 4)
    try:
        from bs4 import BeautifulSoup  # type: ignore
        el = BeautifulSoup(html, "html.parser").find("div", {"class": "YMlKec fxKbKc"})
        if el:
            return round(float(el.get_text().strip().replace(",", "")), 4)
    except Exception:
        pass
    logger.debug("[T1] %s Google: no price pattern found", symbol)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — NSE Official  (sync in executor)
# ─────────────────────────────────────────────────────────────────────────────

_NSE_HEADERS = {
    **_BASE_HEADERS,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.nseindia.com/",
}


def _nse_sync(symbol: str) -> Optional[float]:
    # nsepython preferred
    try:
        from nsepython import nse_eq  # type: ignore
        data  = nse_eq(symbol.upper())
        price = data.get("priceInfo", {}).get("lastPrice") or data.get("lastPrice")
        if price and float(price) > 0:
            logger.info("[T2] %-20s ← NSE = %.4f", symbol, float(price))
            return round(float(price), 4)
    except ImportError:
        pass
    except Exception as e:
        logger.debug("[T2] %s nsepython: %s", symbol, e)

    # Direct NSE API
    try:
        sess = requests.Session()
        sess.get("https://www.nseindia.com", headers=_NSE_HEADERS, timeout=4)
        resp = sess.get(
            f"https://www.nseindia.com/api/quote-equity?symbol={symbol.upper()}",
            headers=_NSE_HEADERS, timeout=5,
        )
        if resp.status_code == 200:
            price = resp.json().get("priceInfo", {}).get("lastPrice")
            if price and float(price) > 0:
                return round(float(price), 4)
    except Exception as e:
        logger.debug("[T2] %s NSE direct: %s", symbol, e)
    return None


async def _tier2_nse_async(symbol: str, exchange: str) -> Optional[float]:
    if not _is_nse(exchange):
        return None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _nse_sync, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3 — yfinance batch  (all remaining symbols, single download call)
# ─────────────────────────────────────────────────────────────────────────────

def _yfinance_batch_sync(symbols: List[str], exchanges: List[str]) -> Dict[str, float]:
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
        if "429" not in str(e) and "403" not in str(e):
            logger.warning("[T3] yf.download %s: %s", type(e).__name__, e)

    # per-symbol fallback
    for sym in [s for s in symbols if s not in result]:
        ex = exchanges[symbols.index(sym)]
        try:
            hist = yf.Ticker(_resolve(sym, ex), session=sess).history(period="1d", auto_adjust=True)
            if not hist.empty:
                hist.columns = [str(c).split(",")[0].strip("() '\"").lower() for c in hist.columns]
                if "close" in hist.columns:
                    series = hist["close"].dropna()
                    if not series.empty:
                        result[sym] = round(float(series.iloc[-1]), 4)
        except Exception as e:
            if "429" not in str(e) and "403" not in str(e):
                logger.debug("[T3] %s per-sym: %s", sym, e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol concurrent race (T1 ‖ T2)
# ─────────────────────────────────────────────────────────────────────────────

async def _race_one_async(symbol: str, exchange: str) -> LTPResult:
    """
    Launch T1 (Google aiohttp) and T2 (NSE executor) concurrently.
    First valid non-zero price wins. T3 (yfinance) handled at batch level.
    """
    t1 = asyncio.create_task(_tier1_google_async(symbol, exchange))
    t2 = asyncio.create_task(_tier2_nse_async(symbol, exchange))

    pending = {t1, t2}
    while pending:
        done, pending = await asyncio.wait(
            pending,
            timeout=7.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            break
        for task in done:
            try:
                price = task.result()
                if price and price > 0:
                    for p in pending:
                        p.cancel()
                    src = "Google" if task is t1 else "NSE"
                    logger.info("[RACE] %-20s ← %-8s %.4f", symbol, src, price)
                    return {"price": price, "source": src}
            except Exception:
                pass

    for t in (t1, t2):
        if not t.done():
            t.cancel()

    return {"price": 0.0, "source": "__pending_yf__"}


# ─────────────────────────────────────────────────────────────────────────────
# Public async API
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_ltp_batch_async(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    """
    FastAPI entry point. All I/O non-blocking.
    Returns {symbol: {"price": float, "source": str}} for every input symbol.
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm_syms = [s.strip().upper() for s in symbols]
    norm_exs  = [e.strip().upper() for e in exchanges]

    logger.info("[LTP] Racing %d symbols: %s", len(norm_syms), norm_syms)

    # All symbols raced in parallel
    race_results: List[LTPResult] = await asyncio.gather(
        *[_race_one_async(s, e) for s, e in zip(norm_syms, norm_exs)],
        return_exceptions=False,
    )

    result:        Dict[str, LTPResult] = {}
    yf_syms:       List[str]            = []
    yf_exs:        List[str]            = []

    for sym, res in zip(norm_syms, race_results):
        if res["source"] == "__pending_yf__":
            yf_syms.append(sym)
            yf_exs.append(norm_exs[norm_syms.index(sym)])
        else:
            result[sym] = res

    # Single yfinance batch for all T1+T2 misses
    if yf_syms:
        logger.info("[T3] yfinance batch: %s", yf_syms)
        loop = asyncio.get_event_loop()
        try:
            yf_prices = await asyncio.wait_for(
                loop.run_in_executor(_yf_executor, _yfinance_batch_sync, yf_syms, yf_exs),
                timeout=10.0,
            )
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning("[T3] yfinance batch: %s", e)
            yf_prices = {}

        for sym in yf_syms:
            p = yf_prices.get(sym, 0.0)
            result[sym] = {"price": p, "source": "yfinance"} if p > 0 else _FAILED

    for sym in norm_syms:
        if sym not in result:
            result[sym] = _FAILED

    resolved = sum(1 for v in result.values() if v["price"] > 0)
    logger.info("[LTP] Done %d/%d  %s",
                resolved, len(norm_syms),
                {s: result[s]["source"] for s in norm_syms})
    return result


# ── Sync shim ─────────────────────────────────────────────────────────────────

def fetch_ltp_batch(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, LTPResult]:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return {s: _FAILED for s in symbols}
        return loop.run_until_complete(fetch_ltp_batch_async(symbols, exchanges))
    except Exception as e:
        logger.error("fetch_ltp_batch: %s", e)
        return {s: _FAILED for s in (symbols or [])}
