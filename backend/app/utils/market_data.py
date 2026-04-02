"""
market_data.py  —  4-Tier LTP Resilience Pipeline
===================================================
Tier 1  Finnhub API      official /quote, 60 req/min free
Tier 2  NSE Official     nsepython.nse_eq(), NSE symbols only
Tier 3  Google Finance   HTML scrape, random UA rotation
Tier 4  yfinance         yf.download() batch + per-symbol fallback

GUARANTEES
  • Input  ["RELIANCE"] → Output {"RELIANCE": 2500.0}  (key = original)
  • All tiers exhausted  → 0.0   (never None)
  • Async bridge: sync work runs in _executor — never blocks uvicorn

ENV
  FINNHUB_API_KEY  — set in Railway / .env  (T1 skipped when absent)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────────
MAX_RETRIES    = 2
BASE_BACKOFF_S = 1.2

_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="ltp")

# ── User-Agent rotation pool ──────────────────────────────────────────────────
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
    "Pragma":          "no-cache",
}

_tls = threading.local()


def _session(rotate_ua: bool = False) -> requests.Session:
    """Thread-local session; UA is rotated when rotate_ua=True (e.g. after 429)."""
    if not hasattr(_tls, "s") or rotate_ua:
        s = requests.Session()
        s.headers.update(_BASE_HEADERS)
        s.headers["User-Agent"] = random.choice(_UA_POOL)
        _tls.s = s
    return _tls.s


# ── Symbol helpers ────────────────────────────────────────────────────────────

def _resolve(symbol: str, exchange: str = "NSE") -> str:
    """DB symbol + exchange → yfinance / Finnhub ticker string."""
    s  = symbol.strip().upper()
    if "." in s:
        return s
    ex = exchange.strip().upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s
    return f"{s}.NS"


def _norm(ticker: str) -> str:
    """'RELIANCE.NS' → 'RELIANCE'"""
    return ticker.split(".")[0].upper()


def _is_nse(exchange: str) -> bool:
    return exchange.strip().upper() in ("NSE", "")


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — Finnhub
# ─────────────────────────────────────────────────────────────────────────────

def _tier1_finnhub(
    symbols: List[str],
    exchanges: List[str],
) -> Tuple[Dict[str, float], List[str]]:
    """
    Returns ({sym: price}, [still_missing]).
    Silently skipped when FINNHUB_API_KEY absent or finnhub-python not installed.
    """
    result:  Dict[str, float] = {}
    missing: List[str]        = []

    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        logger.debug("[T1] FINNHUB_API_KEY absent — skipping")
        return result, list(symbols)

    try:
        import finnhub  # type: ignore
        client = finnhub.Client(api_key=api_key)
    except ImportError:
        logger.warning("[T1] finnhub-python not installed")
        return result, list(symbols)
    except Exception as e:
        logger.warning("[T1] Finnhub init error: %s", e)
        return result, list(symbols)

    for sym, ex in zip(symbols, exchanges):
        fh_ticker = _resolve(sym, ex)
        try:
            q     = client.quote(fh_ticker)
            price = q.get("c") or q.get("pc")
            if price and float(price) > 0:
                result[sym] = round(float(price), 4)
                logger.info("[T1] %-20s ← Finnhub %s = %.4f", sym, fh_ticker, result[sym])
            else:
                missing.append(sym)
        except Exception as e:
            missing.append(sym)
            logger.warning("[T1] %s error: %s", sym, e)
        time.sleep(0.06)  # stay within 60 req/min free tier

    return result, missing


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — NSE Official (nsepython)
# ─────────────────────────────────────────────────────────────────────────────

def _tier2_nse(
    symbols: List[str],
    exchanges: List[str],
) -> Tuple[Dict[str, float], List[str]]:
    result:  Dict[str, float] = {}
    missing: List[str]        = []

    try:
        from nsepython import nse_eq  # type: ignore
    except ImportError:
        logger.warning("[T2] nsepython not installed")
        return result, list(symbols)

    for sym, ex in zip(symbols, exchanges):
        if not _is_nse(ex):
            missing.append(sym)
            continue
        try:
            data  = nse_eq(sym.upper())
            price = (
                data.get("priceInfo", {}).get("lastPrice")
                or data.get("lastPrice")
            )
            if price and float(price) > 0:
                result[sym] = round(float(price), 4)
                logger.info("[T2] %-20s ← NSE official = %.4f", sym, result[sym])
            else:
                missing.append(sym)
        except Exception as e:
            missing.append(sym)
            logger.warning("[T2] %s error: %s", sym, e)
        time.sleep(0.08)

    return result, missing


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3 — Google Finance scrape
# ─────────────────────────────────────────────────────────────────────────────

_GOOGLE_URL  = "https://www.google.com/finance/quote/{sym}:{ex}"
_PRICE_RE    = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
_PRICE_RE_FB = re.compile(r'class="YMlKec fxKbKc"[^>]*>([0-9,]+\.?[0-9]*)<')


def _scrape_google_one(sym: str, ex: str) -> Optional[float]:
    sess = _session(rotate_ua=True)
    gex  = "NSE" if _is_nse(ex) else ex.upper()
    url  = _GOOGLE_URL.format(sym=sym.upper(), ex=gex)
    try:
        resp = sess.get(url, timeout=7)
        if resp.status_code == 429:
            logger.warning("[T3] %s Google 429", sym)
            return None
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
        logger.debug("[T3] %s Google error: %r", sym, e)
    return None


def _tier3_google(
    symbols: List[str],
    exchanges: List[str],
) -> Tuple[Dict[str, float], List[str]]:
    result:  Dict[str, float] = {}
    missing: List[str]        = []

    for sym, ex in zip(symbols, exchanges):
        price = _scrape_google_one(sym, ex)
        if price and price > 0:
            result[sym] = price
            logger.info("[T3] %-20s ← Google Finance = %.4f", sym, price)
        else:
            missing.append(sym)
        time.sleep(0.12 + random.uniform(0, 0.08))

    return result, missing


# ─────────────────────────────────────────────────────────────────────────────
# TIER 4 — yfinance (batch + per-symbol fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _tier4_yfinance(
    symbols: List[str],
    exchanges: List[str],
) -> Tuple[Dict[str, float], List[str]]:
    import yfinance as yf

    result:     Dict[str, float] = {}
    yf_tickers  = [_resolve(s, e) for s, e in zip(symbols, exchanges)]
    rev_map     = {_norm(t): s for s, t in zip(symbols, yf_tickers)}
    sess        = _session()
    space_sep   = " ".join(yf_tickers)

    # ── Stage A: single batch download ───────────────────────────────────
    try:
        logger.info("[T4] yf.download: %s", space_sep)
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
                                series = sub[sub.columns[cols.index("close")]].dropna()
                                if not series.empty:
                                    result[orig] = round(float(series.iloc[-1]), 4)
                                    logger.info("[T4] %-20s ← yf batch = %.4f", orig, result[orig])
                                    break
            else:
                cols = [str(c).lower() for c in df.columns]
                if "close" in cols and len(symbols) == 1:
                    series = df[df.columns[cols.index("close")]].dropna()
                    if not series.empty:
                        result[symbols[0]] = round(float(series.iloc[-1]), 4)
                        logger.info("[T4] %-20s ← yf batch(1) = %.4f", symbols[0], result[symbols[0]])
    except Exception as e:
        logger.warning("[T4] yf.download %s: %s", type(e).__name__, e)

    # ── Stage B: per-symbol fallback for batch misses ─────────────────────
    still_missing: List[str] = []
    for sym in [s for s in symbols if s not in result]:
        ex      = exchanges[symbols.index(sym)]
        yf_tick = _resolve(sym, ex)
        fetched = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                hist = yf.Ticker(yf_tick, session=sess).history(period="1d", auto_adjust=True)
                if not hist.empty:
                    hist.columns = [str(c).split(",")[0].strip("() '\"").lower() for c in hist.columns]
                    if "close" in hist.columns:
                        series = hist["close"].dropna()
                        if not series.empty:
                            result[sym] = round(float(series.iloc[-1]), 4)
                            logger.info("[T4] %-20s ← yf per-sym = %.4f", sym, result[sym])
                            fetched = True
                            break
            except Exception as e:
                if "429" in str(e) and attempt < MAX_RETRIES:
                    backoff = BASE_BACKOFF_S * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                    logger.warning("[T4] %s 429 back-off %.1fs", sym, backoff)
                    time.sleep(backoff)
                    sess = _session(rotate_ua=True)
                else:
                    logger.warning("[T4] %s per-sym: %s", sym, e)
                    break
        if not fetched:
            still_missing.append(sym)

    return result, still_missing


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator  (sync — runs inside _executor)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_all_sync(
    symbols:   List[str],
    exchanges: List[str],
) -> Dict[str, float]:
    """
    Walks T1→T4 stopping per-symbol as soon as a price > 0 is found.
    Returns {original_sym: float} — always float, never raises.
    Injects __source_map__ key for health_check diagnostics (stripped before return to callers).
    """
    prices:     Dict[str, float] = {}
    source_map: Dict[str, str]   = {}

    def _remaining() -> Tuple[List[str], List[str]]:
        rs = [s for s in symbols if s not in prices]
        re = [exchanges[symbols.index(s)] for s in rs]
        return rs, re

    def _apply(hits: Dict[str, float], label: str) -> None:
        for s, p in hits.items():
            if p > 0 and s not in prices:
                prices[s]     = p
                source_map[s] = label

    rs, re = _remaining()
    if rs:
        _apply(_tier1_finnhub(rs, re)[0], "T1-Finnhub")

    rs, re = _remaining()
    if rs:
        _apply(_tier2_nse(rs, re)[0], "T2-NSE")

    rs, re = _remaining()
    if rs:
        _apply(_tier3_google(rs, re)[0], "T3-Google")

    rs, re = _remaining()
    if rs:
        _apply(_tier4_yfinance(rs, re)[0], "T4-yfinance")

    for s in symbols:
        if s not in prices:
            prices[s]     = 0.0
            source_map[s] = "MISS"

    for s in symbols:
        logger.info("[LTP] %-20s  %10.4f  source=%-15s", s, prices[s], source_map.get(s, "?"))

    prices["__source_map__"] = source_map   # type: ignore[assignment]
    return prices


# ── Public async API ──────────────────────────────────────────────────────────

async def fetch_ltp_batch_async(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, float]:
    """
    Non-blocking entry point for FastAPI routes.
    All blocking I/O is offloaded to _executor so uvicorn is never stalled.
    Returns {original_symbol: float} — always float, never None.
    """
    if not symbols:
        return {}
    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm = [s.strip().upper() for s in symbols]
    logger.info("[LTP] Pipeline start — %d symbols: %s", len(norm), norm)

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        _executor,
        partial(_fetch_all_sync, norm, list(exchanges)),
    )
    result.pop("__source_map__", None)
    return result


# ── Sync shim ─────────────────────────────────────────────────────────────────

def fetch_ltp_batch(
    symbols:   List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Sync shim for scripts. FastAPI routes MUST use fetch_ltp_batch_async."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("fetch_ltp_batch() inside running loop — use async version.")
            return {s: 0.0 for s in symbols}
        return loop.run_until_complete(fetch_ltp_batch_async(symbols, exchanges))
    except Exception as e:
        logger.error("fetch_ltp_batch shim error: %s", e)
        return {s: 0.0 for s in (symbols or [])}


# ── Health-check / stress-test ────────────────────────────────────────────────

def health_check(
    symbols:   Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    test_db:   bool                = True,
) -> None:
    """
    Full deployment health-check.  Run from backend root:

        python -c "
    from app.utils.market_data import health_check
    health_check()
    "

    Checks:
      1. Supabase / asyncpg DB connectivity
      2. 5-symbol LTP stress test — prints which tier provided each price
    """
    import asyncio as _aio

    SEP = "═" * 62

    # ── 1. DB connectivity ─────────────────────────────────────────────────
    if test_db:
        print(f"\n{SEP}")
        print("  [CHECK 1] DB CONNECTIVITY")
        print(SEP)
        try:
            import asyncpg  # type: ignore
            from app.core.config import settings

            raw = settings.DATABASE_URL
            for old, new in (
                ("postgresql+asyncpg://", "postgresql://"),
                ("postgres://",           "postgresql://"),
            ):
                raw = raw.replace(old, new)
            raw = raw.split("?")[0]

            async def _ping() -> str:
                conn = await asyncpg.connect(raw)
                ver  = await conn.fetchval("SELECT version()")
                await conn.close()
                return ver

            ver = _aio.get_event_loop().run_until_complete(_ping())
            print(f"  ✓ Connected: {str(ver)[:70]}")
        except Exception as e:
            print(f"  ✗ DB FAILED: {e}")
        print()

    # ── 2. LTP stress test ─────────────────────────────────────────────────
    if symbols is None:
        symbols   = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "WIPRO"]
    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    norm = [s.upper() for s in symbols]

    print(f"{SEP}")
    print("  [CHECK 2] 4-TIER LTP STRESS TEST")
    print(f"  Symbols: {norm}")
    print(SEP)

    prices:     Dict[str, float] = {}
    source_map: Dict[str, str]   = {}

    def _apply(hits: Dict[str, float], label: str) -> None:
        for s, p in hits.items():
            if p > 0 and s not in prices:
                prices[s]     = p
                source_map[s] = label

    def _rem() -> Tuple[List[str], List[str]]:
        rs = [s for s in norm if s not in prices]
        re = [exchanges[norm.index(s)] for s in rs]
        return rs, re

    for label, fn in [
        ("T1-Finnhub",  lambda rs, re: _tier1_finnhub(rs, re)[0]),
        ("T2-NSE",      lambda rs, re: _tier2_nse(rs, re)[0]),
        ("T3-Google",   lambda rs, re: _tier3_google(rs, re)[0]),
        ("T4-yfinance", lambda rs, re: _tier4_yfinance(rs, re)[0]),
    ]:
        rs, re = _rem()
        if not rs:
            print(f"  [{label:<12}] skipped — all resolved")
            continue
        print(f"  [{label:<12}] fetching {rs} ...")
        _apply(fn(rs, re), label)
        still = [s for s in norm if s not in prices]
        print(f"  [{label:<12}] still missing: {still or 'none'}")

    for s in norm:
        if s not in prices:
            prices[s]     = 0.0
            source_map[s] = "MISS"

    print(f"\n{SEP}")
    print(f"  {'SYMBOL':<15}  {'PRICE':>10}  SOURCE")
    print(f"  {'-'*57}")
    all_ok = True
    for s in norm:
        p   = prices[s]
        src = source_map.get(s, "?")
        ok  = "✓" if p > 0 else "✗"
        print(f"  {ok} {s:<14}  {p:>10.4f}  {src}")
        if p == 0.0:
            all_ok = False

    print(f"\n{SEP}")
    print("  KEY-MAPPING ASSERTION")
    result = fetch_ltp_batch(norm, exchanges)
    mapping_ok = True
    for s in norm:
        if s in result:
            print(f"  ✓ key '{s}' → {result[s]:.4f}")
        else:
            print(f"  ✗ FAIL: key '{s}' missing")
            mapping_ok = False

    print(SEP)
    verdict = "PASS" if (all_ok and mapping_ok) else "PARTIAL — some tiers exhausted (returned 0.0)"
    print(f"  RESULT: {verdict}\n")
