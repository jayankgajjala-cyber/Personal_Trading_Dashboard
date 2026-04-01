"""
market_data.py — Threaded LTP fetcher with strict 8-second global timeout.
Uses yf.Ticker.fast_info per symbol in parallel via ThreadPoolExecutor.
All print() calls surface directly in Railway / Uvicorn stdout.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

BATCH_TIMEOUT_SECONDS = 8.0


def _resolve(symbol: str, exchange: str = "NSE") -> str:
    s = symbol.strip().upper()
    if "." in s:
        return s
    ex = exchange.strip().upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s
    return f"{s}.NS"


def _fetch_one(yf_ticker: str, original: str) -> tuple[str, Optional[float]]:
    """
    Fetch LTP for a single ticker.
    Returns (original_symbol, price_or_None).
    Order: fast_info attributes → history() tail.
    """
    try:
        import yfinance as yf

        t = yf.Ticker(yf_ticker)
        fi = t.fast_info

        # Attempt 1: fast_info — attribute names vary across yfinance versions
        for attr in ("last_price", "regularMarketPrice", "previous_close"):
            val = getattr(fi, attr, None)
            if val is not None:
                try:
                    fval = float(val)
                    if fval > 0:
                        price = round(fval, 4)
                        print(f"[market_data] {yf_ticker} → {attr} = {price}")
                        return (original, price)
                except (TypeError, ValueError):
                    pass

        # Attempt 2: 1-day 1-minute history tail
        print(f"[market_data] {yf_ticker} fast_info gave no valid price — trying history()")
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
                    print(f"[market_data] {yf_ticker} → history tail = {price}")
                    return (original, price)

        print(f"[market_data] {yf_ticker} → no price obtained from any source")
        return (original, None)

    except Exception as e:
        print(f"[market_data] {yf_ticker} → exception: {e}")
        return (original, None)


def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Fetch LTP for all symbols in parallel threads.
    Hard cap: BATCH_TIMEOUT_SECONDS — API call never hangs indefinitely.
    Returns {original_symbol: float | None} — keys always match exact DB symbols.
    """
    if not symbols:
        print("[market_data] fetch_ltp_batch called with empty list.")
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    # forward : original_symbol   → yf_ticker
    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }
    # reverse : yf_ticker.UPPER() → original_symbol
    reverse_map: Dict[str, str] = {v.upper(): k for k, v in ticker_map.items()}

    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))

    print(f"[market_data] Batch start — {len(unique_tickers)} tickers: {unique_tickers}")
    print(f"[market_data] Reverse map: {reverse_map}")

    futures: Dict = {}
    with ThreadPoolExecutor(max_workers=min(len(unique_tickers), 10)) as executor:
        for yf_ticker in unique_tickers:
            original = reverse_map.get(yf_ticker.upper())
            if not original:
                print(f"[market_data] WARNING: no reverse_map entry for '{yf_ticker}'")
                continue
            future = executor.submit(_fetch_one, yf_ticker, original)
            futures[future] = yf_ticker

        try:
            for future in as_completed(futures, timeout=BATCH_TIMEOUT_SECONDS):
                yf_ticker = futures[future]
                try:
                    original, price = future.result()
                    result[original] = price
                except Exception as e:
                    print(f"[market_data] Future result error for {yf_ticker}: {e}")

        except TimeoutError:
            print(
                f"[market_data] TIMEOUT hit after {BATCH_TIMEOUT_SECONDS}s — "
                "returning partial results. Tickers not yet resolved stay None."
            )

    fetched = sum(1 for v in result.values() if v is not None)
    print(f"[market_data] Batch done — {fetched}/{len(symbols)} resolved: {result}")
    return result
