"""
market_data.py — yfinance LTP fetcher using yf.Tickers (more reliable than yf.download).
Logs are print()-based so they appear directly in Railway / Uvicorn stdout.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _resolve(symbol: str, exchange: str = "NSE") -> str:
    """Map a DB symbol + exchange to a yfinance ticker string."""
    s = symbol.strip().upper()
    if "." in s:
        return s                      # already qualified (.NS / .BO / .L)
    ex = exchange.strip().upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s                      # raw: AAPL, BTC-USD
    return f"{s}.NS"                  # default → NSE


def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Fetch last traded price for every symbol using yf.Tickers.

    Returns
    -------
    {original_symbol: float | None}
    Keys are always the original DB symbols — never the .NS/.BO suffixed tickers.
    """
    if not symbols:
        print("[market_data] fetch_ltp_batch called with empty symbol list.")
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    # forward  : original_symbol   → yf_ticker
    # reverse  : yf_ticker.UPPER() → original_symbol
    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }
    reverse_map: Dict[str, str] = {
        v.upper(): k for k, v in ticker_map.items()
    }

    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))

    print(f"[market_data] Symbols requested  : {symbols}")
    print(f"[market_data] Resolved yf tickers: {unique_tickers}")
    print(f"[market_data] Reverse map        : {reverse_map}")

    try:
        import yfinance as yf

        # ── Strategy 1: yf.Tickers batch via fast_info ────────────────────
        print(f"[market_data] Calling yf.Tickers for: {unique_tickers}")
        tickers_obj = yf.Tickers(" ".join(unique_tickers))

        for yf_ticker in unique_tickers:
            original = reverse_map.get(yf_ticker.upper())
            if not original:
                print(f"[market_data] WARNING: no reverse_map entry for '{yf_ticker}'")
                continue

            try:
                t = tickers_obj.tickers.get(yf_ticker)
                if t is None:
                    print(f"[market_data] yf.Tickers returned None for '{yf_ticker}'")
                    raise ValueError("Ticker object None")

                fi = t.fast_info
                # Log the full fast_info dict so we can see what fields are available
                try:
                    fi_dict = {k: getattr(fi, k, None) for k in dir(fi) if not k.startswith("_")}
                    print(f"[market_data] fast_info fields for {yf_ticker}: {fi_dict}")
                except Exception:
                    print(f"[market_data] fast_info introspection failed for {yf_ticker}")

                # Attribute name differs across yfinance patch versions — try all known names
                price = (
                    getattr(fi, "last_price",         None) or
                    getattr(fi, "regularMarketPrice", None) or
                    getattr(fi, "previous_close",     None)
                )
                print(f"[market_data] fast_info price for {yf_ticker}: {price}")

                if price is not None and float(price) > 0:
                    result[original] = round(float(price), 4)
                else:
                    raise ValueError(f"fast_info price invalid: {price}")

            except Exception as fi_err:
                print(f"[market_data] fast_info failed for '{yf_ticker}': {fi_err} — trying history()")

                # ── Strategy 2: 1-day 1-minute history per ticker ─────────
                try:
                    t_single = yf.Ticker(yf_ticker)
                    hist = t_single.history(period="1d", interval="1m", auto_adjust=True)
                    print(f"[market_data] history() shape={hist.shape} cols={list(hist.columns)} for {yf_ticker}")

                    if not hist.empty:
                        # Match 'Close' column case-insensitively (handles tuple cols in newer yf)
                        close_col = next(
                            (c for c in hist.columns
                             if str(c).lower().strip("()' ") == "close"),
                            None
                        )
                        if close_col is not None:
                            series = hist[close_col].dropna()
                            if not series.empty:
                                price = round(float(series.iloc[-1]), 4)
                                print(f"[market_data] history() LTP for {yf_ticker}: {price}")
                                result[original] = price
                            else:
                                print(f"[market_data] history() Close series empty for {yf_ticker}")
                        else:
                            print(f"[market_data] No Close col in history for {yf_ticker}. cols={list(hist.columns)}")
                    else:
                        print(f"[market_data] history() empty DataFrame for {yf_ticker}")

                except Exception as hist_err:
                    print(f"[market_data] history() also failed for '{yf_ticker}': {hist_err}")

    except ImportError:
        print("[market_data] ERROR: yfinance not installed. Run: pip install yfinance")
        logger.error("yfinance not installed")
    except Exception as e:
        print(f"[market_data] FATAL fetch error: {e}")
        logger.error("yfinance fetch error: %s", e, exc_info=True)

    fetched = sum(1 for v in result.values() if v is not None)
    print(f"[market_data] Final result — {fetched}/{len(symbols)} resolved: {result}")
    return result
