"""
market_data.py — yfinance batch LTP fetcher for NSE/BSE/US/CRYPTO.

yfinance 0.2.x download() column shapes
────────────────────────────────────────
Single ticker  → flat columns: ['Close', 'Open', ...]
Multi  ticker  → MultiIndex : level-0 = Price field, level-1 = Ticker symbol
                 e.g. ('Close', 'RELIANCE.NS')

The function always returns {original_symbol: ltp | None} — yf suffixes
(.NS / .BO) are stripped back via reverse_map so callers never see them.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _resolve(symbol: str, exchange: str = "NSE") -> str:
    """Map a DB symbol + exchange to a yfinance ticker string."""
    s = symbol.strip().upper()
    if "." in s:
        return s                     # already qualified (.NS / .BO / .L)
    ex = exchange.strip().upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s                     # raw: AAPL, BTC-USD
    return f"{s}.NS"                 # default → NSE


def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Fetch last traded price for every symbol in one yfinance call.

    Returns
    -------
    dict keyed by the *original* symbol strings passed in (never .NS/.BO).
    Missing / failed symbols map to None.
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    # forward  : original_symbol   → yf_ticker
    # reverse  : yf_ticker.upper() → original_symbol
    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }
    reverse_map: Dict[str, str] = {
        v.upper(): k for k, v in ticker_map.items()
    }

    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))

    try:
        import yfinance as yf

        # ── Primary: yf.download batch ────────────────────────────────────
        data = yf.download(
            tickers=" ".join(unique_tickers),
            period="1d",
            interval="1m",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if data is not None and not data.empty:
            cols = data.columns

            # MultiIndex when len(unique_tickers) > 1
            is_multi = (
                hasattr(cols, "levels") and len(cols.levels) == 2
            )

            if is_multi:
                # level-0 = field ('Close'), level-1 = ticker
                try:
                    close_df = data["Close"].copy()
                    close_df.columns = [str(c).upper() for c in close_df.columns]

                    for ticker in unique_tickers:
                        tu = ticker.upper()
                        original = reverse_map.get(tu)
                        if not original:
                            continue
                        if tu not in close_df.columns:
                            logger.debug("Ticker %s absent from close_df columns: %s",
                                         tu, list(close_df.columns))
                            continue
                        series = close_df[tu].dropna()
                        if not series.empty:
                            result[original] = round(float(series.iloc[-1]), 4)

                except KeyError:
                    logger.warning("'Close' not found in MultiIndex. columns=%s", list(cols))

            else:
                # Single-ticker: flat DataFrame — find Close column case-insensitively
                close_col = next(
                    (c for c in data.columns if str(c).lower() == "close"), None
                )
                if close_col is not None:
                    series = data[close_col].dropna()
                    if not series.empty:
                        ltp = round(float(series.iloc[-1]), 4)
                        for ticker in unique_tickers:
                            original = reverse_map.get(ticker.upper())
                            if original:
                                result[original] = ltp
                else:
                    logger.warning("'Close' column not found. columns=%s", list(data.columns))

        else:
            logger.warning("yf.download returned empty for: %s", unique_tickers)

        # ── Fallback: Ticker.fast_info for any still-None ─────────────────
        missing = [sym for sym, v in result.items() if v is None]
        if missing:
            logger.info("fast_info fallback for %d symbols: %s", len(missing), missing)
            for sym in missing:
                ticker_str = ticker_map[sym]
                try:
                    t = yf.Ticker(ticker_str)
                    fi = t.fast_info
                    # Attribute name varies by yfinance version
                    price = (
                        getattr(fi, "last_price", None)
                        or getattr(fi, "regularMarketPrice", None)
                        or getattr(fi, "previous_close", None)
                    )
                    if price is not None:
                        result[sym] = round(float(price), 4)
                except Exception as fe:
                    logger.debug("fast_info failed for %s: %s", ticker_str, fe)

    except ImportError:
        logger.error("yfinance not installed. Run: pip install yfinance")
    except Exception as e:
        logger.error("yfinance fetch error: %s", e, exc_info=True)

    fetched = sum(1 for v in result.values() if v is not None)
    logger.info("LTP fetch: %d/%d symbols resolved", fetched, len(symbols))
    return result
