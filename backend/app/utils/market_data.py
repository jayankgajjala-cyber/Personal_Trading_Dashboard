"""
market_data.py — yfinance batch LTP fetcher.

yfinance 0.2.x DataFrame structure:
  - Single ticker:  flat columns → ['Open', 'High', 'Low', 'Close', 'Volume']
  - Multi ticker:   MultiIndex  → ('Close', 'RELIANCE.NS'), ('Close', 'TCS.NS'), ...
                    i.e. level-0 = field, level-1 = ticker

Symbol resolution:
  - Already has dot suffix (.NS / .BO / .L)  → used as-is
  - Pure alpha ≤ 10 chars                    → append .NS  (NSE default)
  - exchange == "BSE"                         → append .BO
  - exchange == "US" or has digit/dash        → raw  (AAPL, BTC-USD)
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _resolve(symbol: str, exchange: str = "NSE") -> str:
    s = symbol.strip().upper()
    if "." in s:
        return s
    ex = exchange.upper()
    if ex == "BSE":
        return f"{s}.BO"
    if ex in ("US", "CRYPTO"):
        return s
    # Default → NSE
    return f"{s}.NS"


def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Fetch last close price for all symbols in one yfinance call.
    Returns {original_symbol: float | None}
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    # Build forward and reverse maps
    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }
    # reverse: yf_ticker → original symbol
    # If two symbols resolve to same ticker, last wins — acceptable for single-user portfolio
    reverse_map: Dict[str, str] = {v: k for k, v in ticker_map.items()}

    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))  # preserve order, dedup

    try:
        import yfinance as yf

        data = yf.download(
            tickers=" ".join(unique_tickers),
            period="1d",
            interval="1m",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if data is None or data.empty:
            logger.warning("yfinance returned empty DataFrame for tickers: %s", unique_tickers)
            return result

        is_multi = hasattr(data.columns, "levels")  # MultiIndex when >1 ticker

        if is_multi:
            # MultiIndex: level-0 = field ('Close'), level-1 = ticker
            if "Close" not in data.columns.get_level_values(0):
                logger.warning("'Close' field not in MultiIndex columns")
                return result

            close_df = data["Close"]  # DataFrame: rows=time, cols=ticker

            for ticker in unique_tickers:
                original = reverse_map.get(ticker)
                if original is None:
                    continue
                if ticker not in close_df.columns:
                    logger.debug("Ticker %s not in close_df columns: %s", ticker, list(close_df.columns))
                    continue
                series = close_df[ticker].dropna()
                if series.empty:
                    logger.debug("Empty close series for %s", ticker)
                    continue
                result[original] = round(float(series.iloc[-1]), 4)

        else:
            # Single ticker: flat DataFrame with 'Close' column
            if "Close" not in data.columns:
                logger.warning("'Close' not in single-ticker columns: %s", list(data.columns))
                return result

            series = data["Close"].dropna()
            if series.empty:
                return result

            ltp = round(float(series.iloc[-1]), 4)
            # Assign to all symbols that resolved to this ticker
            for ticker in unique_tickers:
                original = reverse_map.get(ticker)
                if original:
                    result[original] = ltp

    except ImportError:
        logger.error("yfinance is not installed. Run: pip install yfinance")
    except Exception as e:
        logger.error("yfinance fetch error: %s", e, exc_info=True)

    logger.info(
        "LTP fetch complete. Got prices for %d/%d symbols.",
        sum(1 for v in result.values() if v is not None),
        len(symbols),
    )
    return result
