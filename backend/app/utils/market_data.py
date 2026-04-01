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
from typing import Dict, List, Optional

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
    return f"{s}.NS"


def fetch_ltp_batch(
    symbols: List[str],
    exchanges: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Fetch last traded price for all symbols in one yfinance call.
    Returns {original_symbol: float | None} — keys are always the
    original symbols passed in, never the resolved .NS/.BO tickers.
    """
    if not symbols:
        return {}

    if exchanges is None:
        exchanges = ["NSE"] * len(symbols)

    # Build forward map: original_symbol → yf_ticker
    ticker_map: Dict[str, str] = {
        sym: _resolve(sym, ex)
        for sym, ex in zip(symbols, exchanges)
    }

    # Build reverse map: yf_ticker (upper) → original_symbol
    # Upper-cased to survive any case mutations yfinance applies to column names.
    reverse_map: Dict[str, str] = {
        v.upper(): k for k, v in ticker_map.items()
    }

    # Initialise all symbols to None
    result: Dict[str, Optional[float]] = {sym: None for sym in symbols}

    # Deduplicated list of tickers to request
    unique_tickers: List[str] = list(dict.fromkeys(ticker_map.values()))

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
            if "Close" not in data.columns.get_level_values(0):
                logger.warning("'Close' field not in MultiIndex columns")
                return result

            close_df = data["Close"]  # DataFrame: rows=time, cols=ticker

            # Normalise column names to upper-case for safe lookup
            close_df.columns = [str(c).upper() for c in close_df.columns]

            for ticker in unique_tickers:
                ticker_upper = ticker.upper()
                original = reverse_map.get(ticker_upper)
                if original is None:
                    continue
                if ticker_upper not in close_df.columns:
                    logger.debug(
                        "Ticker %s not in close_df columns: %s",
                        ticker_upper, list(close_df.columns)
                    )
                    continue
                series = close_df[ticker_upper].dropna()
                if series.empty:
                    logger.debug("Empty close series for %s", ticker_upper)
                    continue
                result[original] = round(float(series.iloc[-1]), 4)

        else:
            # Single-ticker: flat DataFrame with a 'Close' column
            if "Close" not in data.columns:
                logger.warning("'Close' not in single-ticker columns: %s", list(data.columns))
                return result

            series = data["Close"].dropna()
            if series.empty:
                return result

            ltp = round(float(series.iloc[-1]), 4)
            # Assign to every original symbol that resolved to this ticker
            for ticker in unique_tickers:
                original = reverse_map.get(ticker.upper())
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