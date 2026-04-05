"""
app/services/news_service.py  —  Async News Ingestion Engine
=============================================================

SECTIONS
  1. indian_market   — NSE/BSE company news via LiveMint + MoneyControl RSS
  2. global_market   — US/Global markets via GNews API + Reuters RSS
  3. macro_impact    — RBI / Fed / FII / DII policy via RSS
  4. swing_signals   — Analyst upgrades/downgrades via Moneycontrol + ET RSS

STRATEGY
  • 5-minute asyncio loop (start_news_loop)
  • aiohttp for all HTTP (non-blocking)
  • feedparser for RSS XML parsing (sync → executor)
  • GNews API for structured global news
  • Motor (async) upsert into MongoDB news_feed collection
  • Dedup via unique URL index — duplicate inserts silently ignored
  • Python 3.11 safe: typing_extensions.TypedDict throughout

SCHEMA (news_feed document)
  {
    url:          str,        # unique dedup key
    title:        str,
    summary:      str,
    source:       str,
    section:      str,        # indian_market | global_market | macro_impact | swing_signals
    published_at: datetime,
    created_at:   datetime,   # TTL base field (7-day expiry)
    tickers:      List[str],  # extracted / empty list (reserved for AI enrichment)
    image_url:    str | None,
  }
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import List, Optional

from typing_extensions import TypedDict

logger = logging.getLogger(__name__)

# ── News item schema ──────────────────────────────────────────────────────────

class NewsItem(TypedDict):
    url:          str
    title:        str
    summary:      str
    source:       str
    section:      str
    published_at: datetime
    created_at:   datetime
    tickers:      List[str]    # placeholder for future AI ticker extraction
    image_url:    Optional[str]


# ── RSS feed registry ─────────────────────────────────────────────────────────

_RSS_FEEDS: dict[str, list[tuple[str, str]]] = {
    "indian_market": [
        ("LiveMint Markets",    "https://www.livemint.com/rss/markets"),
        ("LiveMint Companies",  "https://www.livemint.com/rss/companies"),
        ("MoneyControl News",   "https://www.moneycontrol.com/rss/latestnews.xml"),
        ("ET Markets",          "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ],
    "global_market": [
        ("Reuters Business",    "https://feeds.reuters.com/reuters/businessNews"),
        ("CNBC World",          "https://www.cnbc.com/id/100727362/device/rss/rss.html"),
        ("FT Markets",          "https://www.ft.com/markets?format=rss"),
    ],
    "macro_impact": [
        ("RBI Press Releases",  "https://www.rbi.org.in/Scripts/RSSFeedsPublicDomain.aspx"),
        ("ET Economy",          "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
        ("MoneyControl Economy","https://www.moneycontrol.com/rss/economy.xml"),
        ("Reuters Fed",         "https://feeds.reuters.com/reuters/USFocusNews"),
    ],
    "swing_signals": [
        ("ET Stocks",           "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"),
        ("MC Stock Reports",    "https://www.moneycontrol.com/rss/stockreports.xml"),
        ("LiveMint IPO",        "https://www.livemint.com/rss/IPO"),
    ],
}

# GNews categories → sections
_GNEWS_QUERIES: dict[str, str] = {
    "global_market":  "stock market OR Fed Reserve OR S&P 500",
    "macro_impact":   "RBI OR Federal Reserve OR inflation OR FII DII India",
    "swing_signals":  "stock upgrade downgrade analyst India NSE",
}

# ── Ticker extractor (regex placeholder — replace with NLP/AI later) ──────────

_NSE_PATTERN = re.compile(r'\b([A-Z]{2,12})(?:\.NS|\.BO)?\b')
_KNOWN_NOISE = {"THE", "AND", "FOR", "NSE", "BSE", "RBI", "FED", "FII", "DII",
                "GDP", "CPI", "IPO", "ETF", "USD", "INR", "USA", "CEO", "CFO"}


def _extract_tickers(text: str) -> List[str]:
    """
    Regex-based NSE ticker placeholder. Returns a deduplicated list.
    Replace with spaCy / OpenAI enrichment in production.
    """
    candidates = _NSE_PATTERN.findall(text.upper())
    return list({t for t in candidates if t not in _KNOWN_NOISE and len(t) >= 3})


# ── MongoDB client (lazy singleton) ──────────────────────────────────────────

_mongo_col = None


def _get_collection():
    global _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    try:
        from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        from app.core.config import settings
        client   = AsyncIOMotorClient(settings.MONGODB_URI, serverSelectionTimeoutMS=5000)
        _mongo_col = client["quantedge"]["news_feed"]
        logger.info("[news] MongoDB connected")
    except Exception as e:
        logger.error("[news] MongoDB init failed: %s", e)
        _mongo_col = None
    return _mongo_col


# ── RSS fetcher ───────────────────────────────────────────────────────────────

def _parse_feed_sync(xml_text: str) -> list[dict]:
    """Sync feedparser call — runs in executor."""
    try:
        import feedparser  # type: ignore
        return feedparser.parse(xml_text).entries
    except ImportError:
        logger.warning("[news] feedparser not installed — RSS disabled")
        return []


async def _fetch_rss(session, url: str, source: str, section: str) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        async with session.get(url, timeout=8) as resp:
            if resp.status not in (200, 301, 302):
                return items
            xml = await resp.text(errors="replace")
    except Exception as e:
        logger.debug("[news] RSS %s error: %r", source, e)
        return items

    loop    = asyncio.get_event_loop()
    entries = await loop.run_in_executor(None, _parse_feed_sync, xml)

    now = datetime.now(timezone.utc)
    for e in entries[:10]:   # cap per feed
        title   = (e.get("title") or "").strip()
        link    = (e.get("link") or "").strip()
        summary = re.sub(r"<[^>]+>", "", e.get("summary") or e.get("description") or "").strip()[:400]
        if not title or not link:
            continue

        # Parse published date
        pub = now
        if e.get("published_parsed"):
            try:
                import time as _time
                pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                pass

        items.append({
            "url":          link,
            "title":        title,
            "summary":      summary,
            "source":       source,
            "section":      section,
            "published_at": pub,
            "created_at":   now,
            "tickers":      _extract_tickers(f"{title} {summary}"),
            "image_url":    None,
        })

    return items


# ── GNews fetcher ─────────────────────────────────────────────────────────────

async def _fetch_gnews(session, query: str, section: str) -> List[NewsItem]:
    api_key = os.getenv("GNEWS_API_KEY", "")
    if not api_key:
        logger.debug("[news] GNEWS_API_KEY not set — skipping GNews")
        return []

    url = (
        f"https://gnews.io/api/v4/search"
        f"?q={query.replace(' ', '+')}&lang=en&max=10&apikey={api_key}"
    )
    items: List[NewsItem] = []
    now = datetime.now(timezone.utc)

    try:
        async with session.get(url, timeout=8) as resp:
            if resp.status in (403, 429):
                logger.debug("[news] GNews HTTP %s — silent", resp.status)
                return items
            if resp.status != 200:
                return items
            data = await resp.json()
    except Exception as e:
        logger.debug("[news] GNews error: %r", e)
        return items

    for art in data.get("articles", []):
        title   = (art.get("title") or "").strip()
        link    = (art.get("url") or "").strip()
        summary = (art.get("description") or "").strip()[:400]
        if not title or not link:
            continue

        pub = now
        try:
            pub = datetime.fromisoformat(art["publishedAt"].replace("Z", "+00:00"))
        except Exception:
            pass

        items.append({
            "url":          link,
            "title":        title,
            "summary":      summary,
            "source":       art.get("source", {}).get("name", "GNews"),
            "section":      section,
            "published_at": pub,
            "created_at":   now,
            "tickers":      _extract_tickers(f"{title} {summary}"),
            "image_url":    art.get("image"),
        })

    return items


# ── Upsert to MongoDB ─────────────────────────────────────────────────────────

async def _upsert_items(items: List[NewsItem]) -> tuple[int, int]:
    """Returns (inserted, skipped)."""
    col = _get_collection()
    if col is None or not items:
        return 0, 0

    from pymongo import UpdateOne  # type: ignore
    from pymongo.errors import BulkWriteError  # type: ignore

    ops = [
        UpdateOne(
            {"url": item["url"]},
            {"$setOnInsert": item},
            upsert=True,
        )
        for item in items
    ]

    try:
        result = await col.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        skipped  = len(items) - inserted
        return inserted, skipped
    except BulkWriteError as bwe:
        # Duplicate key errors are expected and silent
        inserted = bwe.details.get("nUpserted", 0)
        skipped  = len(items) - inserted
        return inserted, skipped
    except Exception as e:
        logger.warning("[news] upsert error: %s", e)
        return 0, len(items)


# ── Single fetch cycle ────────────────────────────────────────────────────────

async def run_fetch_cycle() -> dict[str, int]:
    """
    Fetches all sections once. Returns {section: inserted_count}.
    Call this directly for one-shot fetches or testing.
    """
    try:
        import aiohttp  # type: ignore
    except ImportError:
        logger.error("[news] aiohttp not installed")
        return {}

    totals: dict[str, int] = {}
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for section, feeds in _RSS_FEEDS.items():
            all_items: List[NewsItem] = []

            # RSS tasks
            rss_tasks = [_fetch_rss(session, url, src, section) for src, url in feeds]
            rss_results = await asyncio.gather(*rss_tasks, return_exceptions=True)
            for r in rss_results:
                if isinstance(r, list):
                    all_items.extend(r)

            # GNews supplement for applicable sections
            if section in _GNEWS_QUERIES:
                gnews = await _fetch_gnews(session, _GNEWS_QUERIES[section], section)
                all_items.extend(gnews)

            inserted, skipped = await _upsert_items(all_items)
            totals[section] = inserted
            logger.info("[news] %-18s  fetched=%d  inserted=%d  dup=%d",
                        section, len(all_items), inserted, skipped)

    return totals


# ── Background loop ───────────────────────────────────────────────────────────

_loop_task: Optional[asyncio.Task] = None   # type: ignore[type-arg]
POLL_INTERVAL_S = 300   # 5 minutes


async def _loop() -> None:
    logger.info("[news] Loop started (interval=%ds)", POLL_INTERVAL_S)
    while True:
        try:
            totals = await run_fetch_cycle()
            logger.info("[news] Cycle done: %s", totals)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[news] Cycle error: %s", e)
        await asyncio.sleep(POLL_INTERVAL_S)


def start_news_loop() -> None:
    """Call from FastAPI lifespan to start background ingestion."""
    global _loop_task
    if _loop_task is None or _loop_task.done():
        _loop_task = asyncio.create_task(_loop())
        logger.info("[news] Background task created")


def stop_news_loop() -> None:
    """Call on app shutdown."""
    global _loop_task
    if _loop_task and not _loop_task.done():
        _loop_task.cancel()
        logger.info("[news] Background task cancelled")


# ── FastAPI route helper ──────────────────────────────────────────────────────

async def get_news_feed(section: Optional[str] = None, limit: int = 20) -> List[dict]:
    """
    Fetch latest news from MongoDB for the given section.
    Used by the /news API route.
    """
    col = _get_collection()
    if col is None:
        return []

    query  = {"section": section} if section else {}
    cursor = col.find(query, {"_id": 0}).sort("published_at", -1).limit(limit)

    results = []
    async for doc in cursor:
        # Serialize datetimes for JSON
        for field in ("published_at", "created_at"):
            if isinstance(doc.get(field), datetime):
                doc[field] = doc[field].isoformat()
        results.append(doc)
    return results
