"""
app/services/news_service.py  —  Async News Ingestion Engine + Sentiment Pipeline
===================================================================================

SECTIONS
  1. indian_market   — NSE/BSE company news via LiveMint + MoneyControl RSS
  2. global_market   — US/Global markets via GNews API + Reuters RSS
  3. macro_impact    — RBI / Fed / FII / DII policy via RSS
  4. swing_signals   — Analyst upgrades/downgrades via Moneycontrol + ET RSS

SENTIMENT PIPELINE (per article, batched)
  FinBERT (50%) + VADER (20%) + Macro Context (30%)
  → Time-decayed composite score in [-1, +1]
  → Enriched with: sentiment_label, confidence, action, reasoning,
                   event_type, primary_stocks, secondary_stocks, sectors

SCHEMA (news_feed document — enriched)
  {
    url, title, summary, source, section, published_at, created_at, image_url,
    sentiment_score, sentiment_label, confidence, confidence_pct, action,
    reasoning, event_type, primary_stocks, secondary_stocks, sectors,
    source_reliability, time_decay, finbert_score, vader_score, macro_score,
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


class NewsItem(TypedDict):
    url:          str
    title:        str
    summary:      str
    source:       str
    section:      str
    published_at: datetime
    created_at:   datetime
    image_url:    Optional[str]


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

_GNEWS_QUERIES: dict[str, str] = {
    "global_market":  "stock market OR Fed Reserve OR S&P 500",
    "macro_impact":   "RBI OR Federal Reserve OR inflation OR FII DII India",
    "swing_signals":  "stock upgrade downgrade analyst India NSE",
}


def _parse_feed_sync(xml_text: str) -> list[dict]:
    try:
        import feedparser  # type: ignore
        return feedparser.parse(xml_text).entries
    except ImportError:
        logger.warning("[news] feedparser not installed")
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
    now     = datetime.now(timezone.utc)

    for e in entries[:10]:
        title   = (e.get("title") or "").strip()
        link    = (e.get("link") or "").strip()
        summary = re.sub(r"<[^>]+>", "", e.get("summary") or e.get("description") or "").strip()[:400]
        if not title or not link:
            continue

        pub = now
        if e.get("published_parsed"):
            try:
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
            "image_url":    None,
        })
    return items


async def _fetch_gnews(session, query: str, section: str) -> List[NewsItem]:
    api_key = os.getenv("GNEWS_API_KEY", "")
    if not api_key:
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
            "image_url":    art.get("image"),
        })
    return items


_mongo_col = None


def _get_collection():
    global _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    try:
        from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        from app.core.config import settings
        client     = AsyncIOMotorClient(settings.MONGODB_URI, serverSelectionTimeoutMS=5000)
        _mongo_col = client["quantedge"]["news_feed"]
        logger.info("[news] MongoDB connected")
    except Exception as e:
        logger.error("[news] MongoDB init failed: %s", e)
        _mongo_col = None
    return _mongo_col


async def _upsert_items(items: List[dict]) -> tuple[int, int]:
    col = _get_collection()
    if col is None or not items:
        return 0, 0

    from pymongo import UpdateOne  # type: ignore
    from pymongo.errors import BulkWriteError  # type: ignore

    _base_fields = {
        "url", "title", "summary", "source", "section",
        "published_at", "created_at", "image_url",
    }
    _sentiment_fields = {
        "sentiment_score", "sentiment_label", "confidence", "confidence_pct",
        "action", "reasoning", "event_type", "primary_stocks", "secondary_stocks",
        "sectors", "source_reliability", "time_decay", "finbert_score",
        "finbert_prob", "vader_score", "macro_score", "macro_confidence",
    }

    ops = [
        UpdateOne(
            {"url": item["url"]},
            {
                "$setOnInsert": {k: v for k, v in item.items() if k in _base_fields},
                "$set":         {k: v for k, v in item.items() if k in _sentiment_fields},
            },
            upsert=True,
        )
        for item in items
    ]

    try:
        result   = await col.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        return inserted, len(items) - inserted
    except BulkWriteError as bwe:
        inserted = bwe.details.get("nUpserted", 0)
        return inserted, len(items) - inserted
    except Exception as e:
        logger.warning("[news] upsert error: %s", e)
        return 0, len(items)


async def run_fetch_cycle() -> dict[str, int]:
    """Fetch all sections, enrich with sentiment pipeline, upsert to MongoDB."""
    try:
        import aiohttp  # type: ignore
    except ImportError:
        logger.error("[news] aiohttp not installed")
        return {}

    from app.services.sentiment_engine import enrich_batch  # type: ignore

    totals: dict[str, int] = {}
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for section, feeds in _RSS_FEEDS.items():
            all_items: List[dict] = []

            rss_tasks   = [_fetch_rss(session, url, src, section) for src, url in feeds]
            rss_results = await asyncio.gather(*rss_tasks, return_exceptions=True)
            for r in rss_results:
                if isinstance(r, list):
                    all_items.extend(r)

            if section in _GNEWS_QUERIES:
                gnews = await _fetch_gnews(session, _GNEWS_QUERIES[section], section)
                all_items.extend(gnews)

            enriched = await enrich_batch(all_items)
            inserted, skipped = await _upsert_items(enriched)
            totals[section] = inserted
            logger.info("[news] %-18s  fetched=%d  inserted=%d  dup=%d",
                        section, len(all_items), inserted, skipped)

    return totals


_loop_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]
POLL_INTERVAL_S = 300


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
    global _loop_task
    if _loop_task is None or _loop_task.done():
        _loop_task = asyncio.create_task(_loop())
        logger.info("[news] Background task created")


def stop_news_loop() -> None:
    global _loop_task
    if _loop_task and not _loop_task.done():
        _loop_task.cancel()
        logger.info("[news] Background task cancelled")


async def get_news_feed(
    section:    Optional[str] = None,
    limit:      int           = 20,
    event_type: Optional[str] = None,
    action:     Optional[str] = None,
    stock:      Optional[str] = None,
) -> List[dict]:
    col = _get_collection()
    if col is None:
        return []

    query: dict = {}
    if section:
        query["section"] = section
    if event_type:
        query["event_type"] = event_type
    if action:
        query["action"] = action
    if stock:
        query["$or"] = [
            {"primary_stocks":   stock.upper()},
            {"secondary_stocks": stock.upper()},
        ]

    cursor  = col.find(query, {"_id": 0}).sort("published_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        for field in ("published_at", "created_at"):
            if isinstance(doc.get(field), datetime):
                doc[field] = doc[field].isoformat()
        results.append(doc)
    return results


async def get_clustered_feed(section: Optional[str] = None, limit: int = 50) -> dict:
    from app.services.sentiment_engine import cluster_news  # type: ignore
    items = await get_news_feed(section=section, limit=limit)
    return cluster_news(items)
