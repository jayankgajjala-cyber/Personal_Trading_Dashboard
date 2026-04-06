"""
app/services/sentiment_engine.py — Dynamic Hybrid Sentiment Engine
===================================================================

PIPELINE
  FinBERT + VADER + Macro Context, weights selected dynamically per article
  via WEIGHTING_PROFILES keyed on section / event_type.

WEIGHTING PROFILES
  corporate   {finbert:0.7, macro:0.1, vader:0.2}  Earnings, M&A
  macro_heavy {finbert:0.2, macro:0.7, vader:0.1}  macro_impact section / RBI / Fed / Inflation
  commodity   {finbert:0.3, macro:0.5, vader:0.2}  Oil, Gold, Metals sector
  default     {finbert:0.4, macro:0.3, vader:0.3}  all other cases

THRESHOLDS
  Buy  ≥ +0.15  |  Sell ≤ -0.15  |  Hold otherwise
  Confidence gate: < 0.40 → Hold regardless of score

FEATURES
  • Time-decay applied per article (exponential, half-life = 12h)
  • Source reliability weighting (Tier1=1.0, Tier2=0.7, Unknown=0.4)
  • Entity extraction → primary/secondary stocks (NSE/BSE) + sectors
  • Event classification → Earnings | Regulation | Macro | M&A | Fraud/Negative
  • Confidence score = f(FinBERT prob, macro confidence, source reliability)
  • FinBERT device resolved from TORCH_DEVICE env var (default: cpu)
  • Async batch processing — DB-safe, non-blocking
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import math

logger = logging.getLogger(__name__)

# ── Dynamic Weighting Profiles ────────────────────────────────────────────────

class _WeightProfile:
    __slots__ = ("finbert", "macro", "vader", "name")

    def __init__(self, name: str, finbert: float, macro: float, vader: float) -> None:
        assert abs(finbert + macro + vader - 1.0) < 1e-6, "Weights must sum to 1.0"
        self.name    = name
        self.finbert = finbert
        self.macro   = macro
        self.vader   = vader


WEIGHTING_PROFILES: Dict[str, _WeightProfile] = {
    # High FinBERT trust for company-specific language
    "corporate":   _WeightProfile("corporate",   finbert=0.7, macro=0.1, vader=0.2),
    # Macro signals dominate when the story is about policy / indices
    "macro_heavy": _WeightProfile("macro_heavy", finbert=0.2, macro=0.7, vader=0.1),
    # Commodities: macro context matters more than corporate language
    "commodity":   _WeightProfile("commodity",   finbert=0.3, macro=0.5, vader=0.2),
    # Balanced baseline
    "default":     _WeightProfile("default",     finbert=0.4, macro=0.3, vader=0.3),
}

# section → profile name
_SECTION_PROFILE: Dict[str, str] = {
    "macro_impact":  "macro_heavy",
    "global_market": "macro_heavy",
    "indian_market": "default",
    "swing_signals": "corporate",
}

# event_type → profile name  (event wins over section when both apply)
_EVENT_PROFILE: Dict[str, str] = {
    "Earnings":       "corporate",
    "M&A":            "corporate",
    "Regulation":     "macro_heavy",
    "Macro":          "macro_heavy",
    "Fraud/Negative": "corporate",   # FinBERT excels at detecting negative corporate tone
    "General":        "default",
}

# Commodity sector tag triggers commodity profile (overrides section, loses to event)
_COMMODITY_SECTORS = {"Energy", "Metals"}


def _resolve_profile(section: str, event_type: str, sectors: List[str]) -> _WeightProfile:
    """
    Priority order (highest → lowest):
      1. event_type  (most specific signal)
      2. commodity sector detection
      3. section
      4. default
    """
    if event_type in _EVENT_PROFILE and event_type != "General":
        return WEIGHTING_PROFILES[_EVENT_PROFILE[event_type]]
    if any(s in _COMMODITY_SECTORS for s in sectors):
        return WEIGHTING_PROFILES["commodity"]
    return WEIGHTING_PROFILES.get(_SECTION_PROFILE.get(section, "default"), WEIGHTING_PROFILES["default"])

DECAY_HALF_LIFE_H = 12.0          # exponential half-life in hours

SOURCE_RELIABILITY: Dict[str, float] = {
    # Tier 1
    "reuters":          1.0,
    "bloomberg":        1.0,
    "financial times":  1.0,
    "ft":               1.0,
    "wsj":              1.0,
    "wall street journal": 1.0,
    # Tier 2
    "economic times":   0.7,
    "livemint":         0.7,
    "mint":             0.7,
    "moneycontrol":     0.7,
    "cnbc":             0.7,
    "business standard": 0.7,
    "hindu businessline": 0.7,
    "gnews":            0.7,
    "yahoo finance":    0.7,
    # Default unknown → 0.4
}

EVENT_WEIGHT_MULTIPLIER: Dict[str, float] = {
    "Earnings":          1.20,
    "Regulation":        1.10,
    "Macro":             1.05,
    "M&A":               1.15,
    "Fraud/Negative":    1.30,
    "General":           1.00,
}

# NSE symbol → company name mapping (top 100 NSE stocks + common aliases)
NSE_ENTITY_MAP: Dict[str, str] = {
    "reliance": "RELIANCE", "ril": "RELIANCE",
    "tcs": "TCS", "tata consultancy": "TCS",
    "infosys": "INFY", "infy": "INFY",
    "hdfc bank": "HDFCBANK", "hdfcbank": "HDFCBANK",
    "icici bank": "ICICIBANK", "icici": "ICICIBANK",
    "kotak": "KOTAKBANK", "kotak mahindra": "KOTAKBANK",
    "wipro": "WIPRO",
    "hcl": "HCLTECH", "hcltech": "HCLTECH",
    "bajaj finance": "BAJFINANCE", "bajfinance": "BAJFINANCE",
    "bajaj finserv": "BAJAJFINSV",
    "bharti airtel": "BHARTIARTL", "airtel": "BHARTIARTL",
    "asian paints": "ASIANPAINT",
    "maruti": "MARUTI", "maruti suzuki": "MARUTI",
    "titan": "TITAN",
    "nestle india": "NESTLEIND", "nestle": "NESTLEIND",
    "hindustan unilever": "HINDUNILVR", "hul": "HINDUNILVR",
    "itc": "ITC",
    "axis bank": "AXISBANK",
    "state bank": "SBIN", "sbi": "SBIN",
    "sun pharma": "SUNPHARMA",
    "dr reddy": "DRREDDY", "dr. reddy": "DRREDDY",
    "cipla": "CIPLA",
    "adani": "ADANIENT",
    "adani ports": "ADANIPORTS",
    "adani green": "ADANIGREEN",
    "adani enterprises": "ADANIENT",
    "tata motors": "TATAMOTORS",
    "tata steel": "TATASTEEL",
    "tata power": "TATAPOWER",
    "ongc": "ONGC",
    "ntpc": "NTPC",
    "power grid": "POWERGRID",
    "coal india": "COALINDIA",
    "upl": "UPL",
    "divis": "DIVISLAB", "divi's": "DIVISLAB",
    "dmart": "DMART", "avenue supermarts": "DMART",
    "zomato": "ZOMATO",
    "paytm": "PAYTM", "one97": "PAYTM",
    "nykaa": "NYKAA", "fsl": "NYKAA",
    "policybazaar": "POLICYBZR",
    "tech mahindra": "TECHM",
    "ltimindtree": "LTIM", "lti": "LTIM",
    "l&t": "LT", "larsen": "LT", "larsen & toubro": "LT",
    "ultratech": "ULTRACEMCO",
    "grasim": "GRASIM",
    "hindalco": "HINDALCO",
    "jsw steel": "JSWSTEEL",
    "m&m": "M&M", "mahindra": "M&M",
    "hero motocorp": "HEROMOTOCO", "hero": "HEROMOTOCO",
    "eicher": "EICHERMOT",
    "shriram finance": "SHRIRAMFIN",
    "srf": "SRF",
    "pi industries": "PIIND",
    "mphasis": "MPHASIS",
    "persistent": "PERSISTENT",
    "coforge": "COFORGE",
    "indusind": "INDUSINDBK", "indusind bank": "INDUSINDBK",
    "yes bank": "YESBANK",
    "bandhan": "BANDHANBNK",
    "federal bank": "FEDERALBNK",
    "rbl bank": "RBLBANK",
    "canara bank": "CANARABANK",
    "bank of baroda": "BANKBARODA",
    "pnb": "PNB", "punjab national": "PNB",
    "godrej": "GODREJCP",
    "dabur": "DABUR",
    "marico": "MARICO",
    "britannia": "BRITANNIA",
    "colgate": "COLPAL",
    "pidilite": "PIDILITIND",
    "berger paints": "BERGEPAINT",
    "abbvie": "ABBOTINDIA", "abbott india": "ABBOTINDIA",
    "torrent pharma": "TORNTPHARM",
    "lupin": "LUPIN",
    "aurobindo": "AUROPHARMA",
    "biocon": "BIOCON",
    "alkem": "ALKEM",
    "zydus": "ZYDUSLIFE",
    "havells": "HAVELLS",
    "voltas": "VOLTAS",
    "whirlpool india": "WHIRLPOOL",
    "dixon": "DIXON",
    "amber": "AMBER",
    "irctc": "IRCTC",
    "indian railway": "IRCTC",
    "interglobe": "INDIGO", "indigo": "INDIGO",
    "spicejet": "SPICEJET",
    "concor": "CONCOR",
    "balkrishna": "BALKRISIND", "bkt": "BALKRISIND",
    "mrf": "MRF",
    "apollo tyres": "APOLLOTYRE",
    "ceat": "CEATLTD",
    "exide": "EXIDEIND",
    "amara raja": "AMARAJABAT",
    "manappuram": "MANAPPURAM",
    "muthoot": "MUTHOOTFIN",
    "cholafin": "CHOLAFIN", "chola": "CHOLAFIN",
    "page industries": "PAGEIND",
    "varun beverages": "VBL",
    "united spirits": "MCDOWELL-N",
    "radico": "RADICO",
    "jubilant": "JUBLFOOD", "jubilant foodworks": "JUBLFOOD",
    "westlife": "WESTLIFE", "mcdonald's india": "WESTLIFE",
    "devyani": "DEVYANI",
    "restaurant brands": "RBA",
    "nse": "NSEI", "sensex": "SENSEX", "nifty": "NIFTY",
}

SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "Banking":      ["bank", "nbfc", "lending", "deposit", "npa", "credit"],
    "IT":           ["software", "it ", "tech", "digital", "cloud", "saas", "outsourcing"],
    "Pharma":       ["pharma", "drug", "medicine", "fda", "usfda", "api", "biotech"],
    "Energy":       ["oil", "gas", "refinery", "petroleum", "crude", "power", "renewable", "solar", "wind"],
    "Auto":         ["auto", "vehicle", "ev ", "electric vehicle", "car", "motorcycle", "tyre"],
    "FMCG":         ["fmcg", "consumer", "staples", "food", "beverage", "household"],
    "Metals":       ["steel", "aluminium", "copper", "zinc", "metals", "mining"],
    "Infra":        ["infrastructure", "cement", "construction", "road", "highway", "airport"],
    "Finance":      ["insurance", "mutual fund", "asset management", "wealth", "brokerage"],
    "Telecom":      ["telecom", "5g", "spectrum", "airtel", "jio", "vi "],
    "Realty":       ["real estate", "realty", "housing", "property", "reit"],
    "Aviation":     ["airline", "aviation", "aircraft", "flight"],
}

EVENT_KEYWORDS: Dict[str, List[str]] = {
    "Earnings":       ["earnings", "profit", "revenue", "quarterly", "q1 ", "q2 ", "q3 ", "q4 ",
                       "results", "ebitda", "pat", "net income", "loss"],
    "Regulation":     ["rbi", "sebi", "regulation", "policy", "compliance", "ban", "penalty",
                       "fine", "licence", "circular", "guidelines", "norms", "rate cut", "rate hike"],
    "Macro":          ["gdp", "inflation", "cpi", "wpi", "repo rate", "fed", "federal reserve",
                       "fiscal", "budget", "trade deficit", "fii", "dii", "foreign investment",
                       "s&p 500", "dow jones", "nifty 50", "global market"],
    "M&A":            ["merger", "acquisition", "takeover", "buyout", "stake", "deal", "joint venture",
                       "partnership", "collaboration", "mou"],
    "Fraud/Negative": ["fraud", "scam", "ponzi", "money laundering", "investigation", "arrest",
                       "bankruptcy", "default", "npa", "write-off", "probe", "raid", "cbi", "ed "],
}

# ── Lazy model singletons ─────────────────────────────────────────────────────

_finbert_pipeline = None
_vader_analyzer   = None
_finbert_lock     = asyncio.Lock()
_vader_lock       = asyncio.Lock()


async def _get_finbert():
    global _finbert_pipeline
    async with _finbert_lock:
        if _finbert_pipeline is not None:
            return _finbert_pipeline
        try:
            import os
            import torch  # noqa
            from transformers import pipeline  # type: ignore

            # Honour TORCH_DEVICE env var; fall back to cpu
            _torch_device = os.getenv("TORCH_DEVICE", "cpu").strip().lower()
            # Translate string device to int index for pipeline(device=)
            if _torch_device == "cpu":
                _device_arg = -1
            elif _torch_device.startswith("cuda"):
                # e.g. "cuda:0" → 0, "cuda" → 0
                try:
                    _device_arg = int(_torch_device.split(":")[-1]) if ":" in _torch_device else 0
                except ValueError:
                    _device_arg = 0
            else:
                _device_arg = -1   # unrecognised → safe CPU fallback

            logger.info("[sentiment] Loading FinBERT on device=%s (arg=%s)…", _torch_device, _device_arg)
            _finbert_pipeline = pipeline(
                "text-classification",
                model="ProsusAI/finbert",
                tokenizer="ProsusAI/finbert",
                top_k=None,
                device=_device_arg,
                truncation=True,
                max_length=512,
            )
            logger.info("[sentiment] FinBERT ready (device=%s)", _torch_device)
        except Exception as e:
            logger.warning("[sentiment] FinBERT unavailable (%s) — using VADER-only fallback", e)
            _finbert_pipeline = None
        return _finbert_pipeline


async def _get_vader():
    global _vader_analyzer
    async with _vader_lock:
        if _vader_analyzer is not None:
            return _vader_analyzer
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # type: ignore
            _vader_analyzer = SentimentIntensityAnalyzer()
        except Exception as e:
            logger.warning("[sentiment] VADER unavailable: %s", e)
            _vader_analyzer = None
        return _vader_analyzer


# ── Source reliability lookup ─────────────────────────────────────────────────

def _source_reliability(source: str) -> float:
    s = source.lower().strip()
    for name, score in SOURCE_RELIABILITY.items():
        if name in s:
            return score
    return 0.4  # unknown


# ── Time decay ────────────────────────────────────────────────────────────────

def _time_decay(published_at: datetime) -> float:
    """Exponential decay. Returns 1.0 for brand-new, approaches 0 for old."""
    now  = datetime.now(timezone.utc)
    pub  = published_at.replace(tzinfo=timezone.utc) if published_at.tzinfo is None else published_at
    age_h = (now - pub).total_seconds() / 3600.0
    return math.exp(-math.log(2) * age_h / DECAY_HALF_LIFE_H)


# ── Entity extraction ─────────────────────────────────────────────────────────

def _extract_entities(text: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Returns (primary_stocks, secondary_stocks, sectors).
    Primary  = stocks mentioned by name (explicit match).
    Secondary = stocks inferred from sector / ticker pattern.
    """
    lower = text.lower()
    primary:   List[str] = []
    secondary: List[str] = []

    for alias, symbol in NSE_ENTITY_MAP.items():
        if alias in lower and symbol not in primary:
            primary.append(symbol)

    # Regex fallback for ALL-CAPS tickers (e.g. INFY, RELIANCE)
    _NOISE = {"THE", "AND", "FOR", "NSE", "BSE", "RBI", "FED", "FII", "DII",
              "GDP", "CPI", "IPO", "ETF", "USD", "INR", "USA", "CEO", "CFO",
              "NSE", "WITH", "FROM", "INTO", "THIS", "THAT"}
    caps = re.findall(r'\b([A-Z]{2,12})\b', text)
    for c in caps:
        if c not in _NOISE and c not in primary and len(c) >= 3:
            secondary.append(c)

    # Sector extraction
    sectors: List[str] = []
    for sector, kws in SECTOR_KEYWORDS.items():
        if any(kw in lower for kw in kws):
            sectors.append(sector)

    return list(dict.fromkeys(primary))[:5], list(dict.fromkeys(secondary))[:5], list(dict.fromkeys(sectors))[:3]


# ── Event classification ──────────────────────────────────────────────────────

def _classify_event(text: str) -> str:
    lower = text.lower()
    scores: Dict[str, int] = {}
    for event, kws in EVENT_KEYWORDS.items():
        scores[event] = sum(1 for kw in kws if kw in lower)
    best = max(scores, key=lambda k: scores[k])
    return best if scores[best] > 0 else "General"


# ── FinBERT scoring ───────────────────────────────────────────────────────────

def _finbert_score_sync(pipe, text: str) -> Tuple[float, float]:
    """
    Returns (score [-1, +1], probability [0, 1]).
    Runs synchronously inside an executor.
    """
    try:
        results = pipe(text[:512])
        label_map = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}
        best = max(results[0], key=lambda x: x["score"])
        raw_label = best["label"].lower()
        score = label_map.get(raw_label, 0.0)
        prob  = best["score"]
        return score, prob
    except Exception as e:
        logger.debug("[sentiment] FinBERT inference error: %s", e)
        return 0.0, 0.5


async def _finbert_score(text: str) -> Tuple[float, float]:
    pipe = await _get_finbert()
    if pipe is None:
        return 0.0, 0.5
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _finbert_score_sync, pipe, text)


# ── VADER scoring ─────────────────────────────────────────────────────────────

def _vader_score_sync(analyzer, text: str) -> float:
    """Returns compound score in [-1, +1]."""
    try:
        return analyzer.polarity_scores(text)["compound"]
    except Exception:
        return 0.0


async def _vader_score(text: str) -> float:
    analyzer = await _get_vader()
    if analyzer is None:
        return 0.0
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _vader_score_sync, analyzer, text)


# ── Macro Context Layer ───────────────────────────────────────────────────────

async def _get_macro_signal() -> Tuple[float, float]:
    """
    Queries MongoDB macro_signals collection for structured macro state.
    Each document: { factor, direction (+1/-1), weight, confidence, updated_at }
    Returns (weighted_macro_score [-1,+1], avg_confidence [0,1]).
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        from app.core.config import settings

        client = AsyncIOMotorClient(settings.MONGODB_URI, serverSelectionTimeoutMS=3000)
        col    = client["quantedge"]["macro_signals"]

        # Only use signals updated in last 24h
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        cursor = col.find({"updated_at": {"$gte": cutoff}})

        total_weight = 0.0
        weighted_dir = 0.0
        confidences  = []

        async for doc in cursor:
            direction  = float(doc.get("direction", 0))     # +1 or -1
            weight     = float(doc.get("weight", 1.0))
            confidence = float(doc.get("confidence", 0.5))
            weighted_dir += direction * weight * confidence
            total_weight += weight
            confidences.append(confidence)

        if total_weight == 0:
            # Fallback: neutral with low confidence
            return 0.0, 0.3

        macro_score      = max(-1.0, min(1.0, weighted_dir / total_weight))
        avg_confidence   = sum(confidences) / len(confidences)
        return macro_score, avg_confidence

    except Exception as e:
        logger.debug("[sentiment] Macro signal fetch failed: %s", e)
        return 0.0, 0.3


# ── Seed/upsert macro signals (called on startup to ensure baseline data) ─────

async def ensure_macro_signals() -> None:
    """
    Insert default macro signals if collection is empty.
    Real signals should be updated via separate ingestion jobs or admin API.
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        from pymongo import UpdateOne  # type: ignore
        from app.core.config import settings

        client = AsyncIOMotorClient(settings.MONGODB_URI, serverSelectionTimeoutMS=3000)
        col    = client["quantedge"]["macro_signals"]

        now = datetime.now(timezone.utc)
        baseline = [
            {"factor": "RBI_POLICY",       "direction": 0,   "weight": 1.5, "confidence": 0.5,  "updated_at": now},
            {"factor": "INDIA_CPI",         "direction": -0.5,"weight": 1.0, "confidence": 0.6,  "updated_at": now},
            {"factor": "SP500_TREND",       "direction": 1,   "weight": 1.2, "confidence": 0.7,  "updated_at": now},
            {"factor": "DOW_JONES_TREND",   "direction": 1,   "weight": 1.0, "confidence": 0.6,  "updated_at": now},
            {"factor": "NIFTY50_TREND",     "direction": 0.5, "weight": 1.3, "confidence": 0.65, "updated_at": now},
            {"factor": "FII_FLOW",          "direction": 0,   "weight": 1.1, "confidence": 0.5,  "updated_at": now},
            {"factor": "DXY_INDEX",         "direction": -0.3,"weight": 0.8, "confidence": 0.55, "updated_at": now},
            {"factor": "CRUDE_OIL",         "direction": -0.5,"weight": 0.9, "confidence": 0.6,  "updated_at": now},
        ]
        ops = [
            UpdateOne({"factor": s["factor"]}, {"$setOnInsert": s}, upsert=True)
            for s in baseline
        ]
        await col.bulk_write(ops, ordered=False)
    except Exception as e:
        logger.debug("[sentiment] ensure_macro_signals failed: %s", e)


# ── Composite scoring ─────────────────────────────────────────────────────────

def _normalize(score: float) -> float:
    return max(-1.0, min(1.0, score))


def _label_from_score(score: float) -> str:
    if score >  0.15: return "Bullish"
    if score < -0.15: return "Bearish"
    return "Neutral"


def _action_from_score(score: float, confidence: float) -> str:
    if confidence < 0.40:
        return "Hold"
    if score >=  0.15: return "Buy"
    if score <= -0.15: return "Sell"
    return "Hold"


def _build_reasoning(
    finbert_score: float,
    vader_score:   float,
    macro_score:   float,
    macro_conf:    float,
    event_type:    str,
    source_rel:    float,
    decay:         float,
    sectors:       List[str],
    profile_name:  str = "default",
) -> str:
    parts: List[str] = []

    fb_label = _label_from_score(finbert_score)
    parts.append(f"FinBERT: {fb_label} ({finbert_score:+.2f})")

    mc_label = _label_from_score(macro_score)
    if abs(macro_score) > 0.05:
        parts.append(f"Macro: {mc_label} (conf={macro_conf:.0%})")

    if event_type not in ("General",):
        parts.append(f"Event: {event_type}")

    if sectors:
        parts.append(f"Sectors: {', '.join(sectors[:2])}")

    if decay < 0.5:
        parts.append(f"Age-decay: {decay:.0%}")

    tier = "T1" if source_rel >= 1.0 else ("T2" if source_rel >= 0.7 else "Unk")
    parts.append(f"Source: {tier}")
    parts.append(f"Profile: {profile_name}")

    return " | ".join(parts)


async def score_article(
    title:        str,
    summary:      str,
    source:       str,
    published_at: datetime,
    section:      str = "indian_market",
) -> Dict:
    """
    Core scoring function. Resolves weight profile dynamically from
    section + event_type + sectors, then computes composite score.
    Returns full enriched sentiment payload.
    """
    text = f"{title}. {summary}"

    # Parallel: FinBERT + VADER + Macro
    (finbert_raw, finbert_prob), vader_raw, (macro_raw, macro_conf) = await asyncio.gather(
        _finbert_score(text),
        _vader_score(text),
        _get_macro_signal(),
    )

    # Time decay + source reliability
    decay      = _time_decay(published_at)
    source_rel = _source_reliability(source)

    # Event + entity classification (needed before profile resolution)
    event_type                            = _classify_event(text)
    primary_stocks, secondary_stocks, sectors = _extract_entities(text)

    # ── Dynamic weight resolution ─────────────────────────────────────────────
    profile        = _resolve_profile(section, event_type, sectors)
    event_mult     = EVENT_WEIGHT_MULTIPLIER.get(event_type, 1.0)

    # Weighted composite
    composite = (
        profile.finbert * finbert_raw +
        profile.vader   * vader_raw   +
        profile.macro   * macro_raw
    )

    # Apply source reliability scaling and event multiplier
    composite = composite * source_rel * event_mult

    # Apply time decay
    composite = composite * decay
    composite = _normalize(composite)

    # Confidence score
    confidence = _normalize(
        (finbert_prob * 0.5) +
        (macro_conf   * 0.3) +
        (source_rel   * 0.2)
    )

    sentiment_label = _label_from_score(composite)
    action          = _action_from_score(composite, confidence)
    reasoning       = _build_reasoning(
        finbert_raw, vader_raw, macro_raw, macro_conf,
        event_type, source_rel, decay, sectors, profile.name,
    )

    return {
        "sentiment_score":    round(composite,    4),
        "sentiment_label":    sentiment_label,
        "confidence":         round(confidence,   4),
        "confidence_pct":     round(confidence * 100, 1),
        "action":             action,
        "reasoning":          reasoning,
        "event_type":         event_type,
        "weight_profile":     profile.name,
        "primary_stocks":     primary_stocks,
        "secondary_stocks":   secondary_stocks,
        "sectors":            sectors,
        "source_reliability": source_rel,
        "time_decay":         round(decay,        4),
        "finbert_score":      round(finbert_raw,  4),
        "finbert_prob":       round(finbert_prob, 4),
        "vader_score":        round(vader_raw,    4),
        "macro_score":        round(macro_raw,    4),
        "macro_confidence":   round(macro_conf,   4),
    }


# ── Batch processor ───────────────────────────────────────────────────────────

BATCH_SIZE    = 8    # articles processed concurrently
BATCH_TIMEOUT = 30   # seconds per batch before giving up


async def enrich_batch(articles: List[Dict]) -> List[Dict]:
    """
    Takes a list of raw news dicts (NewsItem-like) and returns them
    with sentiment fields injected. Processes in batches of BATCH_SIZE.
    """
    enriched: List[Dict] = []

    for i in range(0, len(articles), BATCH_SIZE):
        chunk = articles[i : i + BATCH_SIZE]
        tasks = [
            score_article(
                title        = a.get("title", ""),
                summary      = a.get("summary", ""),
                source       = a.get("source", ""),
                published_at = a.get("published_at", datetime.now(timezone.utc)),
                section      = a.get("section", "indian_market"),
            )
            for a in chunk
        ]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=BATCH_TIMEOUT,
            )
            for article, result in zip(chunk, results):
                merged = dict(article)
                if isinstance(result, dict):
                    merged.update(result)
                else:
                    # Fallback neutral on error
                    merged.update({
                        "sentiment_score":  0.0,
                        "sentiment_label": "Neutral",
                        "confidence":       0.3,
                        "confidence_pct":   30.0,
                        "action":          "Hold",
                        "reasoning":       "Scoring error — defaulting to neutral",
                        "event_type":      "General",
                        "weight_profile":  "default",
                        "primary_stocks":  [],
                        "secondary_stocks": [],
                        "sectors":         [],
                    })
                enriched.append(merged)
        except asyncio.TimeoutError:
            logger.warning("[sentiment] Batch %d timed out — skipping", i)
            enriched.extend(chunk)

    return enriched


# ── Clustering ────────────────────────────────────────────────────────────────

def cluster_news(articles: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Groups articles by (primary_stock OR sector) + event_type.
    Returns dict keyed by cluster label, deduped by title similarity.
    """
    clusters: Dict[str, List[Dict]] = {}

    for art in articles:
        primaries = art.get("primary_stocks", [])
        sectors   = art.get("sectors", [])
        event     = art.get("event_type", "General")

        # Build cluster keys
        keys: List[str] = []
        for s in primaries[:1]:
            keys.append(f"{s}::{event}")
        for sec in sectors[:1]:
            keys.append(f"SECTOR:{sec}::{event}")
        if not keys:
            keys.append(f"GENERAL::{event}")

        for key in keys:
            if key not in clusters:
                clusters[key] = []
            # Simple dedup: skip if same title prefix already in cluster
            title_prefix = art.get("title", "")[:60].lower()
            if not any(a.get("title", "")[:60].lower() == title_prefix for a in clusters[key]):
                clusters[key].append(art)

    return clusters
