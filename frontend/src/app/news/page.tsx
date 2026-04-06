"use client";

import { useState, useEffect, useCallback } from "react";
import {
  RefreshCw, ExternalLink, TrendingUp, Globe,
  BarChart2, Zap, Clock, TrendingDown, Minus,
  ShoppingCart, AlertTriangle, Activity,
} from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

// ── Types ─────────────────────────────────────────────────────────────────────

interface NewsItem {
  url:               string;
  title:             string;
  summary:           string;
  source:            string;
  section:           string;
  published_at:      string;
  image_url:         string | null;
  // Sentiment fields
  sentiment_label?:  "Bullish" | "Bearish" | "Neutral";
  sentiment_score?:  number;
  confidence?:       number;
  confidence_pct?:   number;
  action?:           "Buy" | "Sell" | "Hold";
  reasoning?:        string;
  event_type?:       string;
  primary_stocks?:   string[];
  secondary_stocks?: string[];
  sectors?:          string[];
}

type SectionKey = "indian_market" | "global_market" | "macro_impact" | "swing_signals";

const SECTIONS = [
  {
    key:    "indian_market" as SectionKey,
    label:  "Indian Market",
    icon:   TrendingUp,
    color:  "text-cyan-400",
    border: "border-cyan-500/20",
    bg:     "bg-cyan-500/[0.03]",
    accent: "#22d3ee",
  },
  {
    key:    "global_market" as SectionKey,
    label:  "Global Market",
    icon:   Globe,
    color:  "text-blue-400",
    border: "border-blue-500/20",
    bg:     "bg-blue-500/[0.03]",
    accent: "#60a5fa",
  },
  {
    key:    "macro_impact" as SectionKey,
    label:  "Macro Impact",
    icon:   BarChart2,
    color:  "text-violet-400",
    border: "border-violet-500/20",
    bg:     "bg-violet-500/[0.03]",
    accent: "#a78bfa",
  },
  {
    key:    "swing_signals" as SectionKey,
    label:  "Swing Signals",
    icon:   Zap,
    color:  "text-amber-400",
    border: "border-amber-500/20",
    bg:     "bg-amber-500/[0.03]",
    accent: "#fbbf24",
  },
] as const;

// ── Helpers ───────────────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff <    60) return `${diff}s ago`;
  if (diff <  3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function sentimentConfig(label?: string) {
  switch (label) {
    case "Bullish":
      return {
        icon:   TrendingUp,
        color:  "text-emerald-400",
        bg:     "bg-emerald-500/10",
        border: "border-emerald-500/20",
        dot:    "bg-emerald-400",
      };
    case "Bearish":
      return {
        icon:   TrendingDown,
        color:  "text-red-400",
        bg:     "bg-red-500/10",
        border: "border-red-500/20",
        dot:    "bg-red-400",
      };
    default:
      return {
        icon:   Minus,
        color:  "text-muted-foreground/50",
        bg:     "bg-muted/20",
        border: "border-border/30",
        dot:    "bg-muted-foreground/30",
      };
  }
}

function actionConfig(action?: string) {
  switch (action) {
    case "Buy":  return { label: "BUY",  cls: "bg-emerald-500/15 text-emerald-400 border-emerald-500/25" };
    case "Sell": return { label: "SELL", cls: "bg-red-500/15 text-red-400 border-red-500/25" };
    default:     return { label: "HOLD", cls: "bg-muted/30 text-muted-foreground/60 border-border/20" };
  }
}

function eventBadgeClass(event?: string) {
  switch (event) {
    case "Earnings":         return "bg-blue-500/10 text-blue-400/80 border-blue-500/20";
    case "Regulation":       return "bg-violet-500/10 text-violet-400/80 border-violet-500/20";
    case "Macro":            return "bg-amber-500/10 text-amber-400/80 border-amber-500/20";
    case "M&A":              return "bg-cyan-500/10 text-cyan-400/80 border-cyan-500/20";
    case "Fraud/Negative":   return "bg-red-500/10 text-red-400/80 border-red-500/20";
    default:                 return "bg-muted/20 text-muted-foreground/40 border-border/20";
  }
}

// ── Confidence bar ────────────────────────────────────────────────────────────

function ConfBar({ pct }: { pct: number }) {
  const filled = Math.round(pct / 10);
  return (
    <div className="flex items-center gap-1" title={`Confidence: ${pct.toFixed(0)}%`}>
      {Array.from({ length: 10 }).map((_, i) => (
        <div
          key={i}
          className={cn(
            "h-1 w-1 rounded-full",
            i < filled ? "bg-current opacity-70" : "bg-current opacity-10",
          )}
        />
      ))}
      <span className="text-[8px] font-mono ml-0.5 opacity-60">{pct.toFixed(0)}%</span>
    </div>
  );
}

// ── News card ─────────────────────────────────────────────────────────────────

function NewsCard({ item }: { item: NewsItem }) {
  const sent   = sentimentConfig(item.sentiment_label);
  const act    = actionConfig(item.action);
  const SentIcon = sent.icon;

  const stocks = [...(item.primary_stocks ?? []), ...(item.secondary_stocks ?? [])]
    .filter((v, i, a) => a.indexOf(v) === i)
    .slice(0, 3);

  return (
    <div
      className={cn(
        "group flex flex-col gap-2 p-3 rounded-lg transition-all duration-150",
        "border hover:border-border/60 hover:bg-muted/25",
        sent.border,
      )}
    >
      {/* Top row: sentiment badge + action pill */}
      <div className="flex items-center justify-between gap-2 min-w-0">
        <div className={cn("flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-mono font-semibold", sent.bg, sent.color)}>
          <SentIcon className="w-2.5 h-2.5 shrink-0" />
          {item.sentiment_label ?? "—"}
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          {item.event_type && item.event_type !== "General" && (
            <span className={cn("text-[8px] font-mono px-1 py-0.5 rounded border", eventBadgeClass(item.event_type))}>
              {item.event_type}
            </span>
          )}
          <span className={cn("text-[8px] font-mono font-bold px-1.5 py-0.5 rounded border", act.cls)}>
            {act.label}
          </span>
        </div>
      </div>

      {/* Title */}
      <a
        href={item.url}
        target="_blank"
        rel="noopener noreferrer"
        className="group/link"
      >
        <p className="text-[11px] font-medium text-foreground leading-snug line-clamp-2 group-hover/link:text-cyan-400 transition">
          {item.title}
          <ExternalLink className="inline w-2.5 h-2.5 ml-1 opacity-0 group-hover/link:opacity-40 transition align-baseline" />
        </p>
      </a>

      {/* Summary */}
      {item.summary && (
        <p className="text-[10px] text-muted-foreground/70 leading-relaxed line-clamp-2">
          {item.summary}
        </p>
      )}

      {/* Reasoning */}
      {item.reasoning && (
        <p className="text-[9px] font-mono text-muted-foreground/40 leading-snug line-clamp-1 border-t border-border/20 pt-1.5">
          {item.reasoning}
        </p>
      )}

      {/* Footer row */}
      <div className="flex items-center justify-between gap-2 mt-0.5">
        <div className="flex items-center gap-1 min-w-0 flex-wrap">
          {/* Stock badges */}
          {stocks.map(s => (
            <span key={s} className="text-[8px] font-mono bg-muted/50 text-muted-foreground/60 px-1 py-0.5 rounded border border-border/20">
              {s}
            </span>
          ))}
          {/* Sector badges */}
          {(item.sectors ?? []).slice(0, 1).map(sec => (
            <span key={sec} className="text-[8px] font-mono text-muted-foreground/40">
              [{sec}]
            </span>
          ))}
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {/* Confidence */}
          {item.confidence_pct != null && (
            <div className={cn("flex items-center", sent.color)}>
              <ConfBar pct={item.confidence_pct} />
            </div>
          )}
          {/* Source + time */}
          <div className="flex items-center gap-1 text-[8px] font-mono text-muted-foreground/30">
            <span className="truncate max-w-[60px]">{item.source}</span>
            <Clock className="w-2 h-2 shrink-0" />
            <span>{timeAgo(item.published_at)}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Section summary bar ───────────────────────────────────────────────────────

function SectionSummary({ items }: { items: NewsItem[] }) {
  const bullish = items.filter(i => i.sentiment_label === "Bullish").length;
  const bearish = items.filter(i => i.sentiment_label === "Bearish").length;
  const neutral = items.length - bullish - bearish;
  const buys    = items.filter(i => i.action === "Buy").length;
  const sells   = items.filter(i => i.action === "Sell").length;

  if (!items.length) return null;

  return (
    <div className="flex items-center gap-3 px-3 py-1.5 bg-muted/20 border-b border-border/30 text-[8px] font-mono">
      <span className="text-emerald-400/70">▲ {bullish}</span>
      <span className="text-red-400/70">▼ {bearish}</span>
      <span className="text-muted-foreground/30">● {neutral}</span>
      <span className="ml-auto text-emerald-400/60">BUY {buys}</span>
      <span className="text-red-400/60">SELL {sells}</span>
    </div>
  );
}

// ── Per-section data hook ─────────────────────────────────────────────────────

function useNewsFeed(section: SectionKey) {
  const [items,     setItems]     = useState<NewsItem[]>([]);
  const [loading,   setLoading]   = useState(true);
  const [lastFetch, setLastFetch] = useState(0);

  const fetch = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const res = await api.get<NewsItem[]>(`/news/feed?section=${section}&limit=10`);
      if (res.data?.length) {
        setItems(res.data);
        setLastFetch(Date.now());
      }
    } catch {
      // Silent — keep stale data
    } finally {
      setLoading(false);
    }
  }, [section]);

  useEffect(() => { fetch(false); }, [fetch]);

  useEffect(() => {
    const jitter = Math.random() * 30_000;
    const id = setInterval(() => fetch(true), 5 * 60_000 + jitter);
    return () => clearInterval(id);
  }, [fetch]);

  return { items, loading, lastFetch, refetch: () => fetch(false) };
}

// ── Section panel ─────────────────────────────────────────────────────────────

function SectionPanel({ s }: { s: typeof SECTIONS[number] }) {
  const { items, loading, lastFetch, refetch } = useNewsFeed(s.key);
  const Icon = s.icon;

  return (
    <div className={cn("flex flex-col border rounded-lg overflow-hidden", s.border, s.bg)}>

      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 shrink-0">
        <div className="flex items-center gap-2">
          <Icon className={cn("w-3.5 h-3.5 shrink-0", s.color)} />
          <span className={cn("text-[10px] font-mono font-semibold uppercase tracking-wider", s.color)}>
            {s.label}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {lastFetch > 0 && (
            <span className="text-[9px] font-mono text-muted-foreground/25">
              {timeAgo(new Date(lastFetch).toISOString())}
            </span>
          )}
          <button onClick={refetch} title="Refresh" className="text-muted-foreground/30 hover:text-muted-foreground transition">
            <RefreshCw className={cn("w-3 h-3", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      {/* Section summary bar */}
      <SectionSummary items={items} />

      {/* Article list */}
      <div className="flex-1 overflow-y-auto divide-y divide-border/15 min-h-0">
        {loading && items.length === 0 ? (
          <div className="flex items-center justify-center h-32 gap-2 text-muted-foreground/25 text-[10px] font-mono">
            <RefreshCw className="w-3 h-3 animate-spin" />
            Loading…
          </div>
        ) : items.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground/20 text-[10px] font-mono">
            No articles yet — check back in 5 min
          </div>
        ) : (
          items.map(item => <NewsCard key={item.url} item={item} />)
        )}
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function NewsPage() {
  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* Page header */}
      <header className="border-b border-border px-5 py-3 flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-sm font-semibold text-foreground tracking-wide">News Sentiment</h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">
            FinBERT · VADER · Macro Context · 5m refresh
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5 text-[9px] font-mono text-muted-foreground/40">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400/60 inline-block" />Bullish
            <span className="w-1.5 h-1.5 rounded-full bg-red-400/60 inline-block ml-1" />Bearish
            <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/25 inline-block ml-1" />Neutral
          </div>
          <span className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/30">
            <Activity className="w-2.5 h-2.5 text-cyan-500/40" />
            Hybrid Engine
          </span>
        </div>
      </header>

      {/* 4-quadrant grid */}
      <div className="flex-1 overflow-hidden p-3 grid grid-cols-2 grid-rows-2 gap-3">
        {SECTIONS.map(s => (
          <SectionPanel key={s.key} s={s} />
        ))}
      </div>
    </div>
  );
}
