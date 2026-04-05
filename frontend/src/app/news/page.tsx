"use client";

import { useState, useEffect, useCallback } from "react";
import {
  RefreshCw, ExternalLink, TrendingUp, Globe,
  BarChart2, Zap, Clock,
} from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

// ── Types ─────────────────────────────────────────────────────────────────────

interface NewsItem {
  url:          string;
  title:        string;
  summary:      string;
  source:       string;
  section:      string;
  published_at: string;
  tickers:      string[];
  image_url:    string | null;
}

type SectionKey = "indian_market" | "global_market" | "macro_impact" | "swing_signals";

const SECTIONS: {
  key:    SectionKey;
  label:  string;
  icon:   React.ElementType;
  color:  string;   // text colour
  border: string;   // border colour
  bg:     string;   // panel tint
}[] = [
  {
    key:    "indian_market",
    label:  "Indian Market",
    icon:   TrendingUp,
    color:  "text-cyan-400",
    border: "border-cyan-500/20",
    bg:     "bg-cyan-500/[0.03]",
  },
  {
    key:    "global_market",
    label:  "Global Market",
    icon:   Globe,
    color:  "text-blue-400",
    border: "border-blue-500/20",
    bg:     "bg-blue-500/[0.03]",
  },
  {
    key:    "macro_impact",
    label:  "Macro Impact",
    icon:   BarChart2,
    color:  "text-violet-400",
    border: "border-violet-500/20",
    bg:     "bg-violet-500/[0.03]",
  },
  {
    key:    "swing_signals",
    label:  "Swing Signals",
    icon:   Zap,
    color:  "text-amber-400",
    border: "border-amber-500/20",
    bg:     "bg-amber-500/[0.03]",
  },
];

// ── Time-ago ──────────────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff <    60) return `${diff}s ago`;
  if (diff <  3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// ── Per-section data hook ─────────────────────────────────────────────────────

function useNewsFeed(section: SectionKey) {
  const [items,     setItems]     = useState<NewsItem[]>([]);
  const [loading,   setLoading]   = useState(true);
  const [lastFetch, setLastFetch] = useState(0);

  const fetch = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      // Connects to FastAPI /news/feed — backed by MongoDB news_feed collection
      const res = await api.get<NewsItem[]>(
        `/news/feed?section=${section}&limit=8`,
      );
      if (res.data?.length) {
        setItems(res.data);
        setLastFetch(Date.now());
      }
    } catch {
      // Silent — keep stale data visible (optimistic UI)
    } finally {
      setLoading(false);
    }
  }, [section]);

  // Initial load
  useEffect(() => { fetch(false); }, [fetch]);

  // 5-minute auto-refresh — desynchronised per section to avoid burst
  useEffect(() => {
    const jitter = Math.random() * 30_000;   // spread requests by up to 30 s
    const id = setInterval(() => fetch(true), 5 * 60_000 + jitter);
    return () => clearInterval(id);
  }, [fetch]);

  return { items, loading, lastFetch, refetch: () => fetch(false) };
}

// ── NewsCard ──────────────────────────────────────────────────────────────────

function NewsCard({ item }: { item: NewsItem }) {
  return (
    <a
      href={item.url}
      target="_blank"
      rel="noopener noreferrer"
      className={cn(
        "group flex flex-col gap-1 p-2.5 rounded-md transition",
        "border border-transparent hover:border-border hover:bg-muted/40",
      )}
    >
      {/* Title */}
      <p className="text-[11px] font-medium text-foreground leading-snug line-clamp-2 group-hover:text-cyan-400 transition">
        {item.title}
        <ExternalLink className="inline w-2.5 h-2.5 ml-1 opacity-0 group-hover:opacity-50 transition align-baseline" />
      </p>

      {/* Summary */}
      {item.summary && (
        <p className="text-[10px] text-muted-foreground leading-relaxed line-clamp-2">
          {item.summary}
        </p>
      )}

      {/* Footer row */}
      <div className="flex items-center justify-between mt-0.5 gap-2">
        <span className="text-[9px] font-mono text-muted-foreground/50 truncate">
          {item.source}
        </span>
        <div className="flex items-center gap-1 shrink-0">
          {/* Ticker badges (populated by AI enrichment when available) */}
          {item.tickers.slice(0, 2).map(t => (
            <span
              key={t}
              className="text-[8px] font-mono bg-muted/60 text-muted-foreground/70 px-1 py-0.5 rounded"
            >
              {t}
            </span>
          ))}
          <span className="flex items-center gap-0.5 text-[9px] font-mono text-muted-foreground/40">
            <Clock className="w-2 h-2" />
            {timeAgo(item.published_at)}
          </span>
        </div>
      </div>
    </a>
  );
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
            <span className="text-[9px] font-mono text-muted-foreground/30">
              {timeAgo(new Date(lastFetch).toISOString())}
            </span>
          )}
          <button
            onClick={refetch}
            title="Refresh section"
            className="text-muted-foreground/30 hover:text-muted-foreground transition"
          >
            <RefreshCw className={cn("w-3 h-3", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      {/* Article list */}
      <div className="flex-1 overflow-y-auto divide-y divide-border/20 min-h-0">
        {loading && items.length === 0 ? (
          <div className="flex items-center justify-center h-32 gap-2 text-muted-foreground/30 text-[10px] font-mono">
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
// This component receives its layout (sidebar + macro ticker) from
// app/news/layout.tsx → app/dashboard/layout.tsx (DashboardLayout).
// It only needs to render its own content area.

export default function NewsPage() {
  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* Page header */}
      <header className="border-b border-border px-5 py-3 flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-sm font-semibold text-foreground tracking-wide">News</h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">
            4-Section Feed · Auto-refresh 5 min
          </p>
        </div>
        <span className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/40">
          <Zap className="w-2.5 h-2.5 text-cyan-500/50" />
          5m refresh
        </span>
      </header>

      {/* 4-quadrant grid — fills remaining viewport height */}
      <div className="flex-1 overflow-hidden p-3 grid grid-cols-2 grid-rows-2 gap-3">
        {SECTIONS.map(s => (
          <SectionPanel key={s.key} s={s} />
        ))}
      </div>
    </div>
  );
}
