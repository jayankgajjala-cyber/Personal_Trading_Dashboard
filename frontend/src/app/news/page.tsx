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

const SECTIONS: { key: SectionKey; label: string; icon: React.ElementType; accent: string }[] = [
  { key: "indian_market",  label: "Indian Market",  icon: TrendingUp, accent: "text-cyan-400   border-cyan-500/20   bg-cyan-500/5"   },
  { key: "global_market",  label: "Global Market",  icon: Globe,      accent: "text-blue-400   border-blue-500/20   bg-blue-500/5"   },
  { key: "macro_impact",   label: "Macro Impact",   icon: BarChart2,  accent: "text-violet-400 border-violet-500/20 bg-violet-500/5" },
  { key: "swing_signals",  label: "Swing Signals",  icon: Zap,        accent: "text-amber-400  border-amber-500/20  bg-amber-500/5"  },
];

// ── Time-ago helper ───────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff < 60)        return `${diff}s ago`;
  if (diff < 3600)      return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400)     return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// ── Per-section feed hook ─────────────────────────────────────────────────────

function useNewsFeed(section: SectionKey) {
  const [items, setItems]     = useState<NewsItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastFetch, setLastFetch] = useState<number>(0);

  const fetch = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const res = await api.get<NewsItem[]>(`/news/feed?section=${section}&limit=8`);
      if (res.data?.length) setItems(res.data);
      setLastFetch(Date.now());
    } catch {
      // Silent — keep stale data visible (optimistic UI)
    } finally {
      setLoading(false);
    }
  }, [section]);

  // Initial load
  useEffect(() => { fetch(false); }, [fetch]);

  // 5-minute auto-refresh
  useEffect(() => {
    const id = setInterval(() => fetch(true), 5 * 60 * 1000);
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
      className="group flex flex-col gap-1 p-2.5 rounded-md hover:bg-muted/40 transition border border-transparent hover:border-border"
    >
      {/* Title */}
      <p className="text-[11px] font-medium text-foreground leading-snug line-clamp-2 group-hover:text-cyan-400 transition">
        {item.title}
        <ExternalLink className="inline w-2.5 h-2.5 ml-1 opacity-0 group-hover:opacity-60 transition" />
      </p>

      {/* Summary */}
      {item.summary && (
        <p className="text-[10px] text-muted-foreground leading-relaxed line-clamp-2">
          {item.summary}
        </p>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between mt-0.5">
        <span className="text-[9px] font-mono text-muted-foreground/60 truncate max-w-[120px]">
          {item.source}
        </span>
        <div className="flex items-center gap-1">
          {/* Ticker badges */}
          {item.tickers.slice(0, 2).map(t => (
            <span key={t} className="text-[8px] font-mono bg-muted/60 text-muted-foreground px-1 py-0.5 rounded">
              {t}
            </span>
          ))}
          <span className="flex items-center gap-0.5 text-[9px] font-mono text-muted-foreground/50">
            <Clock className="w-2 h-2" />
            {timeAgo(item.published_at)}
          </span>
        </div>
      </div>
    </a>
  );
}

// ── Section panel ─────────────────────────────────────────────────────────────

function SectionPanel({
  section,
}: {
  section: (typeof SECTIONS)[number];
}) {
  const { items, loading, lastFetch, refetch } = useNewsFeed(section.key);
  const Icon = section.icon;
  const [accentColor, borderColor, bgColor] = section.accent.split("   ");

  return (
    <div className={cn(
      "flex flex-col border rounded-lg overflow-hidden",
      borderColor, bgColor,
    )}>
      {/* Panel header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border/60">
        <div className="flex items-center gap-2">
          <Icon className={cn("w-3.5 h-3.5", accentColor)} />
          <span className={cn("text-[10px] font-mono font-semibold uppercase tracking-wider", accentColor)}>
            {section.label}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {lastFetch > 0 && (
            <span className="text-[9px] font-mono text-muted-foreground/40">
              {timeAgo(new Date(lastFetch).toISOString())}
            </span>
          )}
          <button
            onClick={refetch}
            title="Refresh"
            className="text-muted-foreground/40 hover:text-muted-foreground transition"
          >
            <RefreshCw className={cn("w-3 h-3", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      {/* News list */}
      <div className="flex-1 overflow-y-auto divide-y divide-border/30 min-h-0">
        {loading && items.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground/40 text-[10px] font-mono gap-2">
            <RefreshCw className="w-3 h-3 animate-spin" />
            Loading…
          </div>
        ) : items.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground/30 text-[10px] font-mono">
            No articles yet
          </div>
        ) : (
          items.map(item => (
            <NewsCard key={item.url} item={item} />
          ))
        )}
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function NewsPage() {
  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Header */}
      <header className="border-b border-border px-5 py-3 flex items-center justify-between flex-shrink-0">
        <div>
          <h1 className="text-sm font-semibold text-foreground tracking-wide">News</h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">
            4-Section Feed · Auto-refresh 5m
          </p>
        </div>
        <span className="text-[10px] font-mono text-muted-foreground/40 flex items-center gap-1">
          <Zap className="w-2.5 h-2.5 text-cyan-500/50" />
          5m refresh
        </span>
      </header>

      {/* 4-quadrant grid */}
      <div className="flex-1 overflow-hidden p-3 grid grid-cols-2 grid-rows-2 gap-3">
        {SECTIONS.map(section => (
          <SectionPanel key={section.key} section={section} />
        ))}
      </div>
    </div>
  );
}
