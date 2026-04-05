"use client";

import { useState } from "react";
import {
  Plus, Upload, RefreshCw, Trash2, PlusCircle,
  TrendingUp, TrendingDown, Activity, Wallet,
  BarChart2, Cpu, Zap,
} from "lucide-react";
import { useHoldings } from "@/hooks/useHoldings";
import { holdingsApi, Holding } from "@/lib/api";
import { cn, fmtCurrency, fmt, fmtPct, fmtCompact } from "@/lib/utils";
import { AddHoldingModal } from "@/components/portfolio/AddHoldingModal";
import { AddSharesModal } from "@/components/portfolio/AddSharesModal";
import { SellSharesModal } from "@/components/portfolio/SellSharesModal";
import { CsvUploader } from "@/components/portfolio/CsvUploader";

type SortKey = keyof Pick<Holding, "symbol" | "quantity" | "average_buy_price" | "invested_amount">;

export default function DashboardPage() {
  const {
    holdings, loading, error, refetch, flashMap,
    totalInvested, totalValue, totalPnl, totalPnlPct,
    ltpReady,
  } = useHoldings();

  const [addOpen, setAddOpen] = useState(false);
  const [csvOpen, setCsvOpen] = useState(false);
  const [editHolding, setEditHolding] = useState<Holding | null>(null);
  const [sellHolding, setSellHolding] = useState<Holding | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortAsc, setSortAsc] = useState(true);
  const [deleting, setDeleting] = useState<string | null>(null);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortAsc((a) => !a);
    else { setSortKey(key); setSortAsc(true); }
  }

  const sorted = [...holdings].sort((a, b) => {
    const va = a[sortKey], vb = b[sortKey];
    const cmp = typeof va === "string" ? va.localeCompare(vb as string) : (va as number) - (vb as number);
    return sortAsc ? cmp : -cmp;
  });

  async function handleDelete(symbol: string) {
    if (!confirm(`Remove ${symbol} from portfolio?`)) return;
    setDeleting(symbol);
    try {
      await holdingsApi.delete(symbol);
      await refetch();
    } finally {
      setDeleting(null);
    }
  }

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey === k
      ? <span className="text-cyan-400 ml-0.5">{sortAsc ? "↑" : "↓"}</span>
      : <span className="text-muted-foreground/30 ml-0.5">↕</span>;

  const pnlPositive = totalPnl >= 0;

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* ── Top bar ── */}
      <header className="border-b border-border px-5 py-3 flex items-center justify-between gap-4 flex-shrink-0">
        <div>
          <h1 className="text-sm font-semibold text-foreground tracking-wide">Portfolio</h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">
            Holdings · Live View
          </p>
        </div>

        {/* Stats strip */}
        <div className="hidden md:flex items-center gap-4">
          <Stat icon={<Wallet className="w-3.5 h-3.5" />} label="Invested" value={fmtCompact(totalInvested)} />
          <Stat icon={<BarChart2 className="w-3.5 h-3.5" />} label="Curr Value" value={ltpReady ? fmtCompact(totalValue) : "—"} />
          <Stat
            icon={pnlPositive ? <TrendingUp className="w-3.5 h-3.5" /> : <TrendingDown className="w-3.5 h-3.5" />}
            label="Total P&L"
            value={ltpReady ? `${pnlPositive ? "+" : ""}${fmtCompact(Math.abs(totalPnl))} (${fmtPct(totalPnlPct)})` : "—"}
            color={ltpReady ? (pnlPositive ? "text-emerald-400" : "text-rose-400") : undefined}
          />
          <Stat icon={<Activity className="w-3.5 h-3.5" />} label="Positions" value={String(holdings.length)} />
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/50 mr-1">
            <Zap className="w-2.5 h-2.5 text-cyan-500/60" />
            15s+j
          </div>
          <button
            onClick={refetch}
            className="w-8 h-8 rounded-md border border-border flex items-center justify-center text-muted-foreground hover:text-foreground hover:bg-muted transition"
            title="Refresh now"
          >
            <RefreshCw className={cn("w-3.5 h-3.5", loading && "animate-spin")} />
          </button>
          <button
            onClick={() => setCsvOpen(true)}
            className="flex items-center gap-1.5 border border-border text-muted-foreground hover:text-foreground hover:bg-muted text-xs font-mono rounded-md px-3 py-1.5 transition"
          >
            <Upload className="w-3.5 h-3.5" />
            Import CSV
          </button>
          <button
            onClick={() => setAddOpen(true)}
            className="flex items-center gap-1.5 bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-xs font-mono rounded-md px-3 py-1.5 transition"
          >
            <Plus className="w-3.5 h-3.5" />
            Add Position
          </button>
        </div>
      </header>

      {/* ── Table ── */}
      <div className="flex-1 overflow-auto relative">
        {/* Optimistic refresh indicator — table stays visible, subtle top bar pulses */}
        {loading && holdings.length > 0 && (
          <div className="absolute top-0 left-0 right-0 h-0.5 bg-cyan-500/40 animate-pulse z-20" />
        )}

        {error && (
          <div className="m-4 bg-rose-500/10 border border-rose-500/20 rounded-md px-4 py-3 text-rose-400 text-xs font-mono">
            {error}
          </div>
        )}

        {loading && holdings.length === 0 ? (
          <div className="flex items-center justify-center h-48 text-muted-foreground text-xs font-mono gap-2">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" />
            Loading portfolio…
          </div>
        ) : (
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b border-border bg-card/60 sticky top-0 z-10">
                {[
                  { key: "symbol",            label: "Symbol"     },
                  { key: null,                label: "Name"       },
                  { key: "quantity",          label: "Qty",        align: "right" },
                  { key: "average_buy_price", label: "Avg Cost",   align: "right" },
                  { key: null,                label: "LTP",        align: "right" },
                  { key: "invested_amount",   label: "Invested",   align: "right" },
                  { key: null,                label: "Curr Value", align: "right" },
                  { key: null,                label: "P&L",        align: "right" },
                  { key: null,                label: "P&L %",      align: "right" },
                  { key: null,                label: "Source",     align: "center" },
                  { key: null,                label: "",           align: "right" },
                ].map(({ key, label, align }, i) => (
                  <th
                    key={i}
                    onClick={key ? () => toggleSort(key as SortKey) : undefined}
                    className={cn(
                      "px-3 py-2.5 font-mono text-[10px] text-muted-foreground uppercase tracking-wider whitespace-nowrap select-none",
                      align === "right" ? "text-right" : align === "center" ? "text-center" : "text-left",
                      key && "cursor-pointer hover:text-foreground transition"
                    )}
                  >
                    {label}
                    {key && <SortIcon k={key as SortKey} />}
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {sorted.length === 0 ? (
                <tr>
                  <td colSpan={11} className="text-center py-16 text-muted-foreground font-mono text-xs">
                    <div className="flex flex-col items-center gap-3">
                      <BarChart2 className="w-8 h-8 opacity-20" />
                      <div>No holdings yet.</div>
                      <div className="flex gap-2">
                        <button onClick={() => setAddOpen(true)} className="text-cyan-400 hover:underline">Add manually</button>
                        <span className="text-muted-foreground/30">or</span>
                        <button onClick={() => setCsvOpen(true)} className="text-cyan-400 hover:underline">import CSV</button>
                      </div>
                    </div>
                  </td>
                </tr>
              ) : (
                sorted.map((h, idx) => (
                  <HoldingRow
                    key={h.id}
                    holding={h}
                    idx={idx}
                    flash={flashMap[h.symbol] ?? null}
                    onAddShares={() => setEditHolding(h)}
                    onSell={() => setSellHolding(h)}
                    onDelete={() => handleDelete(h.symbol)}
                    isDeleting={deleting === h.symbol}
                  />
                ))
              )}
            </tbody>

            {sorted.length > 0 && (
              <tfoot>
                <tr className="border-t border-border bg-card/40">
                  <td colSpan={3} className="px-3 py-2.5 font-mono text-[10px] text-muted-foreground uppercase tracking-wider">
                    Total · {holdings.length} positions
                  </td>
                  <td /><td />
                  <td className="px-3 py-2.5 text-right font-mono font-semibold text-foreground tabular-nums">
                    {fmtCompact(totalInvested)}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono font-semibold text-foreground tabular-nums">
                    {ltpReady ? fmtCompact(totalValue) : "—"}
                  </td>
                  <td className={cn("px-3 py-2.5 text-right font-mono font-semibold tabular-nums",
                    ltpReady ? (totalPnl >= 0 ? "text-emerald-400" : "text-rose-400") : "text-muted-foreground/40")}>
                    {ltpReady ? (totalPnl >= 0 ? "+" : "") + fmtCompact(Math.abs(totalPnl)) : "—"}
                  </td>
                  <td className={cn("px-3 py-2.5 text-right font-mono font-semibold tabular-nums",
                    ltpReady ? (totalPnlPct >= 0 ? "text-emerald-400" : "text-rose-400") : "text-muted-foreground/40")}>
                    {ltpReady ? fmtPct(totalPnlPct) : "—"}
                  </td>
                  <td colSpan={2} />
                </tr>
              </tfoot>
            )}
          </table>
        )}
      </div>

      <AddHoldingModal open={addOpen} onClose={() => setAddOpen(false)} onSuccess={refetch} />
      <AddSharesModal holding={editHolding} onClose={() => setEditHolding(null)} onSuccess={refetch} />
      <SellSharesModal holding={sellHolding} onClose={() => setSellHolding(null)} onSuccess={refetch} />
      <CsvUploader open={csvOpen} onClose={() => setCsvOpen(false)} onSuccess={refetch} />
    </div>
  );
}

// ── HoldingRow ────────────────────────────────────────────────────────────────

function HoldingRow({
  holding: h, idx, flash, onAddShares, onSell, onDelete, isDeleting,
}: {
  holding: Holding;
  idx: number;
  flash: "up" | "down" | null;
  onAddShares: () => void;
  onSell: () => void;
  onDelete: () => void;
  isDeleting: boolean;
}) {
  const pnlPositive = (h.pnl ?? 0) >= 0;

  return (
    <tr
      className={cn(
        "border-b border-border/50 hover:bg-muted/30 transition-colors group",
        idx % 2 === 0 ? "bg-transparent" : "bg-card/20",
        flash === "up" && "flash-up",
        flash === "down" && "flash-down",
      )}
    >
      {/* Symbol */}
      <td className="px-3 py-2">
        <div className="flex flex-col">
          <span className="font-mono font-semibold text-cyan-400 text-xs">{h.symbol}</span>
          <span className="text-[9px] text-muted-foreground/50 font-mono">{h.exchange}</span>
        </div>
      </td>

      {/* Name */}
      <td className="px-3 py-2 text-muted-foreground max-w-[140px] truncate">{h.stock_name}</td>

      {/* Qty */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">{fmt(h.quantity, 4)}</td>

      {/* Avg Cost */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">
        {fmtCurrency(h.average_buy_price, h.exchange)}
      </td>

      {/* LTP:
            null        → "Fetching..." (backend hasn't responded yet)
            0 / 0.0     → "0.00" (backend responded but price unavailable — pipe confirmed open)
            > 0         → formatted currency                                                    */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">
        {h.ltp === null || h.ltp === undefined
          ? <span className="text-muted-foreground/40 text-[10px] animate-pulse">Fetching...</span>
          : h.ltp > 0
            ? <span className="text-foreground font-medium">{fmtCurrency(h.ltp, h.exchange)}</span>
            : <span className="text-muted-foreground/50 text-[10px]">{fmtCurrency(0, h.exchange)}</span>
        }
      </td>

      {/* Invested */}
      <td className="px-3 py-2 text-right font-mono tabular-nums text-muted-foreground">
        {fmtCurrency(h.invested_amount, h.exchange)}
      </td>

      {/* Current Value */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">
        {h.current_value != null
          ? <span className="text-foreground font-medium">{fmtCurrency(h.current_value, h.exchange)}</span>
          : <span className="text-muted-foreground/40 text-[10px]">—</span>
        }
      </td>

      {/* P&L absolute */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">
        {h.pnl != null
          ? <span className={cn(pnlPositive ? "text-emerald-400" : "text-rose-400")}>
              {h.pnl >= 0 ? "+" : ""}{fmtCurrency(h.pnl, h.exchange)}
            </span>
          : <span className="text-muted-foreground/30 text-[10px]">—</span>
        }
      </td>

      {/* P&L % */}
      <td className="px-3 py-2 text-right font-mono tabular-nums">
        {h.pnl_percent != null
          ? <span className={cn("inline-flex items-center gap-0.5",
              pnlPositive ? "text-emerald-400" : "text-rose-400")}>
              {pnlPositive
                ? <TrendingUp className="w-3 h-3" />
                : <TrendingDown className="w-3 h-3" />
              }
              {fmtPct(h.pnl_percent)}
            </span>
          : <span className="text-muted-foreground/30 text-[10px]">—</span>
        }
      </td>

      {/* Source — colored badge showing which data tier provided the price */}
      <td className="px-3 py-2 text-center">
        <SourceBadge source={h.ltp_source} />
      </td>

      {/* Actions */}
      <td className="px-3 py-2">
        <div className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition">
          <button onClick={onAddShares} title="Buy more"
            className="w-6 h-6 rounded flex items-center justify-center text-muted-foreground hover:text-cyan-400 hover:bg-cyan-500/10 transition">
            <PlusCircle className="w-3.5 h-3.5" />
          </button>
          <button onClick={onSell} title="Sell shares"
            className="w-6 h-6 rounded flex items-center justify-center text-muted-foreground hover:text-rose-400 hover:bg-rose-500/10 transition">
            <TrendingDown className="w-3.5 h-3.5" />
          </button>
          <button onClick={onDelete} disabled={isDeleting} title="Remove position"
            className="w-6 h-6 rounded flex items-center justify-center text-muted-foreground hover:text-rose-400 hover:bg-rose-500/10 transition disabled:opacity-30">
            <Trash2 className="w-3.5 h-3.5" />
          </button>
        </div>
      </td>
    </tr>
  );
}

// ── Source badge ──────────────────────────────────────────────────────────────

const SOURCE_STYLES: Record<string, string> = {
  Google:   "bg-blue-500/10   text-blue-400   border-blue-500/20",
  NSE:      "bg-orange-500/10 text-orange-400 border-orange-500/20",
  yfinance: "bg-amber-500/10  text-amber-400  border-amber-500/20",
  Finnhub:  "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
  Failed:   "bg-rose-500/10   text-rose-400   border-rose-500/20",
};

function SourceBadge({ source }: { source: string | null }) {
  if (!source || source === "Failed") {
    return (
      <span className="inline-flex items-center gap-0.5 text-muted-foreground/30 text-[10px] font-mono">
        <Cpu className="w-2.5 h-2.5" /><span>—</span>
      </span>
    );
  }
  const cls = SOURCE_STYLES[source] ?? "bg-muted/40 text-muted-foreground border-border";
  return (
    <span className={cn("border rounded px-1.5 py-0.5 text-[10px] font-mono tracking-wide", cls)}>
      {source}
    </span>
  );
}

// ── Stat chip ─────────────────────────────────────────────────────────────────

function Stat({ icon, label, value, color, dim }: {
  icon: React.ReactNode; label: string; value: string; color?: string; dim?: boolean;
}) {
  return (
    <div className="flex items-center gap-2 border-l border-border pl-4">
      <div className="text-muted-foreground">{icon}</div>
      <div>
        <div className={cn("text-xs font-mono font-medium",
          color ?? (dim ? "text-muted-foreground/50" : "text-foreground"))}>
          {value}
        </div>
        <div className="text-[9px] text-muted-foreground uppercase tracking-wider font-mono">{label}</div>
      </div>
    </div>
  );
}
