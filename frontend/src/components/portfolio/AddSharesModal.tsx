"use client";

import { useState } from "react";
import { holdingsApi, Holding } from "@/lib/api";
import { X, Loader2, ArrowRight } from "lucide-react";
import { fmtCurrency } from "@/lib/utils";

interface Props {
  holding: Holding | null;
  onClose: () => void;
  onSuccess: () => void;
}

export function AddSharesModal({ holding, onClose, onSuccess }: Props) {
  const [qty, setQty] = useState("");
  const [price, setPrice] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  if (!holding) return null;

  const addQty = parseFloat(qty) || 0;
  const buyPrice = parseFloat(price) || 0;
  const newQty = holding.quantity + addQty;
  const newAvg = addQty > 0 && buyPrice > 0
    ? ((holding.quantity * holding.average_buy_price) + (addQty * buyPrice)) / newQty
    : holding.average_buy_price;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError("");
    try {
      await holdingsApi.addShares(holding!.symbol, addQty, buyPrice);
      onSuccess();
      onClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to update holding");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-lg w-full max-w-sm animate-fade-in">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div>
            <h2 className="text-sm font-semibold text-foreground">
              Add Shares · <span className="text-cyan-400 font-mono">{holding.symbol}</span>
            </h2>
            <p className="text-[11px] text-muted-foreground font-mono">Weighted average recalculation</p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {/* Current state */}
          <div className="bg-muted/40 border border-border rounded-md p-3 font-mono text-xs space-y-1">
            <div className="flex justify-between text-muted-foreground">
              <span>Current Qty</span>
              <span className="text-foreground">{holding.quantity}</span>
            </div>
            <div className="flex justify-between text-muted-foreground">
              <span>Current Avg</span>
              <span className="text-foreground">{fmtCurrency(holding.average_buy_price)}</span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground font-mono uppercase tracking-wider">
                Add Qty <span className="text-cyan-500">*</span>
              </label>
              <input
                type="number"
                step="0.0001"
                min="0.0001"
                value={qty}
                onChange={(e) => { setQty(e.target.value); setError(""); }}
                placeholder="5"
                required
                className="w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition"
              />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground font-mono uppercase tracking-wider">
                Buy Price (₹) <span className="text-cyan-500">*</span>
              </label>
              <input
                type="number"
                step="0.01"
                min="0.01"
                value={price}
                onChange={(e) => { setPrice(e.target.value); setError(""); }}
                placeholder="2600.00"
                required
                className="w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition"
              />
            </div>
          </div>

          {/* Preview new state */}
          {addQty > 0 && buyPrice > 0 && (
            <div className="bg-cyan-500/5 border border-cyan-500/20 rounded-md p-3 font-mono text-xs space-y-1">
              <p className="text-cyan-400 text-[10px] uppercase tracking-wider mb-2">After update</p>
              <div className="flex justify-between text-muted-foreground">
                <span>New Qty</span>
                <span className="text-foreground">{newQty.toFixed(4)}</span>
              </div>
              <div className="flex justify-between text-muted-foreground">
                <span>New Avg</span>
                <span className="text-emerald-400">{fmtCurrency(newAvg)}</span>
              </div>
              <div className="flex justify-between text-muted-foreground">
                <span>Total Invested</span>
                <span className="text-foreground">{fmtCurrency(newQty * newAvg)}</span>
              </div>
            </div>
          )}

          {error && (
            <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">
              {error}
            </p>
          )}

          <div className="flex gap-2 pt-1">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 border border-border text-muted-foreground hover:text-foreground text-sm rounded-md py-2 transition font-mono"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="flex-1 bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2 transition flex items-center justify-center gap-2 disabled:opacity-50 font-mono"
            >
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <ArrowRight className="w-3.5 h-3.5" />}
              {loading ? "Updating…" : "Update"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
