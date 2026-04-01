"use client";

import { useState } from "react";
import { holdingsApi, Holding } from "@/lib/api";
import { X, Loader2, TrendingDown } from "lucide-react";
import { fmtCurrency } from "@/lib/utils";

interface Props {
  holding: Holding | null;
  onClose: () => void;
  onSuccess: () => void;
}

export function SellSharesModal({ holding, onClose, onSuccess }: Props) {
  const [qty, setQty] = useState("");
  const [price, setPrice] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  if (!holding) return null;

  const sellQty = parseFloat(qty) || 0;
  const sellPrice = parseFloat(price) || 0;
  const remainingQty = holding.quantity - sellQty;
  const isFullSell = sellQty === holding.quantity;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (sellQty > holding!.quantity) {
      setError(`Cannot sell more than ${holding!.quantity} shares held.`);
      return;
    }
    setLoading(true);
    setError("");
    try {
      await holdingsApi.sellShares(holding!.symbol, sellQty, sellPrice);
      onSuccess();
      onClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to process sell");
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
              Sell Shares · <span className="text-rose-400 font-mono">{holding.symbol}</span>
            </h2>
            <p className="text-[11px] text-muted-foreground font-mono">Reduce position</p>
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
              <span>Avg Buy Price</span>
              <span className="text-foreground">{fmtCurrency(holding.average_buy_price)}</span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground font-mono uppercase tracking-wider">
                Sell Qty <span className="text-rose-500">*</span>
              </label>
              <input
                type="number"
                step="0.0001"
                min="0.0001"
                max={holding.quantity}
                value={qty}
                onChange={(e) => { setQty(e.target.value); setError(""); }}
                placeholder={String(holding.quantity)}
                required
                className="w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-rose-500 focus:border-rose-500 transition"
              />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground font-mono uppercase tracking-wider">
                Sell Price (₹) <span className="text-rose-500">*</span>
              </label>
              <input
                type="number"
                step="0.01"
                min="0.01"
                value={price}
                onChange={(e) => { setPrice(e.target.value); setError(""); }}
                placeholder="2600.00"
                required
                className="w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-rose-500 focus:border-rose-500 transition"
              />
            </div>
          </div>

          {/* Preview */}
          {sellQty > 0 && sellPrice > 0 && (
            <div className="bg-rose-500/5 border border-rose-500/20 rounded-md p-3 font-mono text-xs space-y-1">
              <p className="text-rose-400 text-[10px] uppercase tracking-wider mb-2">After sell</p>
              <div className="flex justify-between text-muted-foreground">
                <span>Proceeds</span>
                <span className="text-foreground">{fmtCurrency(sellQty * sellPrice)}</span>
              </div>
              {isFullSell ? (
                <div className="flex justify-between text-muted-foreground">
                  <span>Remaining Qty</span>
                  <span className="text-rose-400">Position closed</span>
                </div>
              ) : (
                <div className="flex justify-between text-muted-foreground">
                  <span>Remaining Qty</span>
                  <span className="text-foreground">{remainingQty.toFixed(4)}</span>
                </div>
              )}
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
              className="flex-1 bg-rose-500 hover:bg-rose-400 text-white font-semibold text-sm rounded-md py-2 transition flex items-center justify-center gap-2 disabled:opacity-50 font-mono"
            >
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <TrendingDown className="w-3.5 h-3.5" />}
              {loading ? "Processing…" : "Confirm Sell"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
