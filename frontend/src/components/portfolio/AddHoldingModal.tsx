"use client";

import { useState } from "react";
import { holdingsApi } from "@/lib/api";
import { cn } from "@/lib/utils";
import { X, Loader2 } from "lucide-react";

interface Props { open: boolean; onClose: () => void; onSuccess: () => void; }

const EXCHANGES = ["NSE", "BSE", "US", "CRYPTO"];

export function AddHoldingModal({ open, onClose, onSuccess }: Props) {
  const [form, setForm] = useState({
    symbol: "", stock_name: "", quantity: "", average_buy_price: "", exchange: "NSE",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  function set(key: string, val: string) {
    setForm((f) => ({ ...f, [key]: val }));
    setError("");
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true); setError("");
    try {
      await holdingsApi.create({
        symbol: form.symbol.toUpperCase(),
        stock_name: form.stock_name,
        quantity: parseFloat(form.quantity),
        average_buy_price: parseFloat(form.average_buy_price),
        exchange: form.exchange,
      });
      setForm({ symbol: "", stock_name: "", quantity: "", average_buy_price: "", exchange: "NSE" });
      onSuccess(); onClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to add holding");
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-lg w-full max-w-md animate-fade-in">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div>
            <h2 className="text-sm font-semibold text-foreground">Add Holding</h2>
            <p className="text-[11px] text-muted-foreground font-mono">New position entry</p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition"><X className="w-4 h-4" /></button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          <div className="grid grid-cols-2 gap-3">
            <Field label="Symbol" required>
              <input value={form.symbol} onChange={(e) => set("symbol", e.target.value.toUpperCase())}
                placeholder="RELIANCE" required className={inputCls} />
            </Field>
            <Field label="Exchange" required>
              <select value={form.exchange} onChange={(e) => set("exchange", e.target.value)} className={inputCls}>
                {EXCHANGES.map(ex => <option key={ex} value={ex}>{ex}</option>)}
              </select>
            </Field>
          </div>
          <Field label="Stock Name" required>
            <input value={form.stock_name} onChange={(e) => set("stock_name", e.target.value)}
              placeholder="Reliance Industries" required className={inputCls} />
          </Field>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Quantity" required>
              <input type="number" step="0.0001" min="0.0001" value={form.quantity}
                onChange={(e) => set("quantity", e.target.value)} placeholder="10" required className={inputCls} />
            </Field>
            <Field label="Avg Buy Price" required>
              <input type="number" step="0.01" min="0.01" value={form.average_buy_price}
                onChange={(e) => set("average_buy_price", e.target.value)} placeholder="2450.00" required className={inputCls} />
            </Field>
          </div>

          {form.quantity && form.average_buy_price && (
            <div className="bg-muted/50 border border-border rounded px-3 py-2 font-mono text-xs text-muted-foreground flex justify-between">
              <span>Invested Amount</span>
              <span className="text-foreground">
                {form.exchange === "US" ? "$" : "₹"}
                {(parseFloat(form.quantity || "0") * parseFloat(form.average_buy_price || "0"))
                  .toLocaleString(form.exchange === "US" ? "en-US" : "en-IN", { maximumFractionDigits: 2 })}
              </span>
            </div>
          )}

          {error && <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">{error}</p>}

          <div className="flex gap-2 pt-1">
            <button type="button" onClick={onClose}
              className="flex-1 border border-border text-muted-foreground hover:text-foreground text-sm rounded-md py-2 transition font-mono">Cancel</button>
            <button type="submit" disabled={loading}
              className="flex-1 bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2 transition flex items-center justify-center gap-2 disabled:opacity-50 font-mono">
              {loading && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
              {loading ? "Adding…" : "Add Position"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function Field({ label, children, required }: { label: string; children: React.ReactNode; required?: boolean }) {
  return (
    <div className="space-y-1">
      <label className="text-[10px] text-muted-foreground font-mono uppercase tracking-wider">
        {label}{required && <span className="text-cyan-500 ml-0.5">*</span>}
      </label>
      {children}
    </div>
  );
}

const inputCls = "w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition";
