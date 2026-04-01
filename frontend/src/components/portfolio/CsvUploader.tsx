"use client";

import { useState, useRef } from "react";
import { holdingsApi } from "@/lib/api";
import { Upload, FileText, CheckCircle, XCircle, Loader2, X } from "lucide-react";

interface Props {
  open: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

export function CsvUploader({ open, onClose, onSuccess }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<{ added: number; updated: number; message: string } | null>(null);
  const [error, setError] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f?.name.endsWith(".csv")) { setFile(f); setError(""); }
    else setError("Only .csv files accepted");
  }

  async function handleUpload() {
    if (!file) return;
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const res = await holdingsApi.uploadCsv(file);
      setResult(res.data);
      onSuccess();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Upload failed");
    } finally {
      setLoading(false);
    }
  }

  function reset() {
    setFile(null);
    setResult(null);
    setError("");
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-lg w-full max-w-md animate-fade-in">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div>
            <h2 className="text-sm font-semibold text-foreground">Import Zerodha CSV</h2>
            <p className="text-[11px] text-muted-foreground font-mono">
              Maps: Instrument → Symbol · Avg. cost · Qty.
            </p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          {!result ? (
            <>
              {/* Drop zone */}
              <div
                onDrop={handleDrop}
                onDragOver={(e) => e.preventDefault()}
                onClick={() => inputRef.current?.click()}
                className="border-2 border-dashed border-border hover:border-cyan-500/50 rounded-lg p-8 text-center cursor-pointer transition group"
              >
                <input
                  ref={inputRef}
                  type="file"
                  accept=".csv"
                  className="hidden"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    if (f) { setFile(f); setError(""); }
                  }}
                />
                {file ? (
                  <div className="flex items-center justify-center gap-2 text-sm text-foreground font-mono">
                    <FileText className="w-4 h-4 text-cyan-400" />
                    {file.name}
                  </div>
                ) : (
                  <>
                    <Upload className="w-8 h-8 text-muted-foreground group-hover:text-cyan-400 mx-auto mb-2 transition" />
                    <p className="text-sm text-muted-foreground font-mono">
                      Drop CSV or click to browse
                    </p>
                    <p className="text-[10px] text-muted-foreground/50 font-mono mt-1">
                      Zerodha Holdings export format
                    </p>
                  </>
                )}
              </div>

              {/* Column guide */}
              <div className="bg-muted/30 border border-border rounded-md p-3 font-mono text-[10px] space-y-1">
                <p className="text-muted-foreground uppercase tracking-wider mb-1.5">Required CSV columns</p>
                {[
                  ["Instrument", "Stock symbol"],
                  ["Qty.", "Number of shares"],
                  ["Avg. cost", "Average buy price"],
                ].map(([col, desc]) => (
                  <div key={col} className="flex justify-between">
                    <span className="text-cyan-400">{col}</span>
                    <span className="text-muted-foreground">{desc}</span>
                  </div>
                ))}
              </div>

              {error && (
                <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">
                  {error}
                </p>
              )}

              <div className="flex gap-2">
                {file && (
                  <button
                    onClick={reset}
                    className="border border-border text-muted-foreground hover:text-foreground text-sm rounded-md px-4 py-2 transition font-mono"
                  >
                    Clear
                  </button>
                )}
                <button
                  onClick={handleUpload}
                  disabled={!file || loading}
                  className="flex-1 bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2 transition flex items-center justify-center gap-2 disabled:opacity-50 font-mono"
                >
                  {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Upload className="w-3.5 h-3.5" />}
                  {loading ? "Importing…" : "Import Holdings"}
                </button>
              </div>
            </>
          ) : (
            <div className="space-y-4">
              <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-md p-4 text-center">
                <CheckCircle className="w-8 h-8 text-emerald-400 mx-auto mb-2" />
                <p className="text-sm font-semibold text-foreground">{result.message}</p>
                <div className="flex justify-center gap-6 mt-3 font-mono text-xs">
                  <div>
                    <div className="text-emerald-400 text-lg font-bold">{result.added}</div>
                    <div className="text-muted-foreground">Added</div>
                  </div>
                  <div>
                    <div className="text-cyan-400 text-lg font-bold">{result.updated}</div>
                    <div className="text-muted-foreground">Updated</div>
                  </div>
                </div>
              </div>
              <button
                onClick={onClose}
                className="w-full bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2 transition font-mono"
              >
                Done
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
