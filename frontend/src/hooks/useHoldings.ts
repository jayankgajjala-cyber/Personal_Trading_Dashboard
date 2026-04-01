"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { holdingsApi, Holding } from "@/lib/api";

const POLL_INTERVAL_MS = 10_000;

export function useHoldings() {
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Track previous LTPs so flash logic can diff on each poll
  const prevLtpRef = useRef<Record<string, number | null>>({});
  const [flashMap, setFlashMap] = useState<Record<string, "up" | "down" | null>>({});

  const fetch = useCallback(async (silent = false) => {
    try {
      if (!silent) setLoading(true);
      const res = await holdingsApi.list();
      const next = res.data;

      // Compute flash direction per symbol
      const newFlash: Record<string, "up" | "down" | null> = {};
      next.forEach((h) => {
        const prev = prevLtpRef.current[h.symbol];
        if (prev != null && h.ltp != null) {
          if (h.ltp > prev) newFlash[h.symbol] = "up";
          else if (h.ltp < prev) newFlash[h.symbol] = "down";
        }
        prevLtpRef.current[h.symbol] = h.ltp ?? null;
      });

      setHoldings(next);
      setError(null);

      if (Object.keys(newFlash).length > 0) {
        setFlashMap(newFlash);
        // Clear flash after 800ms
        setTimeout(() => setFlashMap({}), 800);
      }
    } catch (e: any) {
      setError(e.response?.data?.detail || "Failed to load holdings");
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    fetch(false);
  }, [fetch]);

  // 10-second polling (silent — no loading spinner flicker)
  useEffect(() => {
    const id = setInterval(() => fetch(true), POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [fetch]);

  const totalInvested = holdings.reduce((s, h) => s + h.invested_amount, 0);
  const totalValue = holdings.reduce((s, h) => s + (h.current_value ?? h.invested_amount), 0);
  const totalPnl = holdings.reduce((s, h) => s + (h.pnl ?? 0), 0);
  const totalPnlPct = totalInvested > 0 ? (totalPnl / totalInvested) * 100 : 0;

  return {
    holdings,
    loading,
    error,
    refetch: () => fetch(false),
    flashMap,
    totalInvested,
    totalValue,
    totalPnl,
    totalPnlPct,
  };
}
