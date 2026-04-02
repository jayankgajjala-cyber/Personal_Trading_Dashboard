"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { holdingsApi, Holding } from "@/lib/api";

// Base interval 15 s + up to 3 s of random jitter per tick.
// Jitter desynchronises concurrent users so they don't hammer the
// backend (and Yahoo Finance) at the exact same millisecond.
const POLL_BASE_MS  = 15_000;
const POLL_JITTER_MS = 3_000;

function jitteredInterval(): number {
  return POLL_BASE_MS + Math.random() * POLL_JITTER_MS;
}

export function useHoldings() {
  const [holdings, setHoldings]   = useState<Holding[]>([]);
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState<string | null>(null);
  const [ltpReady, setLtpReady]   = useState(false);

  const prevLtpRef = useRef<Record<string, number | null>>({});
  const [flashMap, setFlashMap]   = useState<Record<string, "up" | "down" | null>>({});

  // Stable reference to the current timeout id so we can reschedule after each tick
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetch = useCallback(async (silent = false) => {
    try {
      if (!silent) setLoading(true);

      const res  = await holdingsApi.list();
      const next: Holding[] = res.data;

      // Flash direction: diff against previous LTP snapshot
      const newFlash: Record<string, "up" | "down" | null> = {};
      next.forEach((h) => {
        const prev = prevLtpRef.current[h.symbol];
        // Only flash when ltp > 0 (0.0 sentinel = price unavailable)
        if (prev != null && h.ltp != null && h.ltp > 0) {
          if      (h.ltp > prev) newFlash[h.symbol] = "up";
          else if (h.ltp < prev) newFlash[h.symbol] = "down";
        }
        prevLtpRef.current[h.symbol] = h.ltp ?? null;
      });

      setHoldings(next);
      setError(null);

      // ltpReady once at least one holding has a real (> 0) price
      setLtpReady(next.some((h) => h.ltp != null && h.ltp > 0));

      if (Object.keys(newFlash).length > 0) {
        setFlashMap(newFlash);
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

  // Jittered polling: each tick reschedules itself with a fresh random delay
  // so no two clients stay in lockstep after the first interval.
  useEffect(() => {
    let cancelled = false;

    function scheduleTick() {
      const delay = jitteredInterval();
      timerRef.current = setTimeout(async () => {
        if (cancelled) return;
        await fetch(true);
        if (!cancelled) scheduleTick();   // reschedule with new jitter
      }, delay);
    }

    scheduleTick();

    return () => {
      cancelled = true;
      if (timerRef.current != null) clearTimeout(timerRef.current);
    };
  }, [fetch]);

  const totalInvested = holdings.reduce((s, h) => s + h.invested_amount, 0);
  const totalValue    = holdings.reduce((s, h) => s + (h.current_value ?? 0), 0);
  const totalPnl      = holdings.reduce((s, h) => s + (h.pnl ?? 0), 0);
  const totalPnlPct   = totalInvested > 0 ? (totalPnl / totalInvested) * 100 : 0;

  return {
    holdings,
    loading,
    error,
    ltpReady,
    refetch:  () => fetch(false),
    flashMap,
    totalInvested,
    totalValue,
    totalPnl,
    totalPnlPct,
  };
}
