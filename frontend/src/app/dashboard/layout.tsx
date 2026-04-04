"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import { useRouter, usePathname } from "next/navigation";
import Cookies from "js-cookie";
import {
  TrendingUp, LayoutDashboard, LogOut,
  ChevronRight, ChevronLeft,
} from "lucide-react";
import Link from "next/link";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api";

// ── Macro ticker types ────────────────────────────────────────────────────────
interface MacroQuote {
  symbol:     string;
  label:      string;
  price:      number;
  change:     number;
  pct_change: number;
  cached:     boolean;
}

// ── Macro ticker hook ─────────────────────────────────────────────────────────
function useMacroTicker() {
  const [quotes, setQuotes] = useState<MacroQuote[]>([]);

  const fetchQuotes = useCallback(async () => {
    try {
      const res = await api.get<MacroQuote[]>("/global/macro");
      if (res.data?.length) setQuotes(res.data);
    } catch {
      // Silent — ticker is non-critical
    }
  }, []);

  useEffect(() => {
    fetchQuotes();
    // Refresh every 10 minutes (matches backend cache TTL)
    const id = setInterval(fetchQuotes, 10 * 60 * 1000);
    return () => clearInterval(id);
  }, [fetchQuotes]);

  return quotes;
}

// ── MacroTicker component ─────────────────────────────────────────────────────
const HIDDEN_PATHS = ["/settings", "/diagnostics"];

function MacroTicker({ quotes }: { quotes: MacroQuote[] }) {
  const pathname  = usePathname();
  const trackRef  = useRef<HTMLDivElement>(null);

  if (HIDDEN_PATHS.some(p => pathname?.startsWith(p))) return null;
  if (!quotes.length) return null;

  // Duplicate items so the scroll loop is seamless
  const items = [...quotes, ...quotes];

  return (
    <div className="w-full h-7 bg-card/80 border-b border-border overflow-hidden flex items-center relative shrink-0">
      {/* Left fade */}
      <div className="absolute left-0 top-0 h-full w-8 bg-gradient-to-r from-card/80 to-transparent z-10 pointer-events-none" />
      {/* Right fade */}
      <div className="absolute right-0 top-0 h-full w-8 bg-gradient-to-l from-card/80 to-transparent z-10 pointer-events-none" />

      <div
        ref={trackRef}
        className="flex items-center gap-0 animate-ticker whitespace-nowrap"
        style={{ willChange: "transform" }}
      >
        {items.map((q, i) => {
          const up  = q.pct_change > 0;
          const dn  = q.pct_change < 0;
          const neu = q.pct_change === 0;
          return (
            <span
              key={`${q.symbol}-${i}`}
              className="inline-flex items-center gap-1.5 px-4 border-r border-border/40 h-7"
            >
              <span className="text-[10px] font-mono font-semibold text-foreground/70 tracking-wide">
                {q.label}
              </span>
              <span className="text-[10px] font-mono tabular-nums text-foreground">
                {q.price > 0 ? q.price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "—"}
              </span>
              {q.price > 0 && (
                <span
                  className={cn(
                    "text-[10px] font-mono tabular-nums",
                    up  && "text-emerald-400",
                    dn  && "text-rose-400",
                    neu && "text-muted-foreground/50",
                  )}
                >
                  {up ? "+" : ""}{q.pct_change.toFixed(2)}%
                </span>
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
}

// ── Dashboard layout ──────────────────────────────────────────────────────────
export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const router   = useRouter();
  const pathname = usePathname();
  const quotes   = useMacroTicker();
  const [collapsed, setCollapsed] = useState(true);

  useEffect(() => {
    const token = Cookies.get("qe_token");
    if (!token) router.push("/auth/login");
  }, [router]);

  function logout() {
    Cookies.remove("qe_token");
    router.push("/auth/login");
  }

  const navItems = [
    { href: "/dashboard", icon: LayoutDashboard, label: "Portfolio" },
  ];

  return (
    <div className="min-h-screen bg-background flex overflow-hidden">
      {/* Sidebar */}
      <aside
        className={cn(
          "fixed top-0 left-0 h-screen z-20 border-r border-border flex flex-col items-center py-4 gap-1 bg-card/50 transition-all duration-200",
          collapsed ? "w-14" : "w-44",
        )}
      >
        <div className="w-8 h-8 bg-cyan-500/10 border border-cyan-500/30 rounded-md flex items-center justify-center mb-4 flex-shrink-0">
          <TrendingUp className="w-4 h-4 text-cyan-400" />
        </div>

        {navItems.map(({ href, icon: Icon, label }) => (
          <Link
            key={href}
            href={href}
            title={collapsed ? label : undefined}
            className={cn(
              "h-9 rounded-md flex items-center gap-2 px-2.5 transition w-full",
              collapsed ? "justify-center" : "justify-start",
              pathname === href
                ? "bg-cyan-500/10 text-cyan-400"
                : "text-muted-foreground hover:text-foreground hover:bg-muted",
            )}
          >
            <Icon className="w-4 h-4 flex-shrink-0" />
            {!collapsed && <span className="text-xs font-mono whitespace-nowrap">{label}</span>}
          </Link>
        ))}

        <div className="flex-1" />

        <button
          onClick={() => setCollapsed(c => !c)}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          className="h-9 w-full rounded-md flex items-center gap-2 px-2.5 text-muted-foreground hover:text-foreground hover:bg-muted transition"
        >
          {collapsed ? (
            <ChevronRight className="w-4 h-4 flex-shrink-0 mx-auto" />
          ) : (
            <>
              <ChevronLeft className="w-4 h-4 flex-shrink-0" />
              <span className="text-xs font-mono">Collapse</span>
            </>
          )}
        </button>

        <button
          onClick={logout}
          title="Logout"
          className={cn(
            "h-9 w-full rounded-md flex items-center gap-2 px-2.5 text-muted-foreground hover:text-rose-400 hover:bg-rose-500/10 transition",
            collapsed ? "justify-center" : "justify-start",
          )}
        >
          <LogOut className="w-4 h-4 flex-shrink-0" />
          {!collapsed && <span className="text-xs font-mono">Logout</span>}
        </button>
      </aside>

      {/* Main area — ticker + page content stacked vertically */}
      <main
        className={cn(
          "flex-1 flex flex-col overflow-hidden transition-all duration-200",
          collapsed ? "ml-14" : "ml-44",
        )}
      >
        <MacroTicker quotes={quotes} />
        {children}
      </main>
    </div>
  );
}
