"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Cookies from "js-cookie";
import { TrendingUp, LayoutDashboard, LogOut, ChevronRight, ChevronLeft } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
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
      {/* Sidebar — fixed, non-scrolling */}
      <aside
        className={cn(
          "fixed top-0 left-0 h-screen z-20 border-r border-border flex flex-col items-center py-4 gap-1 bg-card/50 transition-all duration-200",
          collapsed ? "w-14" : "w-44"
        )}
      >
        {/* Logo mark */}
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
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            <Icon className="w-4 h-4 flex-shrink-0" />
            {!collapsed && (
              <span className="text-xs font-mono whitespace-nowrap">{label}</span>
            )}
          </Link>
        ))}

        <div className="flex-1" />

        {/* Collapse/Expand toggle */}
        <button
          onClick={() => setCollapsed((c) => !c)}
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
            collapsed ? "justify-center" : "justify-start"
          )}
        >
          <LogOut className="w-4 h-4 flex-shrink-0" />
          {!collapsed && <span className="text-xs font-mono">Logout</span>}
        </button>
      </aside>

      {/* Main content — offset by sidebar width, only this area scrolls */}
      <main
        className={cn(
          "flex-1 flex flex-col overflow-hidden transition-all duration-200",
          collapsed ? "ml-14" : "ml-44"
        )}
      >
        {children}
      </main>
    </div>
  );
}
