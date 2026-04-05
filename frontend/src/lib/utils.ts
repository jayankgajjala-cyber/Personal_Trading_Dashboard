import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function fmt(n: number | null | undefined, decimals = 2) {
  if (n == null) return "—";
  return n.toLocaleString("en-IN", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function fmtCurrency(n: number | null | undefined, exchange = "NSE") {
  if (n == null) return "—";
  const ex = (exchange ?? "NSE").toUpperCase();
  // Determine prefix and locale by exchange region
  if (ex === "US" || ex === "CRYPTO") {
    return "$" + (n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (ex === "EUR") {
    return "€" + (n).toLocaleString("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (ex === "GBP") {
    return "£" + (n).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  // Default: Indian exchanges (NSE, BSE) → ₹
  return "₹" + (n).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function fmtPct(n: number | null | undefined) {
  if (n == null) return "—";
  const sign = n >= 0 ? "+" : "";
  return sign + fmt(n) + "%";
}

// fmtCompact always uses ₹ — portfolio totals are always INR
export function fmtCompact(n: number | null | undefined) {
  if (n == null) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1_00_00_000) return sign + "₹" + (abs / 1_00_00_000).toFixed(2) + " Cr";
  if (abs >= 1_00_000)    return sign + "₹" + (abs / 1_00_000).toFixed(2) + " L";
  return "₹" + n.toLocaleString("en-IN", { maximumFractionDigits: 2 });
}
