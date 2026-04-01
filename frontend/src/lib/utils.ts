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
  const isUS = exchange === "US";
  const prefix = isUS ? "$" : "₹";
  return prefix + n.toLocaleString(isUS ? "en-US" : "en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export function fmtPct(n: number | null | undefined) {
  if (n == null) return "—";
  const sign = n >= 0 ? "+" : "";
  return sign + fmt(n) + "%";
}

export function fmtCompact(n: number | null | undefined) {
  if (n == null) return "—";
  if (Math.abs(n) >= 1_00_00_000) return "₹" + (n / 1_00_00_000).toFixed(2) + " Cr";
  if (Math.abs(n) >= 1_00_000) return "₹" + (n / 1_00_000).toFixed(2) + " L";
  return "₹" + n.toLocaleString("en-IN", { maximumFractionDigits: 2 });
}
