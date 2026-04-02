"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { authApi } from "@/lib/api";
import { cn } from "@/lib/utils";
import { Eye, EyeOff, Loader2, TrendingUp, UserPlus } from "lucide-react";

export default function SignupPage() {
  const router = useRouter();

  const [form, setForm] = useState({ username: "", email: "", password: "", confirm: "" });
  const [showPw, setShowPw]     = useState(false);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState("");
  const [success, setSuccess]   = useState("");

  function set(key: string, val: string) {
    setForm((f) => ({ ...f, [key]: val }));
    setError("");
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (form.password !== form.confirm) {
      setError("Passwords do not match.");
      return;
    }
    if (form.password.length < 6) {
      setError("Password must be at least 6 characters.");
      return;
    }
    setLoading(true);
    setError("");
    try {
      await authApi.signup(
        form.username.trim(),
        form.password,
        form.email.trim() || undefined,
      );
      setSuccess("Account created! Redirecting to login…");
      setTimeout(() => router.push("/auth/login"), 1500);
    } catch (err: any) {
      setError(err.response?.data?.detail || "Signup failed. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center relative overflow-hidden">
      <div className="absolute inset-0 scanline opacity-40" />
      <div
        className="absolute inset-0 opacity-[0.03]"
        style={{
          backgroundImage: `linear-gradient(hsl(var(--border)) 1px, transparent 1px),
            linear-gradient(90deg, hsl(var(--border)) 1px, transparent 1px)`,
          backgroundSize: "40px 40px",
        }}
      />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-cyan-500/5 rounded-full blur-3xl pointer-events-none" />

      <div className="relative z-10 w-full max-w-sm px-4 animate-fade-in">
        {/* Logo */}
        <div className="flex items-center gap-3 mb-8 justify-center">
          <div className="w-9 h-9 bg-cyan-500/10 border border-cyan-500/30 rounded-md flex items-center justify-center">
            <TrendingUp className="w-5 h-5 text-cyan-400" />
          </div>
          <div>
            <div className="text-white font-semibold tracking-widest text-sm font-mono uppercase">
              Quantedge
            </div>
            <div className="text-muted-foreground text-[10px] font-mono tracking-widest uppercase">
              Portfolio Command Center
            </div>
          </div>
        </div>

        {/* Card */}
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center gap-2 mb-1">
            <UserPlus className="w-4 h-4 text-cyan-400" />
            <h1 className="text-foreground font-semibold text-base">Create account</h1>
          </div>
          <p className="text-muted-foreground text-xs mb-5 font-mono">
            Register to access your portfolio
          </p>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Username */}
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                Username <span className="text-cyan-500">*</span>
              </label>
              <input
                type="text"
                value={form.username}
                onChange={(e) => set("username", e.target.value)}
                required
                autoFocus
                autoComplete="off"
                minLength={3}
                className={inputCls}
              />
            </div>

            {/* Email */}
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                Email <span className="text-cyan-500">*</span>
                <span className="text-muted-foreground/40 normal-case ml-1">(OTP will be sent here)</span>
              </label>
              <input
                type="email"
                value={form.email}
                onChange={(e) => set("email", e.target.value)}
                required
                autoComplete="email"
                placeholder="you@example.com"
                className={inputCls}
              />
            </div>

            {/* Password */}
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                Password <span className="text-cyan-500">*</span>
              </label>
              <div className="relative">
                <input
                  type={showPw ? "text" : "password"}
                  value={form.password}
                  onChange={(e) => set("password", e.target.value)}
                  required
                  minLength={6}
                  placeholder="min. 6 characters"
                  className={cn(inputCls, "pr-10")}
                />
                <button
                  type="button"
                  onClick={() => setShowPw(!showPw)}
                  className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition"
                >
                  {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            {/* Confirm Password */}
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                Confirm Password <span className="text-cyan-500">*</span>
              </label>
              <input
                type={showPw ? "text" : "password"}
                value={form.confirm}
                onChange={(e) => set("confirm", e.target.value)}
                required
                placeholder="repeat password"
                className={inputCls}
              />
            </div>

            {error && (
              <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">
                {error}
              </p>
            )}
            {success && (
              <p className="text-emerald-400 text-xs font-mono bg-emerald-500/10 border border-emerald-500/20 rounded px-3 py-2">
                {success}
              </p>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2.5 transition flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed font-mono"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              {loading ? "Creating account…" : "Create Account"}
            </button>

            <p className="text-center text-xs text-muted-foreground font-mono pt-1">
              Already have an account?{" "}
              <Link href="/auth/login" className="text-cyan-400 hover:text-cyan-300 transition">
                Sign in
              </Link>
            </p>
          </form>
        </div>

        <p className="text-center text-muted-foreground/40 text-[10px] font-mono mt-4 uppercase tracking-widest">
          Quantedge v1.0 · Private Access
        </p>
      </div>
    </div>
  );
}

const inputCls =
  "w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition";
