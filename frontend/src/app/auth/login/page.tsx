"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import Cookies from "js-cookie";
import { authApi } from "@/lib/api";
import { cn } from "@/lib/utils";
import { Eye, EyeOff, Loader2, ShieldCheck, TrendingUp, RefreshCw } from "lucide-react";

type Step = "credentials" | "otp";

export default function LoginPage() {
  const router = useRouter();
  const [step, setStep]         = useState<Step>("credentials");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [otp, setOtp]           = useState("");
  const [showPw, setShowPw]     = useState(false);
  const [loading, setLoading]   = useState(false);
  const [resending, setResending] = useState(false);
  const [resendMsg, setResendMsg] = useState("");
  const [error, setError]       = useState("");

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      await authApi.login(username, password);
      setStep("otp");
    } catch (err: any) {
      setError(err.response?.data?.detail || "Login failed. Check credentials.");
    } finally {
      setLoading(false);
    }
  }

  async function handleOtp(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await authApi.verifyOtp(username, otp);
      Cookies.set("qe_token", res.data.access_token, { expires: 1, sameSite: "strict" });
      router.push("/dashboard");
    } catch (err: any) {
      setError(err.response?.data?.detail || "Invalid OTP. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  async function handleResend() {
    setResendMsg("");
    setError("");
    setResending(true);
    try {
      await authApi.resendOtp(username, password);
      setOtp("");
      setResendMsg("Fresh OTP sent to your email.");
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to resend OTP.");
    } finally {
      setResending(false);
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
          {step === "credentials" ? (
            <>
              <h1 className="text-foreground font-semibold text-base mb-1">Sign in</h1>
              <p className="text-muted-foreground text-xs mb-5 font-mono">
                Enter credentials to receive OTP
              </p>
              <form onSubmit={handleLogin} className="space-y-4">
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                    Username
                  </label>
                  <input
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    required
                    autoFocus
                    autoComplete="off"
                    className={inputCls}
                  />
                </div>
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                    Password
                  </label>
                  <div className="relative">
                    <input
                      type={showPw ? "text" : "password"}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                      placeholder="••••••••"
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

                {error && (
                  <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">
                    {error}
                  </p>
                )}

                <button
                  type="submit"
                  disabled={loading}
                  className="w-full bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2.5 transition flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed font-mono"
                >
                  {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
                  {loading ? "Sending OTP…" : "Continue"}
                </button>

                <p className="text-center text-xs text-muted-foreground font-mono pt-1">
                  No account?{" "}
                  <Link href="/auth/signup" className="text-cyan-400 hover:text-cyan-300 transition">
                    Create one
                  </Link>
                </p>
              </form>
            </>
          ) : (
            <>
              <div className="flex items-center gap-2 mb-1">
                <ShieldCheck className="w-4 h-4 text-cyan-400" />
                <h1 className="text-foreground font-semibold text-base">Verify OTP</h1>
              </div>
              <p className="text-muted-foreground text-xs mb-5 font-mono">
                6-digit code sent to your registered email
              </p>
              <form onSubmit={handleOtp} className="space-y-4">
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground font-mono uppercase tracking-wider">
                    One-Time Password
                  </label>
                  <input
                    type="text"
                    value={otp}
                    onChange={(e) => setOtp(e.target.value.replace(/\D/g, "").slice(0, 6))}
                    required
                    autoFocus
                    maxLength={6}
                    placeholder="000000"
                    className="w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground text-center tracking-[0.5em] placeholder:text-muted-foreground/50 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition text-lg"
                  />
                </div>

                {resendMsg && (
                  <p className="text-emerald-400 text-xs font-mono bg-emerald-500/10 border border-emerald-500/20 rounded px-3 py-2">
                    {resendMsg}
                  </p>
                )}
                {error && (
                  <p className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded px-3 py-2">
                    {error}
                  </p>
                )}

                <button
                  type="submit"
                  disabled={loading || otp.length < 6}
                  className="w-full bg-cyan-500 hover:bg-cyan-400 text-black font-semibold text-sm rounded-md py-2.5 transition flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed font-mono"
                >
                  {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
                  {loading ? "Verifying…" : "Verify & Enter"}
                </button>

                <div className="flex items-center justify-between pt-1">
                  <button
                    type="button"
                    onClick={() => { setStep("credentials"); setError(""); setOtp(""); setResendMsg(""); }}
                    className="text-muted-foreground hover:text-foreground text-xs font-mono transition"
                  >
                    ← Back to login
                  </button>
                  <button
                    type="button"
                    onClick={handleResend}
                    disabled={resending}
                    className="flex items-center gap-1 text-cyan-400 hover:text-cyan-300 text-xs font-mono transition disabled:opacity-50"
                  >
                    <RefreshCw className={cn("w-3 h-3", resending && "animate-spin")} />
                    {resending ? "Sending…" : "Resend OTP"}
                  </button>
                </div>
              </form>
            </>
          )}
        </div>

        <p className="text-center text-muted-foreground/40 text-[10px] font-mono mt-4 uppercase tracking-widest">
          Quantedge v1.0 · Private Access
        </p>
      </div>
    </div>
  );
}

const inputCls =
  "w-full bg-input border border-border rounded-md px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/50 focus:outline-none focus:ring-1 focus:ring-cyan-500 focus:border-cyan-500 transition";
