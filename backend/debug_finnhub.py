#!/usr/bin/env python3
"""
debug_finnhub.py  —  Finnhub vs Google Finance latency & error diagnostics
Run from backend root:  python debug_finnhub.py
"""
import re
import time
import traceback

FINNHUB_API_KEY = "d77cgspr01qp6afl4qjgd77cgspr01qp6afl4qk0"
TEST_SYMBOL_RAW = "RELIANCE"          # DB symbol (no suffix)
TEST_SYMBOL_FH  = "RELIANCE.NS"       # Finnhub requires exchange suffix for NSE
TEST_SYMBOL_G   = "RELIANCE"          # Google Finance symbol
GOOGLE_EXCHANGE = "NSE"

SEP  = "═" * 62
SEP2 = "─" * 62

# ── helpers ───────────────────────────────────────────────────────────────────

def _ms(t0: float) -> str:
    return f"{(time.perf_counter() - t0) * 1000:.1f} ms"

# ── Test 1: Finnhub ───────────────────────────────────────────────────────────

def test_finnhub() -> None:
    print(f"\n{SEP}")
    print("  TEST 1 — Finnhub /quote")
    print(f"  Symbol  : {TEST_SYMBOL_FH}")
    print(f"  API Key : {FINNHUB_API_KEY[:8]}…{FINNHUB_API_KEY[-4:]}")
    print(SEP)

    try:
        import finnhub
    except ImportError:
        print("  ✗ finnhub-python not installed — run: pip install finnhub-python")
        return

    client = finnhub.Client(api_key=FINNHUB_API_KEY)

    # ── bare symbol (common mistake) ─────────────────────────────────────
    print(f"\n  [A] Bare symbol '{TEST_SYMBOL_RAW}' (expected: 0 / empty)")
    t0 = time.perf_counter()
    try:
        q = client.quote(TEST_SYMBOL_RAW)
        print(f"      Response ({_ms(t0)}): {q}")
        c = q.get("c", 0)
        print(f"      Current price (c): {c}  ← {'ZERO — suffix required' if not c else 'OK'}")
    except Exception as e:
        _handle_finnhub_error(e, _ms(t0))

    # ── correct suffixed symbol ───────────────────────────────────────────
    print(f"\n  [B] Suffixed symbol '{TEST_SYMBOL_FH}' (expected: live NSE price)")
    t0 = time.perf_counter()
    try:
        q = client.quote(TEST_SYMBOL_FH)
        latency = _ms(t0)
        print(f"      Response ({latency}): {q}")
        c  = q.get("c", 0)
        pc = q.get("pc", 0)
        print(f"      Current price  (c)  : {c}")
        print(f"      Prev close     (pc) : {pc}")
        effective = c or pc
        if effective and float(effective) > 0:
            print(f"  ✓ Finnhub LIVE price  : {effective}  latency={latency}")
        else:
            print(f"  ✗ Finnhub returned 0 for both c and pc — market may be closed or symbol unsupported")
    except Exception as e:
        _handle_finnhub_error(e, _ms(t0))

    # ── profile endpoint ─────────────────────────────────────────────────
    print(f"\n  [C] Symbol lookup / profile2 for '{TEST_SYMBOL_FH}'")
    t0 = time.perf_counter()
    try:
        p = client.symbol_lookup(TEST_SYMBOL_FH)
        print(f"      symbol_lookup ({_ms(t0)}): count={p.get('count', '?')}  results={p.get('result', [])[:2]}")
    except Exception as e:
        _handle_finnhub_error(e, _ms(t0))


def _handle_finnhub_error(e: Exception, latency: str) -> None:
    msg = str(e)
    print(f"      Exception ({latency}): {type(e).__name__}: {msg}")
    if "401" in msg:
        print("  ✗ HTTP 401 — INVALID API KEY. Check FINNHUB_API_KEY value.")
    elif "403" in msg:
        print("  ✗ HTTP 403 — PERMISSIONS denied. Symbol may require paid plan.")
    elif "429" in msg:
        print("  ✗ HTTP 429 — RATE LIMITED. Free tier: 60 req/min. Back off and retry.")
    else:
        traceback.print_exc()


# ── Test 2: Google Finance ────────────────────────────────────────────────────

def test_google() -> None:
    print(f"\n{SEP}")
    print("  TEST 2 — Google Finance scrape")
    print(f"  URL     : https://www.google.com/finance/quote/{TEST_SYMBOL_G}:{GOOGLE_EXCHANGE}")
    print(SEP)

    import requests
    _PRICE_RE    = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
    _PRICE_RE_FB = re.compile(r'class="YMlKec fxKbKc"[^>]*>([0-9,]+\.?[0-9]*)<')
    url = f"https://www.google.com/finance/quote/{TEST_SYMBOL_G}:{GOOGLE_EXCHANGE}"

    t0 = time.perf_counter()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=8,
        )
        latency = _ms(t0)
        print(f"\n  HTTP status : {resp.status_code}  ({latency})")
        if resp.status_code == 429:
            print("  ✗ 429 — Google rate-limiting this IP")
            return
        if resp.status_code != 200:
            print(f"  ✗ Unexpected status {resp.status_code}")
            return

        price = None
        m = _PRICE_RE.search(resp.text)
        if m:
            price = float(m.group(1))
            print(f"  ✓ data-last-price attr  : {price}  latency={latency}")
        else:
            m = _PRICE_RE_FB.search(resp.text)
            if m:
                price = float(m.group(1).replace(",", ""))
                print(f"  ✓ YMlKec regex fallback : {price}  latency={latency}")
            else:
                print(f"  ✗ No price pattern found in response (latency={latency})")
                print("    First 500 chars of body:")
                print("   ", resp.text[:500])
    except Exception as e:
        print(f"  ✗ Exception ({_ms(t0)}): {e}")
        traceback.print_exc()


# ── Test 3: Head-to-head latency race ────────────────────────────────────────

def test_race() -> None:
    print(f"\n{SEP}")
    print("  TEST 3 — Head-to-head latency race (sequential for script simplicity)")
    print(SEP)

    import requests
    try:
        import finnhub
        client  = finnhub.Client(api_key=FINNHUB_API_KEY)
        t0      = time.perf_counter()
        q       = client.quote(TEST_SYMBOL_FH)
        fh_ms   = (time.perf_counter() - t0) * 1000
        fh_price = q.get("c") or q.get("pc") or 0
    except Exception as e:
        fh_ms    = -1.0
        fh_price = 0
        print(f"  Finnhub error: {e}")

    _PRICE_RE = re.compile(r'data-last-price="([0-9]+\.?[0-9]*)"')
    t0 = time.perf_counter()
    try:
        resp = requests.get(
            f"https://www.google.com/finance/quote/{TEST_SYMBOL_G}:{GOOGLE_EXCHANGE}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        g_ms = (time.perf_counter() - t0) * 1000
        m    = _PRICE_RE.search(resp.text)
        g_price = float(m.group(1)) if m else 0
    except Exception as e:
        g_ms    = -1.0
        g_price = 0
        print(f"  Google error: {e}")

    print(f"\n  {'SOURCE':<12}  {'PRICE':>10}  {'LATENCY':>10}  WINNER")
    print(f"  {SEP2}")
    fh_win = fh_ms > 0 and fh_ms < g_ms
    g_win  = g_ms  > 0 and g_ms  < fh_ms
    print(f"  {'Finnhub':<12}  {fh_price:>10.4f}  {fh_ms:>8.1f}ms  {'← FASTER' if fh_win else ''}")
    print(f"  {'Google':<12}  {g_price:>10.4f}  {g_ms:>8.1f}ms  {'← FASTER' if g_win else ''}")

    if fh_ms > 0 and g_ms > 0:
        diff = abs(fh_ms - g_ms)
        if g_win and diff > 300:
            print(f"\n  ⚠  Google is {diff:.0f}ms faster than Finnhub.")
            print("     Without a T1 headstart, Google wins every race on this host.")
            print("     Recommendation: apply T1_HEADSTART_MS = 300 in market_data.py")
        elif fh_win:
            print(f"\n  ✓ Finnhub wins by {diff:.0f}ms — racing architecture favours T1.")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{SEP}")
    print("  FINNHUB DIAGNOSTIC SCRIPT")
    print(f"  Testing: {TEST_SYMBOL_RAW} (NSE)")
    print(SEP)
    test_finnhub()
    test_google()
    test_race()
    print(f"\n{SEP}\n  DONE\n{SEP}\n")
