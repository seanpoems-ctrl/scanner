"""
market_intelligence.py
======================
Generates the 7-Pillar Market Intelligence briefs (Pre-Market & Post-Market)
using the Google Gemini API.  Falls back to a structured heuristic brief when
the Gemini key is absent or the API call fails.

Environment variable required:
    GEMINI_API_KEY   – Google AI Studio key (free tier works fine)

Data-integrity guarantees
--------------------------
- Every ticker is tried with multiple provider aliases in priority order.
- A module-level LRU cache preserves last-known-good values across calls so
  transient fetch failures never surface "—" for core assets (NQ, ES, VIX,
  10Y, 2Y, DXY, Gold, Oil, DAX, Nikkei, Hang Seng).
- The Gemini prompt explicitly forbids "—" or empty values for named assets.
- Catalyst section is requested as a JSON array for structured table rendering.
- gen_time_et is stamped *after* Gemini returns, reflecting actual finish time.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx
import random
import yfinance as yf
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Emergency fallback lessons — used by _heuristic_brief when the LLM is
# unreachable, so the Analyst Lesson pillar is never blank.
TRADER_LESSONS: list[str] = [
    "Price is the only truth; news is just the catalyst.",
    "In a high-volatility regime, survival is the first priority.",
    "Respect the 200-day MA; it is the line between a correction and a bear market.",
    "The best trades feel obvious in hindsight and uncomfortable in real time.",
    "Volume confirms conviction; a breakout without volume is just noise.",
    "Don't confuse a bounce with a trend — wait for the higher low.",
    "The tape tells you what the crowd believes; your edge is in reading it faster.",
    "Every great setup starts with a clean chart and a clear invalidation level.",
    "Size kills more accounts than bad entries. Respect position sizing above all.",
    "When VIX spikes above 30, reduce size first — ask questions later.",
]

try:
    from backend.scraper import fetch_series_snapshot
    from backend.market_time import is_nyse_trading_day_et
except ModuleNotFoundError:
    from scraper import fetch_series_snapshot          # type: ignore[no-redef]
    from market_time import is_nyse_trading_day_et     # type: ignore[no-redef]

log = logging.getLogger(__name__)
NY_TZ = ZoneInfo("America/New_York")

_GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
_GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-1.5-flash:generateContent"
)

# ---------------------------------------------------------------------------
# Ticker alias table
# Priority order: first alias that returns a usable close wins.
# Trailing yf: aliases use yfinance directly (no TradingView dependency).
# ---------------------------------------------------------------------------

_TICKER_ALIASES: dict[str, list[str]] = {
    "nasdaq_fut":  ["CME_MINI:NQ1!", "NASDAQ:QQQ"],
    "spx_fut":     ["CME_MINI:ES1!", "AMEX:SPY"],
    "rtx_fut":     ["CME_MINI:RTY1!", "AMEX:IWM"],
    "vix":         ["CBOE:VIX", "TVC:VIX"],
    "us10y":       ["TVC:US10Y", "TVC:TNX"],
    "us2y":        ["TVC:US02Y", "TVC:UST2Y"],
    "dxy":         ["TVC:DXY", "CURRENCYCOM:DXY"],
    "gold":        ["COMEX:GC1!", "TVC:GOLD"],
    "oil":         ["NYMEX:CL1!", "TVC:USOIL"],
    "btc":         ["CME:BTC1!", "COINBASE:BTCUSD"],
    # Global indices — TradingView first, then yfinance via ^GDAXI etc.
    "dax":         ["XETR:DAX", "yf:^GDAXI"],
    "ftse":        ["LSE:UKX", "yf:^FTSE"],
    "stoxx50":     ["EURONEXT:SX5E", "yf:^STOXX50E"],
    "nikkei":      ["TSE:NI225", "yf:^N225"],
    "hang_seng":   ["HKEX:HSI", "yf:^HSI"],
    "kospi":       ["KRX:KOSPI", "yf:^KS11"],
    # Breadth proxies
    "s5fi":        ["CBOE:S5FI", "SP:S5FI", "yf:^S5FI"],
    "mmth":        ["CBOE:MMTH", "SP:MMTH", "yf:^MMTH"],
}

# Live-session fallback cache: {key -> {"close": float, "change_pct": float}}
# Updated on every successful fetch; used as emergency fallback when APIs fail.
_MACRO_CACHE: dict[str, dict] = {}

# Previous-session close snapshot: captured at the START of each fetch run,
# before _MACRO_CACHE is overwritten with fresh data.
# This is the "yesterday" baseline that powers velocity narratives.
_PREV_CLOSE_CACHE: dict[str, float] = {}


def _snapshot_ok(d: dict) -> bool:
    """Return True if a snapshot dict has a usable numeric close."""
    try:
        return d is not None and float(d.get("close") or "x") > 0
    except Exception:
        return False


async def _fetch_yf_snapshot(yf_sym: str) -> dict:
    """Fetch close + change_pct via yfinance for a ^XXX symbol."""
    def _sync() -> dict:
        try:
            t = yf.Ticker(yf_sym)
            hist = t.history(period="2d", interval="1d")
            if hist is None or len(hist) < 1:
                return {}
            closes = hist["Close"].dropna()
            if len(closes) < 1:
                return {}
            last = float(closes.iloc[-1])
            chg = None
            if len(closes) >= 2:
                prev = float(closes.iloc[-2])
                if prev > 0:
                    chg = (last - prev) / prev * 100.0
            return {"close": last, "change_pct": chg, "change": chg}
        except Exception:
            return {}
    return await asyncio.to_thread(_sync)


async def _fetch_one(key: str) -> dict:
    """
    Try each alias in _TICKER_ALIASES[key] in order.
    Falls back to cached value if all aliases fail.
    Returns dict with at least {"close": ..., "change_pct": ...}.
    """
    aliases = _TICKER_ALIASES.get(key, [])
    for alias in aliases:
        try:
            if alias.startswith("yf:"):
                data = await _fetch_yf_snapshot(alias[3:])
            else:
                _resolved, data = await fetch_series_snapshot(alias)
            if _snapshot_ok(data):
                result = {
                    "close": data.get("close"),
                    "change_pct": data.get("change_pct") or data.get("change"),
                }
                _MACRO_CACHE[key] = result
                return result
        except Exception:
            continue
    # Emergency: serve last cached value if available
    if key in _MACRO_CACHE:
        log.warning("Macro fetch failed for %s — serving cached value", key)
        return dict(_MACRO_CACHE[key])
    return {}


async def _fetch_macro() -> dict[str, dict]:
    """
    Fetch all macro symbols concurrently with alias fallback + cache.
    Before overwriting _MACRO_CACHE, snapshot current values into
    _PREV_CLOSE_CACHE so the next call can compute velocity deltas.
    """
    global _PREV_CLOSE_CACHE
    # Snapshot current cache as "previous session" BEFORE this fetch run.
    _PREV_CLOSE_CACHE = {
        k: float(v["close"])
        for k, v in _MACRO_CACHE.items()
        if v.get("close") is not None
    }
    keys = list(_TICKER_ALIASES.keys())
    results = await asyncio.gather(*(_fetch_one(k) for k in keys), return_exceptions=True)
    out: dict[str, dict] = {}
    for key, result in zip(keys, results):
        if isinstance(result, Exception):
            out[key] = _MACRO_CACHE.get(key, {})
        else:
            out[key] = result or _MACRO_CACHE.get(key, {})
    return out


# ---------------------------------------------------------------------------
# Velocity / delta calculator for narrative context
# ---------------------------------------------------------------------------

_VELOCITY_LABELS = [
    # (min_pct, max_pct, verb, direction_word)
    (5.0,   float("inf"), "surged",    "surge"),
    (2.0,   5.0,          "ripped",    "rip"),
    (0.5,   2.0,          "edged up",  "grind"),
    (-0.5,  0.5,          "held flat", "consolidation"),
    (-2.0, -0.5,          "dipped",    "drift"),
    (-5.0, -2.0,          "tumbled",   "tumble"),
    (float("-inf"), -5.0, "collapsed", "collapse"),
]

def _velocity_label(pct: float) -> tuple[str, str]:
    """Return (verb, velocity_word) for a given % change."""
    for lo, hi, verb, word in _VELOCITY_LABELS:
        if lo <= pct < hi:
            return verb, word
    return "moved", "move"


def _build_velocity_block(macro: dict[str, dict]) -> str:
    """
    Build the HISTORICAL CONTEXT & VELOCITY block injected into the prompt.
    Compares current close against _PREV_CLOSE_CACHE to show delta and
    generate narrative-ready velocity labels Gemini uses for storytelling.

    Format per line:
      Asset | Now: X | Prev: Y | Delta: +Z% | Velocity: "surged" (rip)
    If no previous close is available, shows current value only.
    """
    # Key assets shown in the velocity block (subset — the ones that drive narrative)
    _VELOCITY_KEYS = [
        ("nasdaq_fut", "Nasdaq Futures (NQ)"),
        ("spx_fut",    "S&P 500 Futures (ES)"),
        ("rtx_fut",    "Russell 2000 (RTY)"),
        ("vix",        "VIX"),
        ("us10y",      "US 10Y Yield"),
        ("us2y",       "US 2Y Yield"),
        ("dxy",        "DXY Dollar"),
        ("gold",       "Gold (GC)"),
        ("oil",        "WTI Crude (CL)"),
        ("dax",        "DAX"),
        ("nikkei",     "Nikkei 225"),
        ("hang_seng",  "Hang Seng"),
    ]
    lines = []
    for key, label in _VELOCITY_KEYS:
        current = macro.get(key, {})
        cur_close = current.get("close")
        prev_close = _PREV_CLOSE_CACHE.get(key)

        if cur_close is None:
            lines.append(f"  {label:<26} | Now: n/a")
            continue

        cur_f = float(cur_close)
        suffix = "%" if key in ("us10y", "us2y") else ""

        if prev_close and prev_close > 0 and prev_close != cur_f:
            pct = (cur_f - prev_close) / prev_close * 100.0
            verb, word = _velocity_label(pct)
            lines.append(
                f"  {label:<26} | Now: {_num(cur_f)}{suffix} | Prev: {_num(prev_close)}{suffix}"
                f" | Delta: {pct:+.2f}% | Velocity: \"{verb}\" ({word})"
            )
        else:
            # No prev close yet (first run of the session) — show current only
            lines.append(
                f"  {label:<26} | Now: {_num(cur_f)}{suffix}"
                f" | Prev: (first run — no prior session data yet)"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting helpers — never return "—" for core assets
# ---------------------------------------------------------------------------

def _pct(v: Any, fallback: str = "unch") -> str:
    try:
        x = float(v)
        return f"{'+' if x >= 0 else ''}{x:.2f}%"
    except Exception:
        return fallback


def _num(v: Any, decimals: int = 2, fallback: str = "n/a") -> str:
    try:
        x = float(v)
        if x >= 10_000:
            return f"{x:,.0f}"
        if x >= 1_000:
            return f"{x:,.0f}"
        return f"{x:.{decimals}f}"
    except Exception:
        return fallback


def _price_line(key_close: Any, key_chg: Any) -> str:
    """Format as '19,847 / +0.42%' — never blank."""
    c = _num(key_close)
    p = _pct(key_chg)
    return f"{c} / {p}"


# ---------------------------------------------------------------------------
# News helpers
# ---------------------------------------------------------------------------

async def _fetch_rss(client: httpx.AsyncClient, query: str, limit: int = 6) -> list[dict]:
    try:
        r = await client.get(
            "https://news.google.com/rss/search",
            params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12.0,
        )
        r.raise_for_status()
        root = ET.fromstring(r.text)
        items = []
        for item in root.findall(".//item"):
            t = (item.findtext("title") or "").strip()
            l = (item.findtext("link") or "").strip()
            p = (item.findtext("pubDate") or "").strip()
            if t:
                items.append({"title": t, "link": l, "pubDate": p})
            if len(items) >= limit:
                break
        return items
    except Exception:
        return []


async def _gather_headlines(brief_type: str) -> list[dict]:
    queries = (
        [
            "US premarket futures Fed CPI earnings today",
            "global markets overnight Asia Europe",
            "economic calendar events week US",
            "premarket movers earnings guidance",
        ]
        if brief_type == "pre"
        else [
            "US stock market close today recap",
            "after-hours earnings guidance results",
            "Fed rates inflation jobs latest",
            "global markets Asia Europe tomorrow",
        ]
    )
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(*(_fetch_rss(client, q) for q in queries))
    seen: set[str] = set()
    out: list[dict] = []
    for batch in results:
        for h in batch:
            key = re.sub(r"[^a-z0-9 ]", " ", h["title"].lower())[:80]
            if key not in seen:
                seen.add(key)
                out.append(h)
                if len(out) >= 20:
                    return out
    return out


def _build_news_block(headlines: list[dict]) -> str:
    """
    Build a structured news feed string for Gemini synthesis.
    Format: [N] Title | [N+1] Title ...
    Each item is numbered so the model can cross-reference themes.
    Returns a string ready for injection into the prompt.
    """
    if not headlines:
        return "No headlines available — focus on the economic calendar for this session."
    lines = []
    for i, h in enumerate(headlines[:20], 1):
        title = h.get("title", "").strip()
        if title:
            lines.append(f"[{i:02d}] {title}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# System instruction (the "Analyst Brain" — sent as systemInstruction to Gemini)
# ---------------------------------------------------------------------------

def _get_system_instruction() -> str:
    return """You are a Market Historian and Lead Macro Strategist at a top-tier global macro hedge fund.
Your briefs are read by PMs managing 9-figure books. You do not report data — you narrate markets.

═══ STEP 1: IDENTIFY THE PRIMARY SENTIMENT DRIVER (PSD) ═══
Before writing anything, identify ONE Primary Sentiment Driver for this session.
The PSD is the single dominant macro force explaining the direction of risk assets.
Examples: "Rate Cut Fever", "Hormuz De-escalation", "Systemic De-risking", "Forced Unwind",
"Earnings Capitulation", "Fed Pivot Hope", "Credit Contagion Fear", "Dollar Wrecking Ball".
This exact phrase MUST appear in EVERY pillar as the connective thread — not just Pillar 1.
The brief must read like one coherent story, not a disconnected data dump.

═══ STEP 2: THE HISTORIAN'S WRITING RULES ═══
RULE 1 — NO NAKED NUMBERS: Never write a number without the WHY.
  FORBIDDEN: "VIX is 23.83." / "Nasdaq is +1.50%."
  REQUIRED:  "VIX retreated to 23.83 as [PSD] priced out the geopolitical risk premium."
             "Nasdaq surged +1.50% as [PSD] removed the oil-spike threat that had been paralyzing growth names."
RULE 2 — NAME THE CROWDED TRADE: When a catalyst moves the market, name what is being unwound or squeezed.
  REQUIRED:  "Triggered a massive unwinding of safety trades in bonds and gold as [PSD] took hold."
             "Trapped short-sellers in semis as the covering cascade added 80bps to the NQ move."
RULE 3 — VELOCITY OVER DIRECTION: Compare today's level to yesterday's close. Speed is the signal.
  REQUIRED:  "ES reclaimed 5,420 — up from yesterday's 5,374 close — completing a 3-day base breakout driven by [PSD]."
RULE 4 — INSTITUTIONAL VERBS ONLY: Ignited, Tumbled, Snapped back, Trapped, Squeezed, Repriced,
  Absorbed, Capitulated, Ripped, Flushed, Reclaimed, Unraveled, Compressed.
RULE 5 — BREADTH AS A STORY: Pillar 5 must diagnose the internal health narrative.
  FORBIDDEN: "S5FI is 22%."
  REQUIRED:  "Internals remain historically weak at ~22% — the rally is top-heavy. This looks like a Dead Cat Bounce
             unless [PSD] sustains enough to push S5FI back above 40% and broaden participation."
RULE 6 — ANALYST LESSON ANCHORED TO TODAY: The Analyst Lesson must directly reference the PSD.
  FORBIDDEN: Generic platitudes like "the market tests patience."
  REQUIRED:  A cycle-aware insight tied to today's specific regime and PSD.

═══ STEP 3: ABSOLUTE DATA RULES ═══
1. ZERO PLACEHOLDERS: "—", "N/A", "Calculating", blank cells — FORBIDDEN. Use numbers from context.
2. FIDELITY: Every number verbatim from LIVE MARKET DATA. Do not round, estimate, or fabricate.
3. CATALYST JSON: Pillar 3 MUST use a ```json_catalysts fence. Schema:
   [{"catalyst": string, "event": string, "impact": string, "impact_level": "Extreme High"|"High"|"Medium"|"Low"}]
   "catalyst": Named theme — NEVER "Market Headline" or "Economic Data".
   "event": Specific figure or named action with context.
   "impact": Start with bold label + sentiment ("**Extreme High.** Bearish." or "**High.** Bullish.")
     then name the CROWDED TRADE being unwound and use an institutional verb.
     Example: "**High.** Bullish. Ignited short-covering in semis; the 'higher-for-longer' pain trade
     unraveled as rate-cut expectations repriced 25bps forward."
   SYNTHESIS: Group related headlines into ONE theme. If news is thin, use the economic calendar.
4. MOOD + PSD: Pillar 1 opens with "**[Regime Name]** — [PSD]: [one sentence on today's dominant force]."
"""


# ---------------------------------------------------------------------------
# Prompt builder — 7 Pillars
# ---------------------------------------------------------------------------

def _build_prompt(brief_type: str, macro: dict[str, dict], headlines: list[dict]) -> str:
    now_et = datetime.now(NY_TZ)
    date_label = now_et.strftime("%A %B %d, %Y")
    time_placeholder = now_et.strftime("%I:%M %p")  # will be replaced post-call with actual finish time

    nq    = macro.get("nasdaq_fut", {})
    es    = macro.get("spx_fut", {})
    rty   = macro.get("rtx_fut", {})
    vix   = macro.get("vix", {})
    us10y = macro.get("us10y", {})
    us2y  = macro.get("us2y", {})
    dxy   = macro.get("dxy", {})
    gold  = macro.get("gold", {})
    oil   = macro.get("oil", {})
    dax   = macro.get("dax", {})
    ftse  = macro.get("ftse", {})
    stoxx = macro.get("stoxx50", {})
    nkk   = macro.get("nikkei", {})
    hsi   = macro.get("hang_seng", {})
    ksp   = macro.get("kospi", {})

    try:
        spread = float(us10y.get("close") or 0) - float(us2y.get("close") or 0)
        spread_str = f"{spread:+.2f}%"
        curve_label = "inverted" if spread < 0 else ("flat" if abs(spread) < 0.15 else "normal")
    except Exception:
        spread_str = "0.00%"
        curve_label = "normal"

    news_block = _build_news_block(headlines)
    velocity_block = _build_velocity_block(macro)
    brief_word = "Pre-Market" if brief_type == "pre" else "Post-Market"

    return f"""DATE: {date_label}
BRIEF TYPE: {brief_word}

===== LIVE MARKET DATA — USE THESE EXACT VALUES, NO MODIFICATIONS =====
Nasdaq Futures (NQ):    {_price_line(nq.get('close'), nq.get('change_pct'))}
S&P 500 Futures (ES):   {_price_line(es.get('close'), es.get('change_pct'))}
Russell 2000 (RTY):     {_price_line(rty.get('close'), rty.get('change_pct'))}
VIX:                    {_num(vix.get('close'))} | Change: {_pct(vix.get('change_pct'))}
US 10Y Yield:           {_num(us10y.get('close'))}% | Change: {_pct(us10y.get('change_pct'))}
US 2Y Yield:            {_num(us2y.get('close'))}% | Change: {_pct(us2y.get('change_pct'))}
Yield Curve (10Y-2Y):   {spread_str} ({curve_label})
DXY Dollar Index:       {_price_line(dxy.get('close'), dxy.get('change_pct'))}
Gold (GC):              {_price_line(gold.get('close'), gold.get('change_pct'))}
WTI Crude (CL):         {_price_line(oil.get('close'), oil.get('change_pct'))}
--- EU ---
DAX:        {_pct(dax.get('change_pct'))}
FTSE 100:   {_pct(ftse.get('change_pct'))}
STOXX 50:   {_pct(stoxx.get('change_pct'))}
--- ASIA ---
Nikkei 225: {_pct(nkk.get('change_pct'))}
Hang Seng:  {_pct(hsi.get('change_pct'))}
KOSPI:      {_pct(ksp.get('change_pct'))}
=======================================================================

===== HISTORICAL CONTEXT & VELOCITY (use these to identify the PSD and write narratives) =====
Each line shows: Asset | Current level | Previous session close | % Delta | Narrative verb
Use the Velocity column to drive your storytelling. If VIX shows "collapsed (-17%)", write that it collapsed.
If NQ shows "surged (+1.8%)", write that it surged — and explain WHY using the news feed below.
Do NOT ignore this block. It is the difference between "VIX is 23" and "VIX collapsed to 23 as the war premium was aggressively priced out."

{velocity_block}
=======================================================================

===== RAW NEWS FEED FOR SYNTHESIS ({len(headlines)} items) =====
Step 1: Scan ALL items below.
Step 2: Group items sharing a theme (e.g., multiple oil headlines = one "Geopolitical Risk Premium" catalyst).
Step 3: Identify the 3-5 most market-moving thematic catalysts for THIS session.
Step 4: If the feed is thin, substitute the most relevant scheduled economic calendar events (FOMC, CPI, Jobs, ISM, Auction).
FORBIDDEN: outputting "Market Headline" or "Economic Data" as a catalyst name.

{news_block}
=======================================================================

===== REQUIRED OUTPUT — FOLLOW THIS STRUCTURE EXACTLY =====

## Gen {time_placeholder} ET

### 1. US Market Mood
Format: "**[Regime Name]** — [PSD]: [one tight sentence declaring today's dominant force]."
Then 1-2 sentences narrating the move with exact NQ and ES values, comparing to yesterday's close to show velocity.
Every number must carry its WHY. Reference the PSD by name.
Example: "**Volatile Relief** — Hormuz De-escalation: the ceasefire proposal drained the oil-spike premium that had been choking risk appetite for 72 hours. NQ ripped to [exact value] from yesterday's close of [prior], while ES reclaimed [exact value], completing a 3-day base breakout as trapped shorts were forced to cover."

### 2. Global Synchronization
Line 1 — Verbatim index data: "DAX {_pct(dax.get('change_pct'))} · FTSE {_pct(ftse.get('change_pct'))} · STOXX {_pct(stoxx.get('change_pct'))} | Nikkei {_pct(nkk.get('change_pct'))} · Hang Seng {_pct(hsi.get('change_pct'))} · KOSPI {_pct(ksp.get('change_pct'))}"
Line 2 — Narrative: explain whether global risk assets are aligned with or diverging from the PSD. Name the strongest and weakest region and what that divergence signals for the US open.

### 3. Economic Data & Catalysts
SYNTHESIS TASK: Scan the RAW NEWS FEED. Group related headlines into 3-5 thematic catalysts. Do NOT list headlines one-by-one.
"impact" MUST follow the Historian format: start with bold label + sentiment ("**High.** Bullish."), then NAME THE CROWDED TRADE being unwound/triggered, then use an institutional verb.
Example impact: "**Extreme High.** Bullish. Triggered a massive unwinding of safety trades in bonds and gold; duration longs in Tech ripped +2.1% as [PSD] repriced rate-cut expectations 25bps forward."
FORBIDDEN: generic catalyst names, naked numbers, and any sentence without a WHY.

```json_catalysts
[
  {{"catalyst": "<specific theme — NEVER 'Market Headline'>", "event": "<specific figure or named action>", "impact": "**Extreme High.** Bullish/Bearish/Mixed. <crowded trade named + institutional verb>", "impact_level": "Extreme High"}},
  {{"catalyst": "<specific theme>", "event": "<specific figure or named action>", "impact": "**High.** Bullish/Bearish/Mixed. <crowded trade named + institutional verb>", "impact_level": "High"}},
  {{"catalyst": "<specific theme>", "event": "<specific figure or named action>", "impact": "**Medium.** Bullish/Bearish/Mixed. <crowded trade named + institutional verb>", "impact_level": "Medium"}}
]
```

### 4. Volatility & Risk Gauges
- VIX {_num(vix.get('close'))}: [Green <15 | Yellow 15-25 | Red 25-35 | Extreme >35] — explain how [PSD] is CAUSING this VIX level and what it mechanically does to dealer gamma and options pricing. No naked number.
- Yield curve {spread_str} ({curve_label}): explain how [PSD] is manifesting in credit spreads and which sectors are being structurally pressured or relieved as a result.
- **Risk Status: [Green/Yellow/Red/Extreme]** — what this level mandates for position sizing and hedge ratio in the context of [PSD].

### 5. Market Breadth
Tell the INTERNAL HEALTH STORY. Reference [PSD] to explain WHY breadth looks the way it does.
- S5FI estimate: [X]% — do NOT just state this number; diagnose it. Is the rally top-heavy? A dead cat bounce? Broadening?
  Example: "Internals remain historically weak at ~[X]% — the rally is top-heavy, suggesting a 'Dead Cat Bounce' driven by a handful of mega-caps unless [PSD] sustains enough to push S5FI back above 40% and trigger real breadth expansion."
- Name which sectors are confirming the move and which are refusing to participate.

### 6. Fixed Income & Yields
- 10Y at {_num(us10y.get('close'))}%: connect this yield level back to [PSD] — is [PSD] compressing or expanding yields? Name the exact threshold (e.g., 4.40%) that flips growth stocks from tailwind to headwind.
- Curve ({spread_str}, {curve_label}): explain the story this curve shape tells about [PSD]'s medium-term implication for bank NIM, credit availability, and cyclical vs. defensive rotation.

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- Key SPX level: [exact price with context — 200d MA / reclaim / distribution] — what a HOLD vs. BREAK means for institutional positioning under the current [PSD] regime.
- High-conviction setup: [specific sector + named catalyst + entry condition + sizing rule]

**The Analyst Lesson:**
> [One cycle-aware insight that directly references [PSD] and today's data — name the historical pattern, not a platitude. E.g., "When [PSD] compresses vol this fast, the first 48 hours are a short squeeze — the real test is whether breadth expands in the 72-hour window."]

Tactical takeaway: [Specific action + specific condition tied to [PSD] + sizing discipline — one sentence]

---
"""


# ---------------------------------------------------------------------------
# Gemini API call with retry
# ---------------------------------------------------------------------------

async def _call_gemini(prompt: str, *, attempts: int = 3) -> str | None:
    if not _GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set; using heuristic brief fallback.")
        return None
    system_instruction = _get_system_instruction()
    body = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.6,
            "maxOutputTokens": 2200,
            "topP": 0.92,
        },
    }
    url = f"{_GEMINI_URL}?key={_GEMINI_API_KEY}"
    for attempt in range(1, attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=50.0) as client:
                r = await client.post(url, json=body)
                r.raise_for_status()
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                if text.strip():
                    return text.strip()
                log.warning("Gemini attempt %d/%d returned empty text", attempt, attempts)
        except Exception as exc:
            log.warning("Gemini attempt %d/%d failed: %s", attempt, attempts, exc)
        if attempt < attempts:
            await asyncio.sleep(4 * attempt)
    return None


# ---------------------------------------------------------------------------
# Catalyst JSON extractor
# ---------------------------------------------------------------------------

_VALID_IMPACT_LEVELS = {"Extreme High", "High", "Medium", "Low"}


def _extract_catalysts(markdown: str) -> list[dict]:
    """
    Parse the ```json_catalysts [...] ``` block in Pillar 3 into catalyst dicts.
    Tries multiple fence patterns for robustness.
    Returns [] if parsing fails — frontend falls back to plain text rendering.
    """
    # Primary: our custom fence label
    m = re.search(r"```json_catalysts\s*(\[.*?\])\s*```", markdown, re.DOTALL)
    if not m:
        # Fallback 1: standard ```json fence
        m = re.search(r"```json\s*(\[.*?\])\s*```", markdown, re.DOTALL)
    if not m:
        # Fallback 2: bare JSON array between Pillar 3 and Pillar 4 headers
        m = re.search(r"###\s*3\..*?(\[.*?\]).*?###\s*4", markdown, re.DOTALL)
    if not m:
        return []
    try:
        raw = json.loads(m.group(1))
        if not isinstance(raw, list):
            return []
        out = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            level = str(item.get("impact_level") or "Medium").strip()
            if level not in _VALID_IMPACT_LEVELS:
                # normalise legacy values
                level_lower = level.lower()
                if "extreme" in level_lower:
                    level = "Extreme High"
                elif "high" in level_lower:
                    level = "High"
                elif "low" in level_lower:
                    level = "Low"
                else:
                    level = "Medium"
            out.append({
                "catalyst":     str(item.get("catalyst") or "").strip(),
                "event":        str(item.get("event") or "").strip(),
                "impact":       str(item.get("impact") or "").strip(),
                "impact_level": level,
            })
        return out
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Heuristic fallback — same 7-pillar structure, no LLM required
# ---------------------------------------------------------------------------

def _heuristic_brief(brief_type: str, macro: dict[str, dict], headlines: list[dict], time_label: str) -> str:
    nq    = macro.get("nasdaq_fut", {})
    es    = macro.get("spx_fut", {})
    vix   = macro.get("vix", {})
    us10y = macro.get("us10y", {})
    us2y  = macro.get("us2y", {})
    dax   = macro.get("dax", {})
    nkk   = macro.get("nikkei", {})
    hsi   = macro.get("hang_seng", {})
    ftse  = macro.get("ftse", {})
    stoxx = macro.get("stoxx50", {})
    ksp   = macro.get("kospi", {})

    try:
        vix_n = float(vix.get("close") or 0)
        vix_zone = (
            "Green Zone (<15)" if vix_n < 15
            else "Yellow Zone (15-25)" if vix_n < 25
            else "Red Zone (25-35)" if vix_n < 35
            else "Extreme Zone (>35)"
        )
        vix_str = f"{vix_n:.2f} — {vix_zone}"
    except Exception:
        vix_zone = "Yellow Zone"
        vix_str = f"n/a — {vix_zone}"

    try:
        spread = float(us10y.get("close") or 0) - float(us2y.get("close") or 0)
        curve_str = f"{spread:+.2f}% ({'inverted' if spread < 0 else 'normal'})"
    except Exception:
        curve_str = "n/a"

    brief_word = "Pre-Market" if brief_type == "pre" else "Post-Market"

    # Catalyst heuristic: keyword-group headlines into themes rather than
    # listing them individually, so the fallback never shows "Market Headline".
    _THEME_KEYWORDS: list[tuple[str, str, str]] = [
        ("fed|fomc|rate|powell|hawkish|dovish|pivot",
         "Fed Policy", "Monetary policy signal repriced across rate-sensitive sectors."),
        ("inflation|cpi|pce|core|prices",
         "Inflation Print", "Duration assets repriced; Tech vs. Financials rotation triggered."),
        ("jobs|payroll|unemployment|adp|claims|labor",
         "Labor Market Data", "Risk appetite adjusted on employment outlook."),
        ("war|geopolit|iran|russia|ukraine|hormuz|sanction|military|shipping|red.sea|supply.chain|strait|ceasefire|conflict",
         "Geopolitical Risk", "Safety bid in Gold and Treasuries; Energy risk premium repriced. Shipping and supply-chain exposed names volatile."),
        ("earnings|guidance|revenue|eps|beat|miss|quarter",
         "Earnings Catalyst", "Single-stock and sector ETF repriced on guidance revision."),
        ("china|tariff|trade|export|import|yuan|renminbi",
         "Trade & China Risk", "EM and export-exposed sectors repriced on trade flow signal."),
        ("gdp|growth|recession|contraction|expansion",
         "Growth Outlook", "Cyclicals vs. defensives rotation signal in play."),
        ("oil|crude|opec|energy|nat.?gas|wti|brent",
         "Energy / Commodities", "Energy sector and transportation costs repriced."),
        ("bank|credit|spread|default|svb|financials",
         "Credit & Financial Stress", "Risk-off signal; financials and HY spreads under pressure."),
    ]

    grouped: dict[str, list[str]] = {}
    ungrouped: list[str] = []
    for h in headlines[:20]:
        title_lower = (h.get("title") or "").lower()
        matched = False
        for pattern, theme, _ in _THEME_KEYWORDS:
            if re.search(pattern, title_lower):
                grouped.setdefault(theme, []).append(h.get("title", ""))
                matched = True
                break
        if not matched:
            ungrouped.append(h.get("title", ""))

    # Build catalyst rows from grouped themes, falling back to econ calendar
    catalyst_rows = []
    tier = ["Extreme High", "High", "Medium", "Low"]
    t_idx = 0
    for pattern, theme, default_impact in _THEME_KEYWORDS:
        if theme in grouped and t_idx < 4:
            titles = grouped[theme]
            event_text = titles[0][:80] if len(titles) == 1 else f"{len(titles)} related items: {titles[0][:55]}…"
            catalyst_rows.append({
                "catalyst": theme,
                "event": event_text,
                "impact": default_impact,
                "impact_level": tier[t_idx],
            })
            t_idx += 1

    # Fill with econ calendar placeholders if under 3 rows
    econ_fillers = [
        ("FOMC Meeting / Fed Speakers", "Scheduled remarks — watch for tone shift on cuts timeline.",
         "Rate-sensitive assets on standby; any hawkish tilt reprices short-end.", "High"),
        ("Economic Calendar", "Jobless Claims / ISM / Housing data due this session.",
         "Mixed. Data-dependent session; breadth confirmation required before sizing up.", "Medium"),
        ("Treasury Auction", "3Y/10Y/30Y supply hitting this week.",
         "Bearish for duration if tails; yields move inversely.", "Low"),
    ]
    for cat, ev, imp, lvl in econ_fillers:
        if len(catalyst_rows) >= 3:
            break
        catalyst_rows.append({"catalyst": cat, "event": ev, "impact": imp, "impact_level": lvl})

    if not catalyst_rows:
        catalyst_rows = [{
            "catalyst": "Session Open",
            "event": f"{brief_word} session — no news fetched",
            "impact": "Follow price action; tape is the primary signal.",
            "impact_level": "Medium",
        }]

    catalyst_json = json.dumps(catalyst_rows, indent=2)

    return f"""## Gen {time_label}

### 1. US Market Mood
{brief_word} session. NQ {_price_line(nq.get('close'), nq.get('change_pct'))} · ES {_price_line(es.get('close'), es.get('change_pct'))}. Price action is the primary signal; follow the tape and not the narrative.

### 2. Global Synchronization
DAX {_pct(dax.get('change_pct'))} · FTSE {_pct(ftse.get('change_pct'))} · STOXX {_pct(stoxx.get('change_pct'))} | Nikkei {_pct(nkk.get('change_pct'))} · Hang Seng {_pct(hsi.get('change_pct'))} · KOSPI {_pct(ksp.get('change_pct'))}. Assess alignment before adding cross-asset exposure.

### 3. Economic Data & Catalysts
```json_catalysts
{catalyst_json}
```

### 4. Volatility & Risk Gauges
- VIX: {vix_str}
- Yield curve (10Y-2Y): {curve_str}
- **Risk Status: {vix_zone}** — size positions accordingly.

### 5. Market Breadth
- S5FI estimate: context-dependent on index context — Internal Health: verify broad participation before adding size.
- A rising tape on narrow breadth is a caution signal; demand confirmation from advancing issues.

### 6. Fixed Income & Yields
- US 10Y at {_num(us10y.get('close'))}%: growth stocks face headwinds above 4.50%; {_num(us10y.get('close'))}% is the current rate regime signal.
- Yield curve {curve_str}: watch for steepening as a risk-on signal for cyclicals over growth.

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- Key SPX level: watch the 200-day MA — the most respected institutional risk toggle.
- High conviction: only A+ setups with confirmed volume; no chasing extended names.

**The Analyst Lesson:**
> "{random.choice(TRADER_LESSONS)}"

Tactical takeaway: In {vix_zone.split("(")[0].strip().lower()} conditions, reduce position size, demand confirmation, and preserve capital for when the edge is clear.

---
"""


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

async def generate_intelligence_brief(brief_type: str) -> dict[str, Any]:
    """
    Generate a Market Intelligence Brief.
    brief_type: "pre" | "post"
    Returns a dict safe to JSON-serialize and cache.
    gen_time_et is stamped *after* Gemini finishes so it reflects actual data freshness.
    """
    assert brief_type in ("pre", "post"), "brief_type must be 'pre' or 'post'"

    macro, headlines = await asyncio.gather(
        _fetch_macro(),
        _gather_headlines(brief_type),
    )

    prompt = _build_prompt(brief_type, macro, headlines)
    markdown = await _call_gemini(prompt)

    # Stamp time AFTER Gemini completes — reflects actual finish time.
    now_utc    = datetime.now(timezone.utc).replace(microsecond=0)
    now_et     = now_utc.astimezone(NY_TZ)
    time_label = now_et.strftime("%I:%M %p ET")

    if not markdown:
        markdown = _heuristic_brief(brief_type, macro, headlines, time_label)

    # Replace the placeholder time in ## Gen header with the actual post-call finish time.
    # Regex matches: ## Gen 08:03 AM ET  OR  ## Gen 8:03 AM ET  OR  ## Gen 08:03 ET
    markdown = re.sub(
        r"^## Gen\s+\d{1,2}:\d{2}(?:\s*[AP]M)?\s*ET",
        f"## Gen {time_label}",
        markdown, count=1, flags=re.MULTILINE
    )

    catalysts = _extract_catalysts(markdown)

    return {
        "brief_type": brief_type,
        "generated_at_utc": now_utc.isoformat(),
        "gen_time_et": time_label,
        "markdown": markdown,
        "catalysts": catalysts,
        "headlines": headlines[:8],
        "macro_snapshot": {
            k: {"close": v.get("close"), "change_pct": v.get("change_pct")}
            for k, v in macro.items()
        },
    }


# ---------------------------------------------------------------------------
# Persistence helpers (used by scheduler + endpoint)
# ---------------------------------------------------------------------------

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
_PRE_FILE  = os.path.join(_DATA_DIR, "intel_brief_pre.json")
_POST_FILE = os.path.join(_DATA_DIR, "intel_brief_post.json")


def _brief_file(brief_type: str) -> str:
    return _PRE_FILE if brief_type == "pre" else _POST_FILE


async def load_brief(brief_type: str) -> dict | None:
    path = _brief_file(brief_type)
    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, lambda: open(path, encoding="utf-8").read())
        return json.loads(raw)
    except Exception:
        return None


async def save_brief(brief_type: str, data: dict) -> None:
    path = _brief_file(brief_type)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    raw = json.dumps(data, ensure_ascii=False, indent=2)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: open(path, "w", encoding="utf-8").write(raw))


async def generate_and_save(brief_type: str) -> dict:
    """Generate, save, and return a brief.  Safe to call from scheduler."""
    data = await generate_intelligence_brief(brief_type)
    await save_brief(brief_type, data)
    log.info("Intelligence brief (%s) saved — Gen %s", brief_type, data.get("gen_time_et"))
    return data
