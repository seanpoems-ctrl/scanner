"""
market_intelligence.py
======================
Generates the 7-Pillar Market Intelligence briefs (Pre-Market & Post-Market)
using the Google Gemini API.  Falls back to a structured heuristic brief when
the Gemini key is absent or the API call fails.

Environment variable required:
    GEMINI_API_KEY   – Google AI Studio key (free tier works fine)

Data flow
---------
1. Fetch live macro data (futures, VIX, yields, EU/Asia) via yfinance + scraper
2. Fetch top headlines from Google News RSS
3. Build a rich analyst prompt following the 7-Pillar structure
4. Send to Gemini (with 3-attempt retry)
5. Return a structured dict that the /api/intelligence-brief endpoint persists to JSON
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx
from zoneinfo import ZoneInfo

try:
    from backend.scraper import fetch_series_snapshot
    from backend.market_time import is_nyse_trading_day_et
except ModuleNotFoundError:
    from scraper import fetch_series_snapshot          # type: ignore[no-redef]
    from market_time import is_nyse_trading_day_et     # type: ignore[no-redef]

log = logging.getLogger(__name__)
NY_TZ = ZoneInfo("America/New_York")

BRIEF_FILE = os.path.join(os.path.dirname(__file__), "data", "intelligence_brief.json")

_GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
_GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-1.5-flash:generateContent"
)

# ---------------------------------------------------------------------------
# Macro data helpers
# ---------------------------------------------------------------------------

_MACRO_SYMBOLS: list[tuple[str, str]] = [
    ("nasdaq_fut",  "CME_MINI:NQ1!"),
    ("spx_fut",     "CME_MINI:ES1!"),
    ("rtx_fut",     "CME_MINI:RTY1!"),
    ("vix",         "CBOE:VIX"),
    ("us10y",       "TVC:US10Y"),
    ("us2y",        "TVC:US02Y"),
    ("dxy",         "TVC:DXY"),
    ("gold",        "COMEX:GC1!"),
    ("oil",         "NYMEX:CL1!"),
    ("btc",         "CME:BTC1!"),
    ("dax",         "XETR:DAX"),
    ("ftse",        "LSE:UKX"),
    ("stoxx50",     "EURONEXT:SX5E"),
    ("nikkei",      "TSE:NI225"),
    ("hang_seng",   "HKEX:HSI"),
    ("kospi",       "KRX:KOSPI"),
]


async def _fetch_macro() -> dict[str, dict]:
    snaps = await asyncio.gather(
        *(fetch_series_snapshot(sym) for _, sym in _MACRO_SYMBOLS),
        return_exceptions=True,
    )
    out: dict[str, dict] = {}
    for (key, _sym), result in zip(_MACRO_SYMBOLS, snaps):
        if isinstance(result, Exception):
            out[key] = {}
        else:
            _resolved, data = result
            out[key] = data
    return out


def _pct(v: Any) -> str:
    try:
        x = float(v)
        return f"{'+' if x >= 0 else ''}{x:.2f}%"
    except Exception:
        return "—"


def _num(v: Any, decimals: int = 2) -> str:
    try:
        x = float(v)
        if x >= 1_000:
            return f"{x:,.0f}"
        return f"{x:.{decimals}f}"
    except Exception:
        return "—"


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
                if len(out) >= 12:
                    return out
    return out


# ---------------------------------------------------------------------------
# Prompt builder — 7 Pillars
# ---------------------------------------------------------------------------

def _build_prompt(brief_type: str, macro: dict[str, dict], headlines: list[dict]) -> str:
    now_et = datetime.now(NY_TZ)
    time_label = now_et.strftime("%I:%M %p ET")
    date_label = now_et.strftime("%A %B %d, %Y")

    nq  = macro.get("nasdaq_fut", {})
    es  = macro.get("spx_fut", {})
    rty = macro.get("rtx_fut", {})
    vix = macro.get("vix", {})
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

    hl_block = "\n".join(f"- {h['title']}" for h in headlines[:10]) or "No headlines fetched."

    brief_word = "Pre-Market" if brief_type == "pre" else "Post-Market"

    prompt = f"""You are an elite institutional equity analyst producing a {brief_word} Market Intelligence Brief.

DATE: {date_label}
GENERATION TIME: {time_label}

CURRENT MARKET DATA:
- Nasdaq Futures (NQ): {_num(nq.get('close'))} ({_pct(nq.get('change_pct'))})
- S&P 500 Futures (ES): {_num(es.get('close'))} ({_pct(es.get('change_pct'))})
- Russell 2000 Futures (RTY): {_num(rty.get('close'))} ({_pct(rty.get('change_pct'))})
- VIX: {_num(vix.get('close'))} ({_pct(vix.get('change_pct'))})
- US 10Y Yield: {_num(us10y.get('close'))}% ({_pct(us10y.get('change_pct'))})
- US 2Y Yield: {_num(us2y.get('close'))}% ({_pct(us2y.get('change_pct'))})
- DXY (Dollar): {_num(dxy.get('close'))} ({_pct(dxy.get('change_pct'))})
- Gold: {_num(gold.get('close'))} ({_pct(gold.get('change_pct'))})
- WTI Crude: {_num(oil.get('close'))} ({_pct(oil.get('change_pct'))})
- DAX: {_pct(dax.get('change_pct'))} | FTSE: {_pct(ftse.get('change_pct'))} | STOXX50: {_pct(stoxx.get('change_pct'))}
- Nikkei: {_pct(nkk.get('change_pct'))} | Hang Seng: {_pct(hsi.get('change_pct'))} | KOSPI: {_pct(ksp.get('change_pct'))}

TODAY'S TOP HEADLINES:
{hl_block}

INSTRUCTIONS:
Generate a {brief_word} Market Intelligence Brief following the EXACT 7-Pillar structure below.
Tone: Grounded, skeptical, and professional. Speak like a senior PM who has seen many market cycles.
DO NOT pad with generic disclaimers. Every sentence must be actionable or insightful.

---

## Gen {time_label}

### 1. US Market Mood
Summarize the primary sentiment driver and "vibe" in 2-3 tight sentences.
Reference the specific futures levels provided above.

### 2. Global Synchronization
Comment on EU and Asian market coupling. Are risk assets aligned or diverging?
Reference the specific index changes provided.

### 3. Economic Data & Catalysts
Create a markdown table using EXACTLY this format:
| Catalyst | Data/Event | Market Impact |
|---|---|---|
Fill with 3-5 rows derived from the headlines and your knowledge of the economic calendar.

### 4. Volatility & Risk Gauges
- State the current VIX level and what it means (use: Green Zone <15, Yellow Zone 15-25, Red Zone 25-35, Extreme >35)
- Comment on the yield curve spread (10Y minus 2Y) and its implication
- Assign a "Risk Status" color with one-line justification

### 5. Market Breadth
Using professional knowledge (S5FI typically lags by a session), provide your breadth assessment:
- State the likely S5FI range (% of S&P 500 stocks above 50SMA) given the current market context
- Assess "Internal Health": is this a narrow (few leaders) or broad (wide participation) move?

### 6. Fixed Income & Yields
- Current 10Y yield impact: at what level does it become a headwind for high-multiple growth stocks?
- Is the yield curve steepening or flattening? What does this signal for the next 30 days?

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- State the key S&P 500 level to watch (200-day MA, recent pivot, support/resistance)
- One high-conviction setup or sector to watch today

**The Analyst Lesson:**
> [A memorable, non-generic trading insight in one sentence — a quote-style observation]

Tactical takeaway: [One specific, actionable follow-through sentence based on today's data]

---
End your brief here. Do not add a summary or disclaimer after the 7 pillars.
"""
    return prompt


# ---------------------------------------------------------------------------
# Gemini API call with retry
# ---------------------------------------------------------------------------

async def _call_gemini(prompt: str, *, attempts: int = 3) -> str | None:
    if not _GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set; using heuristic brief fallback.")
        return None
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 1800,
            "topP": 0.9,
        },
    }
    url = f"{_GEMINI_URL}?key={_GEMINI_API_KEY}"
    for attempt in range(1, attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=40.0) as client:
                r = await client.post(url, json=body)
                r.raise_for_status()
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                return text.strip()
        except Exception as exc:
            log.warning("Gemini attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                await asyncio.sleep(3 * attempt)
    return None


# ---------------------------------------------------------------------------
# Heuristic fallback — same 7-pillar structure, no LLM required
# ---------------------------------------------------------------------------

def _heuristic_brief(brief_type: str, macro: dict[str, dict], headlines: list[dict]) -> str:
    now_et = datetime.now(NY_TZ)
    time_label = now_et.strftime("%I:%M %p ET")
    nq  = macro.get("nasdaq_fut", {})
    es  = macro.get("spx_fut", {})
    vix_val = macro.get("vix", {})
    us10y = macro.get("us10y", {})
    us2y  = macro.get("us2y", {})
    dax   = macro.get("dax", {})
    nkk   = macro.get("nikkei", {})

    try:
        vix_n = float(vix_val.get("close") or 0)
        vix_zone = (
            "Green Zone (<15)" if vix_n < 15
            else "Yellow Zone (15-25)" if vix_n < 25
            else "Red Zone (25-35)" if vix_n < 35
            else "Extreme Zone (>35)"
        )
    except Exception:
        vix_zone = "—"

    brief_word = "Pre-Market" if brief_type == "pre" else "Post-Market"
    hl_lines = "\n".join(f"- {h['title']}" for h in headlines[:5]) or "- No headlines available."

    return f"""## Gen {time_label}

### 1. US Market Mood
{brief_word} session. Nasdaq ({_num(nq.get('close'))} / {_pct(nq.get('change_pct'))}) and S&P 500 ({_num(es.get('close'))} / {_pct(es.get('change_pct'))}) futures signal the primary direction. Price is the primary sentiment driver; follow the tape, not the narrative.

### 2. Global Synchronization
DAX {_pct(dax.get('change_pct'))} · Nikkei {_pct(nkk.get('change_pct'))}. Assess whether risk assets are moving in concert or showing divergence — divergence is a warning, not a signal.

### 3. Economic Data & Catalysts
| Catalyst | Data/Event | Market Impact |
|---|---|---|
| Headlines | See below | Monitor for directional shift |
{chr(10).join(f"| News | {h['title'][:55]} | Assess on open |" for h in headlines[:3])}

### 4. Volatility & Risk Gauges
- VIX: {_num(vix_val.get('close'))} → **{vix_zone}**
- Risk Status: {vix_zone} — size positions accordingly.

### 5. Market Breadth
- S5FI inference: Context-dependent based on index performance.
- Internal Health: Confirm broad participation before adding exposure. Narrow breadth in a rising tape is a warning.

### 6. Fixed Income & Yields
- US 10Y: {_num(us10y.get('close'))}% — growth stocks face headwinds above 4.5%.
- Yield curve (10Y-2Y): watch for steepening as a risk-on signal.

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- Watch the 200-day MA on SPX; it remains the most respected institutional risk toggle.
- High conviction: only A+ setups with clean technicals and confirmed volume.

**The Analyst Lesson:**
> "The market will test your patience before it tests your thesis."

Tactical takeaway: In {vix_zone.split('(')[0].strip().lower()} conditions, reduce size, demand confirmation, and preserve capital for when the edge is clear.

---
*Headlines context:*
{hl_lines}
"""


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

async def generate_intelligence_brief(brief_type: str) -> dict[str, Any]:
    """
    Generate a Market Intelligence Brief.
    brief_type: "pre" | "post"
    Returns a dict safe to JSON-serialize and cache.
    """
    assert brief_type in ("pre", "post"), "brief_type must be 'pre' or 'post'"
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    now_et  = now_utc.astimezone(NY_TZ)
    time_label = now_et.strftime("%I:%M %p ET")

    macro, headlines = await asyncio.gather(
        _fetch_macro(),
        _gather_headlines(brief_type),
    )

    prompt = _build_prompt(brief_type, macro, headlines)
    markdown = await _call_gemini(prompt)

    if not markdown:
        # Gemini unavailable — use structured heuristic
        markdown = _heuristic_brief(brief_type, macro, headlines)

    return {
        "brief_type": brief_type,
        "generated_at_utc": now_utc.isoformat(),
        "gen_time_et": time_label,
        "markdown": markdown,
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
