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
import yfinance as yf
from zoneinfo import ZoneInfo

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

# Last-known-good cache: {key -> {"close": float, "change_pct": float}}
_MACRO_CACHE: dict[str, dict] = {}


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
    """Fetch all macro symbols concurrently with alias fallback + cache."""
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
                if len(out) >= 12:
                    return out
    return out


# ---------------------------------------------------------------------------
# Prompt builder — 7 Pillars + Catalyst JSON mandate
# ---------------------------------------------------------------------------

def _build_prompt(brief_type: str, macro: dict[str, dict], headlines: list[dict]) -> str:
    now_et = datetime.now(NY_TZ)
    date_label = now_et.strftime("%A %B %d, %Y")

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

    # Derive yield curve
    try:
        spread = float(us10y.get("close") or 0) - float(us2y.get("close") or 0)
        spread_str = f"{spread:+.2f}%"
    except Exception:
        spread_str = "n/a"

    hl_block = "\n".join(f"- {h['title']}" for h in headlines[:10]) or "No headlines available."
    brief_word = "Pre-Market" if brief_type == "pre" else "Post-Market"

    return f"""You are a senior institutional equity analyst. Generate a {brief_word} Market Intelligence Brief.

DATE: {date_label}

===== LIVE MARKET DATA (DO NOT output "—" or empty values for these — use the exact numbers below) =====
Nasdaq Futures (NQ):   {_price_line(nq.get('close'), nq.get('change_pct'))}
S&P 500 Futures (ES):  {_price_line(es.get('close'), es.get('change_pct'))}
Russell 2000 (RTY):    {_price_line(rty.get('close'), rty.get('change_pct'))}
VIX:                   {_num(vix.get('close'))} ({_pct(vix.get('change_pct'))})
US 10Y Yield:          {_num(us10y.get('close'))}%  ({_pct(us10y.get('change_pct'))})
US 2Y Yield:           {_num(us2y.get('close'))}%  ({_pct(us2y.get('change_pct'))})
Yield Curve (10Y-2Y):  {spread_str}
DXY (Dollar Index):    {_num(dxy.get('close'))} ({_pct(dxy.get('change_pct'))})
Gold (GC):             {_num(gold.get('close'))} ({_pct(gold.get('change_pct'))})
WTI Crude (CL):        {_num(oil.get('close'))} ({_pct(oil.get('change_pct'))})
DAX:                   {_pct(dax.get('change_pct'))}  |  FTSE 100: {_pct(ftse.get('change_pct'))}  |  EURO STOXX 50: {_pct(stoxx.get('change_pct'))}
Nikkei 225:            {_pct(nkk.get('change_pct'))}  |  Hang Seng: {_pct(hsi.get('change_pct'))}  |  KOSPI: {_pct(ksp.get('change_pct'))}
========================================================================================================

TODAY'S HEADLINES:
{hl_block}

===== STRICT OUTPUT RULES =====
1. FORBIDDEN: Never output "—", "N/A", "()", or blank cells anywhere. Use the data provided above.
2. The ## Gen header must use EXACTLY "## Gen [TIME] ET" where [TIME] is the actual current generation time.
3. Pillar 3 (Catalysts) MUST be output as a fenced JSON block using this exact schema — NO markdown table:
```json
[
  {{"catalyst": "string", "event": "string", "impact": "string", "impact_level": "High|Medium|Low"}},
  ...3-5 items total...
]
```
4. Tone: grounded, skeptical, senior PM. No generic disclaimers. Every sentence earns its place.
5. Use (Price / Change) format for index levels, e.g. "NQ 19,847 / +0.42%"

===== OUTPUT STRUCTURE =====

## Gen {now_et.strftime("%I:%M %p")} ET

### 1. US Market Mood
2-3 sentences. Reference NQ and ES prices and changes. State the primary sentiment driver.

### 2. Global Synchronization
EU and Asian coupling assessment. Format: "DAX [change] · FTSE [change] · STOXX [change]". Call out divergence if EU and Asia disagree.

### 3. Economic Data & Catalysts
Output ONLY the JSON block (no table, no extra text outside the block):
```json
[{{"catalyst": "...", "event": "...", "impact": "...", "impact_level": "High|Medium|Low"}}]
```

### 4. Volatility & Risk Gauges
- VIX [exact level]: [zone — Green <15 | Yellow 15-25 | Red 25-35 | Extreme >35]
- Yield curve [spread]: [steepening/flattening/inverted] — implication
- **Risk Status: [color zone]** — one-line justification

### 5. Market Breadth
- S5FI estimate: [range]% — [Internal Health: Broad/Narrow/Mixed]
- One sentence on whether the move is confirmed by breadth or suspect

### 6. Fixed Income & Yields
- 10Y at [yield]%: [headwind/tailwind for growth stocks, with specific threshold]
- Curve [steepening/flattening]: [30-day implication in one sentence]

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- Key SPX level: [specific price — 200d MA, pivot, support/resistance]
- High-conviction setup: [sector or pattern to watch today]

**The Analyst Lesson:**
> [One non-generic, memorable trading observation — quote style]

Tactical takeaway: [One specific, data-driven action sentence for today]

---
"""


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
            "temperature": 0.65,
            "maxOutputTokens": 2000,
            "topP": 0.92,
        },
    }
    url = f"{_GEMINI_URL}?key={_GEMINI_API_KEY}"
    for attempt in range(1, attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                r = await client.post(url, json=body)
                r.raise_for_status()
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                return text.strip()
        except Exception as exc:
            log.warning("Gemini attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                await asyncio.sleep(4 * attempt)
    return None


# ---------------------------------------------------------------------------
# Catalyst JSON extractor
# ---------------------------------------------------------------------------

def _extract_catalysts(markdown: str) -> list[dict]:
    """
    Parse the ```json [...] ``` block in Pillar 3 into a list of catalyst dicts.
    Returns [] if parsing fails (frontend falls back to plain text rendering).
    """
    m = re.search(r"```json\s*(\[.*?\])\s*```", markdown, re.DOTALL)
    if not m:
        # Try relaxed: bare JSON array between ### 3 and ### 4
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
            out.append({
                "catalyst":     str(item.get("catalyst") or "").strip(),
                "event":        str(item.get("event") or "").strip(),
                "impact":       str(item.get("impact") or "").strip(),
                "impact_level": str(item.get("impact_level") or "Medium").strip(),
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

    # Catalyst JSON block
    catalyst_rows = []
    for h in headlines[:4]:
        catalyst_rows.append({
            "catalyst": "Headline",
            "event": h["title"][:60],
            "impact": "Monitor for directional shift at open",
            "impact_level": "Medium",
        })
    catalyst_json = json.dumps(catalyst_rows, indent=2)

    return f"""## Gen {time_label}

### 1. US Market Mood
{brief_word} session. Nasdaq futures at {_num(nq.get('close'))} ({_pct(nq.get('change_pct'))}), S&P 500 at {_num(es.get('close'))} ({_pct(es.get('change_pct'))}). Price action is the primary signal; follow the tape and not the narrative.

### 2. Global Synchronization
DAX {_pct(dax.get('change_pct'))} · FTSE {_pct(ftse.get('change_pct'))} · Nikkei {_pct(nkk.get('change_pct'))} · Hang Seng {_pct(hsi.get('change_pct'))}. Assess whether risk assets are aligned or diverging — divergence is a warning sign.

### 3. Economic Data & Catalysts
```json
{catalyst_json}
```

### 4. Volatility & Risk Gauges
- VIX: {vix_str}
- Yield curve (10Y-2Y): {curve_str}
- **Risk Status: {vix_zone}** — size positions accordingly.

### 5. Market Breadth
- S5FI estimate: context-dependent on index context — Internal Health: verify participation.
- A rising tape on narrow breadth is a caution signal; demand broad confirmation before adding size.

### 6. Fixed Income & Yields
- US 10Y at {_num(us10y.get('close'))}%: growth stocks face headwinds above 4.5%.
- Yield curve {curve_str}: watch for steepening as a risk-on signal for cyclicals.

### 7. Actionable Technicals & The Analyst Lesson
**Technicals:**
- Key SPX level: watch the 200-day MA — the most respected institutional risk toggle.
- High conviction: only A+ setups with confirmed volume; no chasing extended names.

**The Analyst Lesson:**
> "The market will test your patience before it tests your thesis."

Tactical takeaway: In {vix_zone.split('(')[0].strip().lower()} conditions, reduce position size, demand confirmation, and preserve capital for high-conviction A+ setups.

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

    # Replace the placeholder time in ## Gen header with the actual finish time
    markdown = re.sub(r"^## Gen .+? ET", f"## Gen {time_label}", markdown, count=1, flags=re.MULTILINE)

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
