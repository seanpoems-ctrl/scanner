from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from zoneinfo import ZoneInfo

try:
    from backend.scraper import fetch_series_snapshot
    from backend.market_time import is_nyse_trading_day_et
except ModuleNotFoundError:
    from scraper import fetch_series_snapshot
    from market_time import is_nyse_trading_day_et

NY_TZ = ZoneInfo("America/New_York")
BRIEF_PATH = os.path.join(os.path.dirname(__file__), "data", "premarket_brief.json")
POST_BRIEF_PATH = os.path.join(os.path.dirname(__file__), "data", "postmarket_brief.json")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _pct_str(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "—"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def _num_str(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "—"
    if x >= 1000:
        return f"{int(round(x)):,}"
    if x >= 100:
        return f"{x:.1f}"
    return f"{x:.2f}"


async def _fetch_google_news_rss(
    query: str,
    *,
    limit: int = 8,
    hl: str = "en-US",
    gl: str = "US",
    ceid: str = "US:en",
) -> list[dict[str, str]]:
    q = (query or "").strip()
    if not q:
        return []
    url = "https://news.google.com/rss/search"
    params = {"q": q, "hl": hl, "gl": gl, "ceid": ceid}
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        xml = r.text

    # Minimal RSS parsing without extra deps.
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []
    items: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if not title:
            continue
        items.append({"title": title, "link": link, "pubDate": pub})
        if len(items) >= limit:
            break
    return items


async def _fetch_google_news_rss_with_client(
    client: httpx.AsyncClient,
    query: str,
    *,
    limit: int,
    hl: str,
    gl: str,
    ceid: str,
) -> list[dict[str, str]]:
    q = (query or "").strip()
    if not q:
        return []
    url = "https://news.google.com/rss/search"
    params = {"q": q, "hl": hl, "gl": gl, "ceid": ceid}
    r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    xml = r.text

    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []
    items: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if not title:
            continue
        items.append({"title": title, "link": link, "pubDate": pub})
        if len(items) >= limit:
            break
    return items


def _headline_key(title: str) -> str:
    # Normalize headline text to dedupe near-identical stories.
    cleaned = re.sub(r"[^a-z0-9\s]", " ", str(title or "").lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _dedupe_headlines(items: list[dict[str, str]], *, limit: int) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for h in items:
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        key = _headline_key(title)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "title": title,
                "link": str(h.get("link") or "").strip(),
                "pubDate": str(h.get("pubDate") or "").strip(),
            }
        )
        if len(out) >= limit:
            break
    return out


def _categorize_headline(title: str) -> str:
    t = str(title or "").lower()
    if any(k in t for k in ["cpi", "pce", "inflation", "ppi", "jobs", "payroll", "unemployment", "ism", "gdp", "retail sales", "consumer", "housing"]):
        return "Macro"
    if any(k in t for k in ["fed", "fomc", "powell", "ecb", "boe", "boj", "central bank", "rate", "rates", "yield", "treasury", "bond"]):
        return "Rates"
    if any(k in t for k in ["oil", "crude", "opec", "gas", "gold", "copper", "commodity", "commodities", "shipping", "freight"]):
        return "Commodities"
    if any(k in t for k in ["dollar", "usd", "eur", "yen", "cny", "yuan", "fx", "currency", "forex"]):
        return "FX"
    if any(k in t for k in ["war", "ceasefire", "missile", "attack", "sanction", "tariff", "geopolit", "china", "taiwan", "iran", "israel", "russia", "ukraine"]):
        return "Geopolitics"
    if any(k in t for k in ["earnings", "guidance", "beats", "misses", "raises", "lowers", "profit", "revenue", "after hours", "pre-market", "premarket"]):
        return "Earnings"
    return "News"


def _build_catalyst_bullets(
    *,
    headlines: list[dict[str, str]],
    limit: int = 5,
) -> list[str]:
    """
    Turn the brief's catalyst "prompt" into a few actionable bullets.
    We avoid long headline dumps by selecting a small, categorized set.
    """
    titles = [str(h.get("title") or "").strip() for h in (headlines or []) if str(h.get("title") or "").strip()]
    if not titles:
        return [
            "Scan: Fed speakers, CPI/PCE, jobs data, large-cap earnings, and energy/FX shocks.",
            "If rates/volatility are unstable, reduce size and demand cleaner confirmation at the open.",
        ]

    picked: list[str] = []
    seen_cat: set[str] = set()
    for title in titles:
        cat = _categorize_headline(title)
        if cat in seen_cat and cat not in {"News"}:
            continue
        seen_cat.add(cat)
        picked.append(f"{cat}: {title}")
        if len(picked) >= limit:
            break
    return picked


async def _fetch_multi_news(
    *,
    queries: list[str],
    locales: list[tuple[str, str, str]],
    per_query_limit: int = 6,
    total_limit: int = 12,
) -> list[dict[str, str]]:
    if not queries or not locales:
        return []
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        tasks: list[asyncio.Task] = []
        for q in queries:
            for hl, gl, ceid in locales:
                tasks.append(asyncio.create_task(_fetch_google_news_rss_with_client(client, q, limit=per_query_limit, hl=hl, gl=gl, ceid=ceid)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
    merged: list[dict[str, str]] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        merged.extend(r)
    return _dedupe_headlines(merged, limit=total_limit)


async def _tv_snap(symbol: str) -> dict[str, Any]:
    try:
        sym_u = (symbol or "").strip().upper()
        timeout_s = 20.0 if sym_u in {"ICE:BAMLH0A0HYM2", "FRED:BAMLH0A0HYM2", "BAMLH0A0HYM2"} else 8.0
        _resolved, data = await asyncio.wait_for(fetch_series_snapshot(symbol), timeout=timeout_s)
        return data or {}
    except Exception:
        return {}


async def _fetch_macro_snapshot() -> dict[str, Any]:
    symbols = {
        "nasdaq_fut": "CME_MINI:NQ1!",
        "spx_fut": "CME_MINI:ES1!",
        "vix": "CBOE:VIX",
        "us10y": "TVC:US10Y",
        "dax": "TVC:DAX",
        "ftse": "TVC:UKX",
        "stoxx50": "TVC:SX5E",
        "nikkei": "TVC:NI225",
        "hang_seng": "TVC:HSI",
        "kospi": "TVC:KOSPI",
    }
    snaps = await asyncio.gather(*( _tv_snap(sym) for sym in symbols.values() ))
    out: dict[str, Any] = {}
    for (k, sym), data in zip(symbols.items(), snaps, strict=False):
        out[k] = {
            "symbol": sym,
            "close": data.get("close"),
            "change_pct": data.get("change"),
            "description": data.get("description"),
        }
    return out


def _market_mood(nq: dict[str, Any], es: dict[str, Any]) -> str:
    nq_chg = nq.get("change_pct")
    es_chg = es.get("change_pct")
    try:
        nq_v = float(nq_chg)
        es_v = float(es_chg)
        avg = (nq_v + es_v) / 2.0
    except Exception:
        return "Futures tone is mixed; monitor opening breadth for confirmation."
    if avg > 0.7:
        return "Risk-on tone: futures bid, growth leadership favored into the open."
    if avg > 0.15:
        return "Constructive tone: modest bid in futures; stay selective and prioritize liquid leaders."
    if avg < -0.7:
        return "Risk-off tone: futures heavy; prioritize defense and wait for confirmation."
    if avg < -0.15:
        return "Cautious tone: futures soft; focus on A+ setups with tight risk."
    return "Neutral tone: futures flat; let early price action dictate aggression."


def _sync_note(eu: dict[str, Any], asia: dict[str, Any]) -> str:
    # Simple heuristic: compare average EU vs Asia returns.
    def avg(keys: list[str]) -> float | None:
        vals: list[float] = []
        for k in keys:
            try:
                vals.append(float((eu if k in eu else asia)[k].get("change_pct")))
            except Exception:
                continue
        return sum(vals) / len(vals) if vals else None

    eu_avg = avg(["dax", "ftse", "stoxx50"])
    as_avg = avg(["nikkei", "hang_seng", "kospi"])
    if eu_avg is None or as_avg is None:
        return "Global sync: partial data — watch EU/Asia follow-through vs US futures."
    if eu_avg > 0.3 and as_avg > 0.3:
        return "Global sync: broad risk-on (EU + Asia green) supports US upside continuation."
    if eu_avg < -0.3 and as_avg < -0.3:
        return "Global sync: broad risk-off (EU + Asia red) raises downside risk for US open."
    if eu_avg > 0.3 and as_avg < -0.3:
        return "Global sync: Europe strong but Asia weak — expect choppy US open and rotation risk."
    if eu_avg < -0.3 and as_avg > 0.3:
        return "Global sync: Asia strong but Europe weak — mixed macro impulse; trade what you see."
    return "Global sync: mixed; prioritize price/volume confirmation in US leaders."


def _fmt_move(sym_label: str, snap: dict[str, Any]) -> str:
    # Keep ASCII-only separators to avoid mojibake in some terminals/JSON viewers.
    return f"{sym_label} {_pct_str(snap.get('change_pct'))}"


def _trend_word(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "mixed"
    if x > 0.25:
        return "higher"
    if x < -0.25:
        return "lower"
    return "flat"


def _regime_label(vix_close: Any, nq_chg: Any, es_chg: Any) -> tuple[str, str]:
    """
    Returns (regime_label, focus_directive) synthesizing VIX + futures.
    regime_label: 'Risk-On', 'Selective', 'Defensive', 'Extreme Caution'
    focus_directive: actionable one-liner for traders.
    """
    try:
        vix_v = float(vix_close) if vix_close is not None else 20.0
    except Exception:
        vix_v = 20.0
    try:
        avg_fut = ((float(nq_chg) if nq_chg is not None else 0.0) + (float(es_chg) if es_chg is not None else 0.0)) / 2.0
    except Exception:
        avg_fut = 0.0

    if vix_v >= 30:
        return "Extreme Caution", "VIX ≥30 — stay flat or hedge; only trade the highest-conviction A+ setups."
    if vix_v >= 22:
        if avg_fut < -0.3:
            return "Defensive", "Elevated VIX + futures weak — reduce size, wait for breadth confirmation."
        return "Selective", "Elevated VIX — focus on liquid A+ Stage-2 leaders with tight risk parameters."
    if avg_fut > 0.5 and vix_v < 18:
        return "Risk-On", "Low VIX + strong futures — A+ EP setups favored; press confirmed breakouts."
    if avg_fut > 0.15:
        return "Selective", "Constructive tone — prioritize A+ names with clean EMA stacks and high ADDV."
    if avg_fut < -0.5:
        return "Defensive", "Futures weak — reduce size and require tighter confirmation before entries."
    return "Selective", "Mixed tone — trade what you see; only press if early breadth and volume confirm."


def _build_narrative(macro: dict[str, Any], headlines: list[dict[str, str]]) -> list[str]:
    nq = macro.get("nasdaq_fut", {})
    es = macro.get("spx_fut", {})
    vix = macro.get("vix", {})
    us10y = macro.get("us10y", {})
    dax = macro.get("dax", {})
    ftse = macro.get("ftse", {})
    stoxx50 = macro.get("stoxx50", {})
    nikkei = macro.get("nikkei", {})
    hsi = macro.get("hang_seng", {})
    kospi = macro.get("kospi", {})

    nq_chg = nq.get("change_pct")
    es_chg = es.get("change_pct")
    vix_close = vix.get("close")

    regime, focus = _regime_label(vix_close, nq_chg, es_chg)
    mood_line = _market_mood(nq, es)

    try:
        pair_chg = ((float(nq_chg) if nq_chg is not None else 0.0) + (float(es_chg) if es_chg is not None else 0.0)) / 2.0
    except Exception:
        pair_chg = None

    regime_line = (
        f"Market Regime: {regime}. {focus}"
    )

    us_tone = (
        f"US futures: Nasdaq and S&P are {_trend_word(pair_chg)} overall. "
        f"{mood_line}"
    )
    vol_rates = (
        f"Risk gates — VIX {_num_str(vix_close)} ({_pct_str(vix.get('change_pct'))}), "
        f"US10Y {_num_str(us10y.get('close'))} ({_pct_str(us10y.get('change_pct'))}). "
        "Rising volatility or yields demand tighter sizing and cleaner entry confirmation."
    )

    eu_line = (
        f"Europe {_trend_word((dax.get('change_pct') or 0) + (ftse.get('change_pct') or 0) + (stoxx50.get('change_pct') or 0))} "
        f"(DAX {_pct_str(dax.get('change_pct'))} · FTSE {_pct_str(ftse.get('change_pct'))} · STOXX50 {_pct_str(stoxx50.get('change_pct'))})."
    )
    as_line = (
        f"Asia {_trend_word((hsi.get('change_pct') or 0) + (nikkei.get('change_pct') or 0) + (kospi.get('change_pct') or 0))} "
        f"(Nikkei {_pct_str(nikkei.get('change_pct'))} · HSI {_pct_str(hsi.get('change_pct'))} · KOSPI {_pct_str(kospi.get('change_pct'))})."
    )
    sync = _sync_note(macro, macro)
    global_sync = f"{eu_line} {as_line} {sync}"

    actionable = (
        "Execution: treat the open as a confirmation test. "
        "Prioritize liquid A+ Stage-2 leaders (price > 10EMA > 20EMA > 50EMA > 200EMA, ADDV > $100M, ADR > 4.5%). "
        "EP setups take priority on confirmed volume surge; U&R and Pullback setups require the EMA level to hold as support. "
        "If VIX is elevated, stay defensive and wait for structure."
    )

    return [
        regime_line,
        us_tone,
        vol_rates,
        global_sync,
        actionable,
    ]


@dataclass(slots=True)
class PremarketBrief:
    generated_at_utc: str
    scheduled_for_et: str
    narrative: list[str]
    sections: list[dict[str, Any]]
    headlines: list[dict[str, str]]


class PremarketBriefStore:
    def __init__(self, path: str = BRIEF_PATH) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._cached: dict[str, Any] | None = None

    async def load(self) -> dict[str, Any] | None:
        async with self._lock:
            if self._cached is not None:
                return self._cached
            if not os.path.exists(self.path):
                return None
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._cached = json.load(f)
            except Exception:
                self._cached = None
            return self._cached

    async def save(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=False)
            self._cached = payload


class PostmarketBriefStore(PremarketBriefStore):
    def __init__(self, path: str = POST_BRIEF_PATH) -> None:
        super().__init__(path=path)


def next_release_et(now_utc: datetime | None = None) -> datetime:
    now = now_utc or _utc_now()
    now_et = now.astimezone(NY_TZ)
    target = now_et.replace(hour=8, minute=3, second=0, microsecond=0)
    while True:
        if now_et < target and is_nyse_trading_day_et(target.astimezone(timezone.utc)):
            break
        target = (target + timedelta(days=1)).replace(hour=8, minute=3, second=0, microsecond=0)
    return target


def next_postmarket_release_et(now_utc: datetime | None = None) -> datetime:
    now = now_utc or _utc_now()
    now_et = now.astimezone(NY_TZ)
    target = now_et.replace(hour=16, minute=33, second=0, microsecond=0)
    while True:
        if now_et < target and is_nyse_trading_day_et(target.astimezone(timezone.utc)):
            break
        target = (target + timedelta(days=1)).replace(hour=16, minute=33, second=0, microsecond=0)
    return target


def _build_postmarket_narrative(macro: dict[str, Any], headlines: list[dict[str, str]]) -> list[str]:
    nq = macro.get("nasdaq_fut", {})
    es = macro.get("spx_fut", {})
    vix = macro.get("vix", {})
    us10y = macro.get("us10y", {})

    vix_close = vix.get("close")
    nq_chg = nq.get("change_pct")
    es_chg = es.get("change_pct")

    regime, focus = _regime_label(vix_close, nq_chg, es_chg)
    key_moves = ", ".join(
        [
            _fmt_move("NQ", nq),
            _fmt_move("ES", es),
            _fmt_move("VIX", vix),
            _fmt_move("US10Y", us10y),
        ]
    )
    return [
        f"Market Regime (close): {regime}. {focus}",
        f"Session recap: {key_moves}.",
        "Post-close focus: carry forward only A+ leaders that held key EMAs into the close; trim laggards and reset risk.",
        "Plan for tomorrow: review overnight catalysts, earnings movers, and any after-hours gaps before the pre-market brief.",
    ]


async def generate_postmarket_brief() -> dict[str, Any]:
    macro = await _fetch_macro_snapshot()
    us_headlines = await _fetch_multi_news(
        queries=[
            "US stock market close Fed earnings after hours",
            "S&P 500 Nasdaq close market recap",
            "after hours movers earnings guidance",
        ],
        locales=[("en-US", "US", "US:en")],
        per_query_limit=5,
        total_limit=8,
    )
    global_headlines = await _fetch_multi_news(
        queries=[
            "global markets overnight Asia Europe stocks bonds",
            "ECB BOE BOJ central bank rates inflation",
            "geopolitics oil commodities currency volatility",
            "China economy stimulus property yuan",
        ],
        locales=[("en-US", "US", "US:en"), ("en-GB", "GB", "GB:en"), ("en-AU", "AU", "AU:en")],
        per_query_limit=4,
        total_limit=10,
    )
    headlines = _dedupe_headlines([*global_headlines, *us_headlines], limit=14)

    narrative = _build_postmarket_narrative(macro, headlines)
    sections: list[dict[str, Any]] = [
        {
            "title": "Session recap (open → close)",
            "bullets": [
                f"Nasdaq (NQ1!) { _num_str(macro.get('nasdaq_fut', {}).get('close')) } ({ _pct_str(macro.get('nasdaq_fut', {}).get('change_pct')) })",
                f"S&P 500 (ES1!) { _num_str(macro.get('spx_fut', {}).get('close')) } ({ _pct_str(macro.get('spx_fut', {}).get('change_pct')) })",
                "Note: this is a macro snapshot; pair with your tape + theme leaders for the true session story.",
            ],
        },
        {
            "title": "Volatility + rates (risk gate)",
            "bullets": [
                f"VIX { _num_str(macro.get('vix', {}).get('close')) } ({ _pct_str(macro.get('vix', {}).get('change_pct')) })",
                f"US10Y { _num_str(macro.get('us10y', {}).get('close')) } ({ _pct_str(macro.get('us10y', {}).get('change_pct')) })",
                "If volatility expanded into the close, trim exposure and demand cleaner setups tomorrow.",
            ],
        },
        {
            "title": "Economic data / catalysts",
            "bullets": _build_catalyst_bullets(headlines=headlines, limit=5),
        },
        {
            "title": "Plan for tomorrow",
            "bullets": [
                "Carry forward only the highest-quality leaders (A+ rubric) that held key EMAs into the close.",
                "Prepare: earnings, macro prints, and any after-hours movers that can gap the open.",
            ],
        },
    ]

    now = _utc_now()
    release_et = next_postmarket_release_et(now).astimezone(NY_TZ)
    payload = {
        "generated_at_utc": _iso(now),
        "scheduled_for_et": _iso(release_et),
        "macro": macro,
        "narrative": narrative,
        "sections": sections,
        "headlines": headlines,
        "headlines_global": global_headlines,
        "headlines_us": us_headlines,
        "source": {"markets": "tradingview", "news": "google_news_rss"},
    }
    return payload


async def scheduled_postmarket_loop(store: PostmarketBriefStore) -> None:
    """
    In-process scheduler: generates the brief every weekday at 4:33pm ET.
    """
    last_run_et_date: str | None = None
    while True:
        try:
            now = _utc_now()
            nxt = next_postmarket_release_et(now)
            sleep_for = max(5.0, (nxt.astimezone(timezone.utc) - now).total_seconds())
            await asyncio.sleep(min(sleep_for, 60.0))
            now2 = _utc_now()
            now2_et = now2.astimezone(NY_TZ)
            # Robust trigger: if server was asleep, still run once after the target time.
            target_et = datetime.combine(now2_et.date(), datetime.min.time(), tzinfo=NY_TZ).replace(hour=16, minute=33, second=0, microsecond=0)
            today_key = now2_et.date().isoformat()
            if (
                is_nyse_trading_day_et(now2)
                and now2_et >= target_et
                and last_run_et_date != today_key
            ):
                payload = await generate_postmarket_brief()
                await store.save(payload)
                last_run_et_date = today_key
                await asyncio.sleep(65.0)
        except Exception:
            await asyncio.sleep(10.0)


async def generate_premarket_brief() -> dict[str, Any]:
    macro = await _fetch_macro_snapshot()
    nq = macro.get("nasdaq_fut", {})
    es = macro.get("spx_fut", {})
    vix = macro.get("vix", {})
    us10y = macro.get("us10y", {})

    us_headlines = await _fetch_multi_news(
        queries=[
            "US premarket futures Fed CPI earnings",
            "US economic calendar jobs CPI PCE ISM",
            "premarket movers earnings guidance",
        ],
        locales=[("en-US", "US", "US:en")],
        per_query_limit=5,
        total_limit=8,
    )
    global_headlines = await _fetch_multi_news(
        queries=[
            "global markets overnight Asia Europe stocks bonds",
            "ECB BOE BOJ central bank inflation rates",
            "geopolitics oil commodities shipping sanctions",
            "China economy stimulus yuan property",
        ],
        locales=[("en-US", "US", "US:en"), ("en-GB", "GB", "GB:en"), ("en-AU", "AU", "AU:en")],
        per_query_limit=4,
        total_limit=10,
    )
    headlines = _dedupe_headlines([*global_headlines, *us_headlines], limit=14)

    narrative = _build_narrative(macro, headlines)
    mood = _market_mood(nq, es)
    sync = _sync_note(macro, macro)

    sections: list[dict[str, Any]] = [
        {
            "title": "US market mood (futures)",
            "bullets": [
                f"Nasdaq (NQ1!) { _num_str(nq.get('close')) } ({ _pct_str(nq.get('change_pct')) })",
                f"S&P 500 (ES1!) { _num_str(es.get('close')) } ({ _pct_str(es.get('change_pct')) })",
                mood,
            ],
        },
        {
            "title": "Global synchronization (EU + Asia)",
            "bullets": [
                f"DAX { _pct_str(macro.get('dax', {}).get('change_pct')) } · FTSE { _pct_str(macro.get('ftse', {}).get('change_pct')) } · STOXX50 { _pct_str(macro.get('stoxx50', {}).get('change_pct')) }",
                f"Nikkei { _pct_str(macro.get('nikkei', {}).get('change_pct')) } · Hang Seng { _pct_str(macro.get('hang_seng', {}).get('change_pct')) } · Kospi { _pct_str(macro.get('kospi', {}).get('change_pct')) }",
                sync,
            ],
        },
        {
            "title": "Volatility + yields",
            "bullets": [
                f"VIX { _num_str(vix.get('close')) } ({ _pct_str(vix.get('change_pct')) })",
                f"US10Y { _num_str(us10y.get('close')) } ({ _pct_str(us10y.get('change_pct')) })",
            ],
        },
        {
            "title": "Economic data / catalysts",
            "bullets": _build_catalyst_bullets(headlines=headlines, limit=5),
        },
        {
            "title": "Actionable technicals / lessons",
            "bullets": [
                "Trade only A+ stage‑2 names: price > 10EMA > 20EMA > 50EMA > 200EMA, high liquidity, ADR > 4.5%.",
                "If futures are flat but credit/vol deteriorate, reduce size and demand cleaner entries.",
            ],
        },
    ]

    now = _utc_now()
    release_et = next_release_et(now).astimezone(NY_TZ)
    payload = {
        "generated_at_utc": _iso(now),
        "scheduled_for_et": _iso(release_et),
        "macro": macro,
        "narrative": narrative,
        "sections": sections,
        "headlines": headlines,
        "headlines_global": global_headlines,
        "headlines_us": us_headlines,
        "source": {"markets": "tradingview", "news": "google_news_rss"},
    }
    return payload


async def scheduled_premarket_loop(store: PremarketBriefStore) -> None:
    """
    In-process scheduler: generates the brief every weekday at 8:03am ET.
    """
    last_run_et_date: str | None = None
    while True:
        try:
            now = _utc_now()
            nxt = next_release_et(now)
            sleep_for = max(5.0, (nxt.astimezone(timezone.utc) - now).total_seconds())
            await asyncio.sleep(min(sleep_for, 60.0))
            # Re-check each minute; run once we're past target time.
            now2 = _utc_now()
            now2_et = now2.astimezone(NY_TZ)
            # Robust trigger: if server was asleep, still run once after the target time.
            target_et = datetime.combine(now2_et.date(), datetime.min.time(), tzinfo=NY_TZ).replace(hour=8, minute=3, second=0, microsecond=0)
            today_key = now2_et.date().isoformat()
            if (
                is_nyse_trading_day_et(now2)
                and now2_et >= target_et
                and last_run_et_date != today_key
            ):
                payload = await generate_premarket_brief()
                await store.save(payload)
                # Prevent double-run within the same minute.
                last_run_et_date = today_key
                await asyncio.sleep(65.0)
        except Exception:
            await asyncio.sleep(10.0)

