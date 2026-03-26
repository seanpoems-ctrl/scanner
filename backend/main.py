import asyncio
import hashlib
import json
import re
from dataclasses import asdict
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Query as FQuery
from fastapi.middleware.cors import CORSMiddleware

from time import monotonic

from yfinance.exceptions import YFRateLimitError

from backend.premarket_tv import PremarketTvParams, run_premarket_tv_scan_sync
from backend.scraper import (
    build_theme_leaderboard,
    extract_us_ticker_from_tv_scan_row,
    fetch_finviz_industry_filter_map,
    fetch_finviz_short_float_pct_batch,
    fetch_yfinance_short_percent_float_pct_batch,
    fetch_tradingview_tape,
)
from backend.theme_universe import ThemeUniverseStore, scheduled_refresh_loop
from backend.news_brief import (
    PremarketBriefStore,
    PostmarketBriefStore,
    scheduled_premarket_loop,
    scheduled_postmarket_loop,
    generate_premarket_brief,
    generate_postmarket_brief,
    is_nyse_trading_day_et,
)

import yfinance as yf

# Per-view cache with short TTL so polling refreshes Finviz-backed leaderboards.
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_SEC = 55.0
_THEME_UNIVERSE = ThemeUniverseStore()
_UNIVERSE_TASK: asyncio.Task[None] | None = None
_NEWS_STORE = PremarketBriefStore()
_POST_NEWS_STORE = PostmarketBriefStore()
_NEWS_TASK: asyncio.Task[None] | None = None
_POST_NEWS_TASK: asyncio.Task[None] | None = None

_PREMARKET_GAP_CACHE: dict[str, tuple[float, dict]] = {}
_PREMARKET_GAP_TTL_SEC = 50.0

# Cache ticker intel to avoid Yahoo throttling.
_TICKER_INTEL_CACHE: dict[str, tuple[float, dict]] = {}
_TICKER_INTEL_TTL_SEC = 6 * 60.0

_TICKER_SUGGEST_CACHE: dict[str, tuple[float, list[dict]]] = {}
_TICKER_SUGGEST_TTL_SEC = 6 * 60.0

# Adaptive backoff for upstream 429s (Finviz/Yahoo). Frontend reads these headers.
_POLL_BASE_SEC = 110.0
_POLL_MAX_SEC = 8 * 60.0
_POLL_SEC = _POLL_BASE_SEC
_BACKOFF_UNTIL = 0.0


def _backoff_note(now: float) -> tuple[bool, int, int]:
    active = now < _BACKOFF_UNTIL
    retry = int(max(1.0, (_BACKOFF_UNTIL - now))) if active else int(_POLL_BASE_SEC)
    poll = int(_POLL_SEC)
    return active, poll, retry


def _on_upstream_429(now: float) -> int:
    global _POLL_SEC, _BACKOFF_UNTIL
    _POLL_SEC = min(_POLL_MAX_SEC, max(_POLL_BASE_SEC, _POLL_SEC * 2))
    _BACKOFF_UNTIL = now + _POLL_SEC
    return int(_POLL_SEC)


def _on_upstream_ok(now: float) -> None:
    global _POLL_SEC
    if now >= _BACKOFF_UNTIL and _POLL_SEC > _POLL_BASE_SEC:
        _POLL_SEC = max(_POLL_BASE_SEC, _POLL_SEC * 0.5)

app = FastAPI(title="POWER-THEME API", version="0.1.0")

# Vite may use 5173, 5174, … if the default port is busy — allow any local dev origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _attach_market_momentum(payload: dict) -> dict:
    themes = payload.get("themes", [])
    score = 0
    a_plus_count = 0
    a_count = 0
    for theme in themes:
        for stock in theme.get("stocks", []):
            grade = stock.get("gradeLabel", "-")
            if grade == "A+":
                score += 10
                a_plus_count += 1
            elif grade == "A":
                score += 4
                a_count += 1

    if score > 30:
        state = "bullish"
        message = "🔥 AGGRESSIVE BULLISH: Momentum Clusters Confirmed."
    elif score >= 10:
        state = "neutral"
        message = "⚖️ NEUTRAL: Selective Setups Only."
    else:
        state = "bearish"
        message = "🛡️ BEARISH: Defensive Positioning Required."

    payload["market_momentum_score"] = {
        "score": score,
        "state": state,
        "message": message,
        "aPlusCount": a_plus_count,
        "aCount": a_count,
    }
    return payload


@app.get("/api/themes")
async def get_themes(view: str = "themes") -> dict:
    v = view.strip().lower()
    if v == "industry":
        key = "industry"
    elif v in ("scanner", "legacy", "audit"):
        key = "scanner"
    else:
        key = "themes"
    now = monotonic()
    cached = _CACHE.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SEC:
        _on_upstream_ok(now)
        active, poll, retry = _backoff_note(now)
        out = dict(cached[1])
        out["polling"] = {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry}
        return out

    try:
        payload = _attach_market_momentum(await build_theme_leaderboard(leader_view=key))
    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response is not None else None
        if code == 429:
            wait = _on_upstream_429(now)
            raise HTTPException(
                status_code=429,
                detail="Upstream rate-limited. Backing off.",
                headers={"Retry-After": str(wait)},
            )
        raise
    except YFRateLimitError:
        wait = _on_upstream_429(now)
        raise HTTPException(
            status_code=429,
            detail="Upstream rate-limited. Backing off.",
            headers={"Retry-After": str(wait)},
        )

    _on_upstream_ok(now)
    _CACHE[key] = (now, payload)
    active, poll, retry = _backoff_note(now)
    payload["polling"] = {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry}
    try:
        payload["tape"] = await fetch_tradingview_tape()
    except Exception:
        payload["tape"] = []
    return payload


@app.on_event("startup")
async def _startup() -> None:
    global _UNIVERSE_TASK, _NEWS_TASK, _POST_NEWS_TASK
    await _THEME_UNIVERSE.load()
    # Refresh movers every 30 minutes (prices change; tickers-only automation).
    _UNIVERSE_TASK = asyncio.create_task(scheduled_refresh_loop(_THEME_UNIVERSE, every_sec=30 * 60))
    _NEWS_TASK = asyncio.create_task(scheduled_premarket_loop(_NEWS_STORE))
    _POST_NEWS_TASK = asyncio.create_task(scheduled_postmarket_loop(_POST_NEWS_STORE))


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _UNIVERSE_TASK, _NEWS_TASK, _POST_NEWS_TASK
    if _UNIVERSE_TASK is not None:
        _UNIVERSE_TASK.cancel()
        _UNIVERSE_TASK = None
    if _NEWS_TASK is not None:
        _NEWS_TASK.cancel()
        _NEWS_TASK = None
    if _POST_NEWS_TASK is not None:
        _POST_NEWS_TASK.cancel()
        _POST_NEWS_TASK = None


@app.get("/api/news/premarket")
async def get_premarket_brief() -> dict:
    cached = await _NEWS_STORE.load()
    return cached or {"generated_at_utc": None, "scheduled_for_et": None, "sections": [], "headlines": []}


@app.post("/api/news/premarket/refresh")
async def refresh_premarket_brief() -> dict:
    if not is_nyse_trading_day_et():
        raise HTTPException(
            status_code=400,
            detail="Pre-market briefs are generated on NYSE trading days only.",
        )
    payload = await generate_premarket_brief()
    await _NEWS_STORE.save(payload)
    return payload


@app.get("/api/news/postmarket")
async def get_postmarket_brief() -> dict:
    cached = await _POST_NEWS_STORE.load()
    return cached or {"generated_at_utc": None, "scheduled_for_et": None, "sections": [], "headlines": []}


@app.post("/api/news/postmarket/refresh")
async def refresh_postmarket_brief() -> dict:
    if not is_nyse_trading_day_et():
        raise HTTPException(
            status_code=400,
            detail="Post-market briefs are generated on NYSE trading days only.",
        )
    payload = await generate_postmarket_brief()
    await _POST_NEWS_STORE.save(payload)
    return payload


@app.get("/api/scanner/premarket-gappers")
async def get_premarket_gappers_endpoint(
    min_gap_pct: float = FQuery(0, ge=0, description="Minimum premarket gap % (TradingView premarket_gap)"),
    min_pm_vol_k: float = FQuery(0, ge=0, description="Min premarket volume, thousands of shares"),
    min_price: float = FQuery(0, ge=0, description="Min last price (close)"),
    min_avg_vol_10d_k: float = FQuery(0, ge=0, description="Min 10d average volume, thousands of shares"),
    min_mkt_cap_b: float = FQuery(0, ge=0, description="Min market cap, billions USD"),
    min_avg_dollar_vol_m: float = FQuery(0, ge=0, description="Min est avg $ volume (10d avg vol × price), millions USD"),
    limit: int = FQuery(100, ge=10, le=500),
) -> dict:
    """Pre-market gappers via tradingview-screener (TV scanneramerica); cached ~50s per filter set."""
    global _PREMARKET_GAP_CACHE
    now = monotonic()
    params = PremarketTvParams(
        min_gap_pct=min_gap_pct,
        min_pm_vol_k=min_pm_vol_k,
        min_price=min_price,
        min_avg_vol_10d_k=min_avg_vol_10d_k,
        min_mkt_cap_b=min_mkt_cap_b,
        min_avg_dollar_vol_m=min_avg_dollar_vol_m,
        limit=limit,
    )
    cache_key = hashlib.sha256(
        json.dumps(asdict(params), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    hit = _PREMARKET_GAP_CACHE.get(cache_key)
    if hit is not None and (now - hit[0]) < _PREMARKET_GAP_TTL_SEC:
        return hit[1]
    try:
        data = await asyncio.to_thread(run_premarket_tv_scan_sync, params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    rows = data.get("rows") or []
    symbols_in_order = [extract_us_ticker_from_tv_scan_row(r) for r in rows]
    uniq_syms = [s for s in dict.fromkeys(s for s in symbols_in_order if s)]
    short_by_sym: dict[str, float | None] = {}
    short_source: str | None = None
    if uniq_syms:
        short_by_sym = await fetch_yfinance_short_percent_float_pct_batch(uniq_syms)
        short_source = "yfinance_short_percent_of_float"
        # Fallback: if Yahoo throttles / returns nothing, Finviz is a reliable secondary.
        if not any(v is not None for v in short_by_sym.values()):
            finviz = await fetch_finviz_short_float_pct_batch(uniq_syms)
            if any(v is not None for v in finviz.values()):
                short_by_sym = finviz
                short_source = "finviz_short_float"
    for r, sym in zip(rows, symbols_in_order):
        if sym and short_by_sym.get(sym) is not None:
            r["short_interest_pct"] = short_by_sym[sym]
    payload = {
        **data,
        "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "short_interest_source": short_source,
    }
    _PREMARKET_GAP_CACHE[cache_key] = (now, payload)
    return payload


@app.get("/api/theme-universe/spotlight")
async def get_theme_spotlight(label: str) -> dict:
    theme = await _THEME_UNIVERSE.find_by_label(label)
    if theme is None:
        raise HTTPException(status_code=404, detail="Theme not found in theme_universe.json")
    cached = await _THEME_UNIVERSE.get_cached_movers(theme)
    return cached or await _THEME_UNIVERSE.refresh_movers(theme)


@app.post("/api/theme-universe/refresh")
async def refresh_theme_universe() -> dict:
    return await _THEME_UNIVERSE.refresh_all_movers()


@app.post("/api/theme-universe/rebuild-tickers")
async def rebuild_theme_universe_tickers() -> dict:
    return await _THEME_UNIVERSE.rebuild_all_tickers()


@app.post("/api/theme-universe/bootstrap")
async def bootstrap_theme_universe(count: int = 20) -> dict:
    """
    Auto-add the top N Themes and top N Industries by 6M % into theme_universe.json.
    This does NOT auto-populate tickers (requires per-theme `source` or manual tickers).
    """
    if count <= 0 or count > 200:
        raise HTTPException(status_code=400, detail="count must be between 1 and 200")

    themes_payload, industry_payload, industry_filter_map = await asyncio.gather(
        build_theme_leaderboard(leader_view="themes"),
        build_theme_leaderboard(leader_view="industry"),
        fetch_finviz_industry_filter_map(),
    )

    def top_by_6m(rows: list[dict], n: int) -> list[dict]:
        def v(r: dict) -> float:
            x = r.get("perf6M")
            return float(x) if isinstance(x, (int, float)) else float("-inf")

        out = [r for r in rows if isinstance(r, dict)]
        out.sort(key=v, reverse=True)
        return out[:n]

    top_themes = top_by_6m(themes_payload.get("themes", []), count)
    top_industry = top_by_6m(industry_payload.get("themes", []), count)

    def slugify(label: str) -> str:
        s = "".join(ch.lower() if ch.isalnum() else " " for ch in (label or ""))
        s = " ".join(s.split()).strip()
        return ("-".join(s.split())[:80]) or "theme"

    upserts: list[dict] = []
    for r in top_themes:
        label = str(r.get("theme") or "").strip()
        bucket = label.split("·")[0].strip() if "·" in label else "Themes"
        bnorm = " ".join(bucket.lower().split())
        # Best-effort deterministic sources for theme buckets (approximate constituents).
        bucket_filter_map = {
            "semiconductors": "ind_semiconductors",
            "space": "ind_aerospacedefense",
            "defense": "ind_aerospacedefense",
            "energy": "sec_energy",
            "commodities": "sec_basicmaterials",
            "transportation": "sec_industrials",
            "hardware": "sec_technology",
            "software": "sec_technology",
            "telecom": "ind_communicationequipment",
            "nanotech": "sec_technology",
            "vr / ar": "sec_technology",
        }
        filt = bucket_filter_map.get(bnorm)
        source = {"type": "finviz_screener", "path": f"/screener.ashx?v=111&f={filt}&o=-perf4w", "max_pages": 20} if filt else None
        upserts.append(
            {
                "slug": f"themes-{slugify(label)}",
                "label": label,
                "bucket": bucket,
                "source": source,
            }
        )
    for r in top_industry:
        name = str(r.get("theme") or "").strip()
        label = f"Industry · {name}" if name else "Industry · Unknown"
        ind_token = industry_filter_map.get(" ".join(name.lower().strip().split())) if name else None
        source = None
        if ind_token:
            source = {"type": "finviz_screener", "path": f"/screener.ashx?v=111&f={ind_token}&o=-perf4w", "max_pages": 20}
        upserts.append(
            {
                "slug": f"industry-{slugify(name)}",
                "label": label,
                "bucket": "Industry",
                "source": source,
            }
        )

    merge = await _THEME_UNIVERSE.upsert_themes(upserts)
    return {
        "requested": count,
        "added": merge["added"],
        "updated": merge["updated"],
        "total": merge["total"],
        "universe_updated_at": merge["updated_at"],
        "topThemes6M": [u["label"] for u in upserts[: len(top_themes)]],
        "topIndustry6M": [u["label"] for u in upserts[len(top_themes) :]],
        "note": "Tickers are not auto-populated; add tickers or a deterministic `source` per theme for automation.",
    }


def _norm_key(s: str) -> str:
    return " ".join((s or "").lower().strip().split())


_SECTOR_ETF_MAP: dict[str, str] = {
    "technology": "XLK",
    "communication services": "XLC",
    "consumer discretionary": "XLY",
    "consumer staples": "XLP",
    "energy": "XLE",
    "financial services": "XLF",
    "financial": "XLF",
    "healthcare": "XLV",
    "industrials": "XLI",
    "basic materials": "XLB",
    "materials": "XLB",
    "real estate": "XLRE",
    "utilities": "XLU",
}


@app.get("/api/ticker-intel")
async def get_ticker_intel(ticker: str) -> dict:
    """
    Ticker lookup used by the header search bar.
    Returns sector/industry plus best-effort theme/subtheme from theme_universe membership.
    """
    raw = (ticker or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="ticker is required")

    # Allow lookup by ticker OR company name. If this looks like a symbol, use it directly.
    # Otherwise, attempt yfinance search and pick best match.
    sym_candidate = re.sub(r"[^A-Za-z0-9\.\-]", "", raw.upper().strip())
    is_symbolish = bool(sym_candidate) and len(sym_candidate) <= 7 and " " not in raw

    def _resolve() -> str:
        if is_symbolish:
            return sym_candidate
        try:
            search = yf.Search(raw)  # yfinance Search (best-effort)
            quotes = getattr(search, "quotes", None) or []
            if isinstance(quotes, list) and quotes:
                s = str((quotes[0] or {}).get("symbol") or "").strip().upper()
                if s:
                    return re.sub(r"[^A-Za-z0-9\.\-]", "", s)
        except Exception:
            pass
        # Fallback: last resort, try treating the input as a symbol.
        return sym_candidate or raw.upper()

    t = await asyncio.to_thread(_resolve)
    if not t:
        raise HTTPException(status_code=404, detail="Unable to resolve query to a ticker")

    now = monotonic()
    cached = _TICKER_INTEL_CACHE.get(t)
    if cached is not None and (now - cached[0]) < _TICKER_INTEL_TTL_SEC:
        return cached[1]

    def _fetch() -> dict:
        tk = yf.Ticker(t)
        hist = tk.history(period="5d", interval="1d")
        info = tk.fast_info if hasattr(tk, "fast_info") else {}

        # `tk.info` is the most rate-limited call; keep it best-effort.
        info2: dict = {}
        try:
            info2 = tk.info if hasattr(tk, "info") else {}
        except Exception:
            info2 = {}

        close = float(hist["Close"].iloc[-1]) if not hist.empty else float(info.get("last_price") or 0)
        prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else close
        chg_pct = ((close - prev) / prev) * 100.0 if prev else 0.0
        name = str(info.get("shortName") or info2.get("shortName") or info2.get("longName") or t)
        sector = str(info2.get("sector") or "Unknown")
        industry = str(info2.get("industry") or "Unknown")
        return {
            "ticker": t,
            "name": name,
            "close": round(close, 2) if close else None,
            "today_return_pct": round(chg_pct, 2),
            "sector": sector,
            "industry": industry,
        }

    try:
        snap = await asyncio.to_thread(_fetch)
    except YFRateLimitError:
        # If we already have a stale cache, serve it; otherwise report throttling.
        cached2 = _TICKER_INTEL_CACHE.get(t)
        if cached2 is not None:
            return cached2[1]
        raise HTTPException(status_code=429, detail="Ticker lookup is temporarily rate-limited. Try again shortly.")

    # Theme/subtheme membership from universe (can be multiple; we display primary + count).
    themes = await _THEME_UNIVERSE.list_themes()
    matches = [th for th in themes if t in (th.tickers or [])]
    labels = [m.label for m in matches]

    primary_theme = None
    primary_subtheme = None
    if labels:
        # Prefer "Bucket · Subtheme" style when present.
        parts = [p.strip() for p in labels[0].split("·")]
        if len(parts) >= 2:
            primary_theme = parts[0]
            primary_subtheme = parts[1]
        else:
            primary_theme = labels[0]
            primary_subtheme = None

    sector_key = _norm_key(str(snap.get("sector") or ""))
    sector_etf = _SECTOR_ETF_MAP.get(sector_key)

    out = {
        **snap,
        "sector_etf": sector_etf,
        "theme": primary_theme,
        "subtheme": primary_subtheme,
        "theme_matches": labels,
    }
    _TICKER_INTEL_CACHE[t] = (now, out)
    return out


@app.get("/api/ticker-suggest")
async def ticker_suggest(q: str) -> dict:
    query = (q or "").strip()
    if len(query) < 1:
        return {"query": query, "results": []}

    key = _norm_key(query)[:80]
    now = monotonic()
    cached = _TICKER_SUGGEST_CACHE.get(key)
    if cached is not None and cached[1] and (now - cached[0]) < _TICKER_SUGGEST_TTL_SEC:
        return {"query": query, "results": cached[1]}

    sym_candidate = re.sub(r"[^A-Za-z0-9\.\-]", "", query.upper().strip())
    seed: list[dict] = []
    if re.fullmatch(r"[A-Z]{1,5}", sym_candidate or ""):
        seed.append({"ticker": sym_candidate, "name": ""})

    def _search() -> list[dict]:
        try:
            s = yf.Search(query)
            quotes = getattr(s, "quotes", None) or []
        except Exception:
            quotes = []
        out: list[dict] = []
        if isinstance(quotes, list):
            for raw in quotes[:10]:
                if not isinstance(raw, dict):
                    continue
                sym = str(raw.get("symbol") or "").strip().upper()
                name = str(raw.get("shortname") or raw.get("longname") or raw.get("name") or "").strip()
                if not sym:
                    continue
                cleaned = re.sub(r"[^A-Za-z0-9\.\-]", "", sym)
                if not cleaned:
                    continue
                # If search returns an exchange suffix (ASTS.MX), prefer the base symbol (ASTS).
                base = cleaned.split(".", 1)[0]
                if re.fullmatch(r"[A-Z]{1,5}", base or ""):
                    cleaned = base
                # Keep UI clean: prefer primary US-style tickers.
                if not re.fullmatch(r"[A-Z]{1,5}", cleaned):
                    continue
                out.append({"ticker": cleaned, "name": name})
        # De-dupe, keep order.
        seen: set[str] = set()
        dedup: list[dict] = []
        for r in out:
            t = str(r.get("ticker") or "")
            if not t or t in seen:
                continue
            seen.add(t)
            dedup.append(r)
        # Prepend seed symbol if present.
        if seed:
            for x in reversed(seed):
                dedup.insert(0, x)
        # De-dupe again after seed insert.
        seen2: set[str] = set()
        out2: list[dict] = []
        for r in dedup:
            t = str(r.get("ticker") or "")
            if not t or t in seen2:
                continue
            seen2.add(t)
            out2.append(r)
        return out2[:8]

    try:
        results = await asyncio.to_thread(_search)
    except YFRateLimitError:
        results = []

    if results:
        _TICKER_SUGGEST_CACHE[key] = (now, results)
    return {"query": query, "results": results}
