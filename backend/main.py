import asyncio
import hashlib
import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query as FQuery
from fastapi.middleware.cors import CORSMiddleware

from time import monotonic

from yfinance.exceptions import YFRateLimitError

try:
    # Local package-style imports (used in local dev from repo root).
    from backend.breadth import compute_market_ocean_sync, OceanSnapshot
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
        scheduled_premarket_loop,
        generate_premarket_brief,
        PostmarketBriefStore,
        scheduled_postmarket_loop,
        generate_postmarket_brief,
    )
    from backend.market_time import is_nyse_trading_day_et, market_status_dict
    from backend.earnings import EarningsCache, next_earnings_for_tickers
except ModuleNotFoundError:
    # Render may run with cwd at /backend, where absolute `backend.*` is unavailable.
    from premarket_tv import PremarketTvParams, run_premarket_tv_scan_sync
    from scraper import (
        build_theme_leaderboard,
        extract_us_ticker_from_tv_scan_row,
        fetch_finviz_industry_filter_map,
        fetch_finviz_short_float_pct_batch,
        fetch_yfinance_short_percent_float_pct_batch,
        fetch_tradingview_tape,
    )
    from theme_universe import ThemeUniverseStore, scheduled_refresh_loop
    from news_brief import (
        PremarketBriefStore,
        scheduled_premarket_loop,
        generate_premarket_brief,
        PostmarketBriefStore,
        scheduled_postmarket_loop,
        generate_postmarket_brief,
    )
    from breadth import compute_market_ocean_sync, OceanSnapshot
    from market_time import is_nyse_trading_day_et, market_status_dict
    from earnings import EarningsCache, next_earnings_for_tickers

import yfinance as yf

# Gate: market trading days only (XNYS).
def _is_weekday_et() -> bool:
    try:
        return is_nyse_trading_day_et(datetime.now(timezone.utc))
    except Exception:
        # If timezone info fails, don't block refresh.
        return True

# Per-view cache with short TTL so polling refreshes Finviz-backed leaderboards.
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_SEC = 55.0  # legacy per-request TTL (no longer primary gate)
_THEME_UNIVERSE = ThemeUniverseStore()
_UNIVERSE_TASK: asyncio.Task[None] | None = None
_PRE_NEWS_STORE = PremarketBriefStore()
_POST_NEWS_STORE = PostmarketBriefStore()
_PRE_NEWS_TASK: asyncio.Task[None] | None = None
_POST_NEWS_TASK: asyncio.Task[None] | None = None
_PRE_NEWS_REFRESH_TASK: asyncio.Task[None] | None = None
_POST_NEWS_REFRESH_TASK: asyncio.Task[None] | None = None

# Best solution: scheduled refresh + stale-while-revalidate snapshots.
_THEMES_REFRESH_SEC = 15 * 60  # refresh cadence for heavy scrapes
_THEMES_MAX_STALE_SEC = 6 * 60 * 60  # keep last-good for 6h even if upstream is down
_THEMES_LOCKS: dict[str, asyncio.Lock] = {"themes": asyncio.Lock(), "industry": asyncio.Lock(), "scanner": asyncio.Lock()}
_THEMES_TASK: asyncio.Task[None] | None = None

_THEMES_META: dict[str, dict] = {
    "themes": {"last_ok_monotonic": None, "last_ok_utc": None, "last_err": None, "refreshing": False},
    "industry": {"last_ok_monotonic": None, "last_ok_utc": None, "last_err": None, "refreshing": False},
    "scanner": {"last_ok_monotonic": None, "last_ok_utc": None, "last_err": None, "refreshing": False},
}


async def _refresh_themes_snapshot(key: str) -> None:
    """
    Refresh one snapshot and store it as last-known-good.
    Never raises to callers; keeps previous snapshot on errors/429.
    """
    if key not in _THEMES_LOCKS:
        return
    lock = _THEMES_LOCKS[key]
    if lock.locked():
        return
    async with lock:
        _THEMES_META[key]["refreshing"] = True
        try:
            now = monotonic()
            # Respect adaptive backoff: if active, don't hammer upstream.
            active, _poll, _retry = _backoff_note(now)
            if active:
                return
            payload = _attach_market_momentum(await build_theme_leaderboard(leader_view=key))
            try:
                payload["tape"] = await fetch_tradingview_tape()
            except Exception:
                payload["tape"] = payload.get("tape") or []
            _on_upstream_ok(now)
            _CACHE[key] = (now, payload)
            _THEMES_META[key]["last_ok_monotonic"] = now
            _THEMES_META[key]["last_ok_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            _THEMES_META[key]["last_err"] = None
        except httpx.HTTPStatusError as e:
            now = monotonic()
            code = e.response.status_code if e.response is not None else None
            if code == 429:
                _on_upstream_429(now)
                _THEMES_META[key]["last_err"] = "Upstream rate-limited (429). Keeping last snapshot."
            else:
                _THEMES_META[key]["last_err"] = f"Upstream HTTP error: {code}"
        except YFRateLimitError:
            now = monotonic()
            _on_upstream_429(now)
            _THEMES_META[key]["last_err"] = "Yahoo rate-limited (429). Keeping last snapshot."
        except Exception as e:
            _THEMES_META[key]["last_err"] = f"Refresh failed: {type(e).__name__}"
        finally:
            _THEMES_META[key]["refreshing"] = False


async def _themes_refresh_loop() -> None:
    # Warm cache early, then refresh on cadence.
    await asyncio.sleep(0.2)
    for k in ("themes", "industry"):
        await _refresh_themes_snapshot(k)
        await asyncio.sleep(0.25)

    while True:
        try:
            # Refresh both primary views (themes + industry). Scanner/audit is heavier; refresh on-demand.
            for k in ("themes", "industry"):
                await _refresh_themes_snapshot(k)
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never crash loop.
            pass
        await asyncio.sleep(_THEMES_REFRESH_SEC)

_PREMARKET_GAP_CACHE: dict[str, tuple[float, dict]] = {}
_PREMARKET_GAP_TTL_SEC = 50.0

_OCEAN_CACHE: dict[str, Any] | None = None
_OCEAN_CACHE_TS: float = 0.0
_OCEAN_CACHE_TTL_SEC = 20 * 60.0  # recompute at most every 20 min (Yahoo-heavy call)

# Cache ticker intel to avoid Yahoo throttling.
_TICKER_INTEL_CACHE: dict[str, tuple[float, dict]] = {}
_TICKER_INTEL_TTL_SEC = 6 * 60.0

_TICKER_SUGGEST_CACHE: dict[str, tuple[float, list[dict]]] = {}
_TICKER_SUGGEST_TTL_SEC = 6 * 60.0

_EARNINGS_CACHE = EarningsCache(ttl_sec=6 * 60.0)

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
    allow_origins=[
        "https://scanner-gules-rho.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app|http://(localhost|127\.0\.0\.1):\d+",
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

    # Scanner/audit view can intermittently come back empty when upstream sources throttle.
    # In that case, fall back to the regular themes snapshot so UI cards stay populated.
    def _scanner_fallback_payload(base_payload: dict) -> dict:
        if key != "scanner":
            return base_payload
        themes_rows = base_payload.get("themes") or []
        if themes_rows:
            return base_payload
        themes_cached = _CACHE.get("themes")
        if themes_cached is None:
            return base_payload
        merged = dict(themes_cached[1])
        merged["snapshot_fallback"] = "themes"
        return merged

    # Serve last-known-good snapshot immediately (stale-while-revalidate).
    if cached is not None:
        age = now - cached[0]
        active, poll, retry = _backoff_note(now)
        out = _scanner_fallback_payload(dict(cached[1]))
        out["polling"] = {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry}
        out["snapshot"] = {
            "key": key,
            "served_from_cache": True,
            "ageSeconds": int(age),
            "last_ok_utc": _THEMES_META.get(key, {}).get("last_ok_utc"),
            "refreshing": bool(_THEMES_META.get(key, {}).get("refreshing")),
            "last_error": _THEMES_META.get(key, {}).get("last_err"),
        }
        # Kick a background refresh if stale and not already refreshing.
        if age > _THEMES_REFRESH_SEC and not active:
            asyncio.create_task(_refresh_themes_snapshot(key))
        return out

    # No snapshot yet: do a one-time blocking refresh (first load).
    await _refresh_themes_snapshot(key)
    cached2 = _CACHE.get(key)
    if cached2 is None:
        if key == "scanner":
            themes_cached = _CACHE.get("themes")
            if themes_cached is not None:
                out_fallback = dict(themes_cached[1])
                active, poll, retry = _backoff_note(monotonic())
                out_fallback["snapshot_fallback"] = "themes"
                out_fallback["polling"] = {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry}
                out_fallback["snapshot"] = {
                    "key": key,
                    "served_from_cache": True,
                    "ageSeconds": int(monotonic() - themes_cached[0]),
                    "last_ok_utc": _THEMES_META.get("themes", {}).get("last_ok_utc"),
                    "refreshing": bool(_THEMES_META.get(key, {}).get("refreshing")),
                    "last_error": _THEMES_META.get(key, {}).get("last_err"),
                }
                # Keep trying to refresh scanner view in background.
                asyncio.create_task(_refresh_themes_snapshot(key))
                return out_fallback
        # Cold-start (or upstream down): serve a safe placeholder so the UI never blanks.
        # A background refresh loop will keep trying.
        active, poll, retry = _backoff_note(monotonic())
        asyncio.create_task(_refresh_themes_snapshot(key))
        return {
            "vix": {"symbol": "^VIX", "close": 0.0, "change_pct": 0.0},
            "themes": [],
            "tape": [],
            "market_momentum_score": {
                "score": 0,
                "state": "neutral",
                "message": "Data warming up. Showing placeholder snapshot.",
                "aPlusCount": 0,
                "aCount": 0,
            },
            "polling": {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry},
            "snapshot": {
                "key": key,
                "served_from_cache": False,
                "ageSeconds": None,
                "last_ok_utc": _THEMES_META.get(key, {}).get("last_ok_utc"),
                "refreshing": True,
                "last_error": _THEMES_META.get(key, {}).get("last_err"),
            },
        }
    out2 = dict(cached2[1])
    out2 = _scanner_fallback_payload(out2)
    active, poll, retry = _backoff_note(monotonic())
    out2["polling"] = {"pollSeconds": poll, "backoffActive": active, "retryAfterSeconds": retry}
    out2["snapshot"] = {
        "key": key,
        "served_from_cache": True,
        "ageSeconds": int(monotonic() - cached2[0]),
        "last_ok_utc": _THEMES_META.get(key, {}).get("last_ok_utc"),
        "refreshing": bool(_THEMES_META.get(key, {}).get("refreshing")),
        "last_error": _THEMES_META.get(key, {}).get("last_err"),
    }
    return out2


@app.on_event("startup")
async def _startup() -> None:
    global _UNIVERSE_TASK, _PRE_NEWS_TASK, _POST_NEWS_TASK, _THEMES_TASK
    await _THEME_UNIVERSE.load()
    # Refresh movers every 30 minutes (prices change; tickers-only automation).
    _UNIVERSE_TASK = asyncio.create_task(scheduled_refresh_loop(_THEME_UNIVERSE, every_sec=30 * 60))
    _PRE_NEWS_TASK = asyncio.create_task(scheduled_premarket_loop(_PRE_NEWS_STORE))
    _POST_NEWS_TASK = asyncio.create_task(scheduled_postmarket_loop(_POST_NEWS_STORE))
    _THEMES_TASK = asyncio.create_task(_themes_refresh_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _UNIVERSE_TASK, _PRE_NEWS_TASK, _POST_NEWS_TASK, _THEMES_TASK
    if _UNIVERSE_TASK is not None:
        _UNIVERSE_TASK.cancel()
        _UNIVERSE_TASK = None
    if _PRE_NEWS_TASK is not None:
        _PRE_NEWS_TASK.cancel()
        _PRE_NEWS_TASK = None
    if _POST_NEWS_TASK is not None:
        _POST_NEWS_TASK.cancel()
        _POST_NEWS_TASK = None
    if _THEMES_TASK is not None:
        _THEMES_TASK.cancel()
        _THEMES_TASK = None


@app.get("/api/news/premarket")
async def get_premarket_brief() -> dict:
    cached = await _PRE_NEWS_STORE.load()
    return cached or {"generated_at_utc": None, "scheduled_for_et": None, "sections": [], "headlines": []}


@app.post("/api/news/premarket/refresh")
async def refresh_premarket_brief() -> dict:
    if not _is_weekday_et():
        raise HTTPException(
            status_code=400,
            detail="Pre-market briefs are generated on NYSE trading days only.",
        )
    global _PRE_NEWS_REFRESH_TASK

    if _PRE_NEWS_REFRESH_TASK is None or _PRE_NEWS_REFRESH_TASK.done():
        async def _run() -> None:
            payload = await generate_premarket_brief()
            await _PRE_NEWS_STORE.save(payload)

        _PRE_NEWS_REFRESH_TASK = asyncio.create_task(_run())

    cached = await _PRE_NEWS_STORE.load()
    return {
        "status": "refreshing",
        "generated_at_utc": (cached or {}).get("generated_at_utc"),
        "scheduled_for_et": (cached or {}).get("scheduled_for_et"),
    }


@app.get("/api/news/postmarket")
async def get_postmarket_brief() -> dict:
    cached = await _POST_NEWS_STORE.load()
    return cached or {"generated_at_utc": None, "scheduled_for_et": None, "sections": [], "headlines": []}


@app.post("/api/news/postmarket/refresh")
async def refresh_postmarket_brief() -> dict:
    if not _is_weekday_et():
        raise HTTPException(status_code=400, detail="Post-market briefs are generated on NYSE trading days only.")
    global _POST_NEWS_REFRESH_TASK

    if _POST_NEWS_REFRESH_TASK is None or _POST_NEWS_REFRESH_TASK.done():
        async def _run() -> None:
            payload = await generate_postmarket_brief()
            await _POST_NEWS_STORE.save(payload)

        _POST_NEWS_REFRESH_TASK = asyncio.create_task(_run())

    cached = await _POST_NEWS_STORE.load()
    return {
        "status": "refreshing",
        "generated_at_utc": (cached or {}).get("generated_at_utc"),
        "scheduled_for_et": (cached or {}).get("scheduled_for_et"),
    }


@app.get("/api/market/status")
async def get_market_status() -> dict:
    """Lightweight market calendar status (ET-based)."""
    return market_status_dict()


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
    try:
        if uniq_syms:
            short_by_sym = await fetch_yfinance_short_percent_float_pct_batch(uniq_syms)
            short_source = "yfinance_short_percent_of_float"
            # Fallback: if Yahoo throttles / returns nothing, Finviz is a reliable secondary.
            if not any(v is not None for v in short_by_sym.values()):
                finviz = await fetch_finviz_short_float_pct_batch(uniq_syms)
                if any(v is not None for v in finviz.values()):
                    short_by_sym = finviz
                    short_source = "finviz_short_float"
    except Exception:
        # Yahoo 401 / Finviz 429 etc. must not blank the whole gappers response.
        short_by_sym = {}
        short_source = None
    for r, sym in zip(rows, symbols_in_order):
        if sym and short_by_sym.get(sym) is not None:
            r["short_interest_pct"] = short_by_sym[sym]

    payload = {
        **data,
        "rows": rows,
        "row_count": len(rows),
        "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "short_interest_source": short_source,
    }
    _PREMARKET_GAP_CACHE[cache_key] = (now, payload)
    return payload


@app.get("/api/market-ocean")
async def get_market_ocean() -> dict:
    """
    Market Ocean regime endpoint.
    Returns:
      s5fi              - % of S&P 500 proxy stocks above their 50 SMA (0-100)
      speedboat_count   - count of elite universe stocks meeting ALL:
                          price >$12, 30d avg daily $vol >$100M, change >+4%,
                          20d ADR% >4%, market cap >$2B
      is_blast_off      - true when speedboat_count >= 125 (institutional thrust signal)
      blast_off_threshold - the threshold used (125), so the UI never hard-codes it
      s5fi_history / speedboat_history - 10-day trend lists [{date, value}]
    Cached 20 min; thread-safe via _OCEAN_COMPUTE_LOCK inside compute_market_ocean_sync.
    """
    # TEMP: hardcoded preview — Deep Ocean (s5fi=65) + Elite Blast-off (count=145 >= 125).
    # Remove this block once the blast-off glow and rocket directive are confirmed in the UI.
    _today = datetime.now(timezone.utc).date().isoformat()
    _s5fi_vals  = [50, 52, 55, 58, 60, 62, 65]
    _boat_vals  = [40, 50, 80, 110, 120, 130, 145]
    return {
        "s5fi": 65.0,
        "speedboat_count": 145,
        "is_blast_off": True,
        "blast_off_threshold": 125,
        "s5fi_history":        [{"date": _today, "value": v} for v in _s5fi_vals],
        "speedboat_history":   [{"date": _today, "value": v} for v in _boat_vals],
        "universe_size": 312,
        "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "note": "TEMP ui-preview — remove after blast-off visual is confirmed",
    }
    # END TEMP

    global _OCEAN_CACHE, _OCEAN_CACHE_TS
    now = monotonic()
    if _OCEAN_CACHE is not None and (now - _OCEAN_CACHE_TS) < _OCEAN_CACHE_TTL_SEC:
        return _OCEAN_CACHE
    try:
        snapshot: OceanSnapshot = await asyncio.to_thread(compute_market_ocean_sync, history_days=10)
        result = {
            **snapshot.to_dict(),
            "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
    except Exception as exc:
        # Never 500 — return a graceful degraded payload that still carries the flag.
        result = {
            "s5fi": None,
            "speedboat_count": None,
            "is_blast_off": False,
            "blast_off_threshold": 125,
            "s5fi_history": [],
            "speedboat_history": [],
            "universe_size": 0,
            "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "error": str(exc),
        }
    _OCEAN_CACHE = result
    _OCEAN_CACHE_TS = now
    return result


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
        # If we already have a stale cache, serve it; otherwise serve a safe fallback
        # (avoid hard-failing UI drawers during upstream throttling).
        cached2 = _TICKER_INTEL_CACHE.get(t)
        if cached2 is not None:
            return cached2[1]
        snap = {
            "ticker": t,
            "name": t,
            "close": None,
            "today_return_pct": 0.0,
            "sector": "Unknown",
            "industry": "Unknown",
        }

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


@app.get("/api/earnings/next")
async def get_next_earnings(
    tickers: list[str] = FQuery(default=[]),
) -> dict:
    """
    Best-effort next earnings timestamp (ET) for a list of tickers.
    """
    if not tickers:
        return {"results": []}
    results = await next_earnings_for_tickers(tickers, cache=_EARNINGS_CACHE)
    return {"results": results}


@app.get("/api/ticker/news")
async def get_ticker_news(
    ticker: str,
    days: int = 90,
) -> dict:
    """
    Best-effort ticker news/events from yfinance.
    Returns a normalized list for the UI (last ~90 days by default).
    """
    raw = (ticker or "").strip().upper()
    if not raw:
        raise HTTPException(status_code=400, detail="ticker is required")
    days = int(days or 90)
    days = max(7, min(days, 365))
    since = datetime.now(timezone.utc).timestamp() - (days * 24 * 3600)

    def _fetch() -> list[dict]:
        tk = yf.Ticker(raw)
        items = getattr(tk, "news", None) or []
        out: list[dict] = []
        if isinstance(items, list):
            for it in items[:80]:
                if not isinstance(it, dict):
                    continue
                ts = it.get("providerPublishTime") or it.get("pubDate") or it.get("time")
                try:
                    ts_f = float(ts) if ts is not None else None
                except Exception:
                    ts_f = None
                if ts_f is not None and ts_f < since:
                    continue
                dt = datetime.fromtimestamp(ts_f, tz=timezone.utc) if ts_f is not None else None
                link = str(it.get("link") or it.get("url") or "").strip()
                title = str(it.get("title") or "").strip()
                if not title:
                    continue
                out.append(
                    {
                        "date_utc": dt.date().isoformat() if dt else None,
                        "published_at_utc": dt.replace(microsecond=0).isoformat() if dt else None,
                        "event_type": str(it.get("type") or it.get("publisher") or "News"),
                        "title": title,
                        "link": link or None,
                        "source": str(it.get("publisher") or it.get("provider") or "yfinance"),
                    }
                )
        return out

    try:
        rows = await asyncio.to_thread(_fetch)
    except YFRateLimitError:
        rows = []
    return {"ticker": raw, "days": days, "results": rows}
