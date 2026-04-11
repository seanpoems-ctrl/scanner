"""
routers/ibkr.py — FastAPI router for IBKR Client Portal API endpoints.

Endpoints:
  GET  /api/ibkr/status          — gateway reachable + session valid?
  POST /api/ibkr/tickle          — manually extend session (also runs automatically)
  GET  /api/ibkr/quotes          — real-time quotes by symbol (resolves conids)
  GET  /api/ibkr/news            — latest IBKR news headlines
  GET  /api/ibkr/calendar        — upcoming economic events
  POST /api/ibkr/search          — resolve symbol → conid

All endpoints return { ok: bool, live: bool, ... } so the frontend can
gracefully degrade to delayed sources when live=false.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ibkr", tags=["ibkr"])


def _ibkr():
    """Lazy import so the router can be registered even if ibkr.py has an import error."""
    try:
        import backend.ibkr as _m
    except ImportError:
        import ibkr as _m  # type: ignore[no-redef]
    return _m


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ── Status ──────────────────────────────────────────────────────────────────

@router.get("/status")
async def ibkr_status() -> dict[str, Any]:
    """
    Check IBKR gateway connectivity and session state.
    Returns { ok, live, authenticated, connected, competing, error, checked_at_utc }.
    Frontend polls this every 60 s to show the LIVE / DELAYED badge.
    """
    m = _ibkr()
    status = await m.get_status()
    return {"ok": True, **status.as_dict()}


# ── Tickle ──────────────────────────────────────────────────────────────────

@router.post("/tickle")
async def ibkr_tickle() -> dict[str, Any]:
    """Manually extend the IBKR session. Also called automatically every 55 s."""
    m = _ibkr()
    ok = await m.tickle()
    return {"ok": ok, "tickled_at_utc": _now_utc()}


# ── Symbol → Conid search ────────────────────────────────────────────────────

@router.get("/search")
async def ibkr_search(symbol: str = Query(..., description="Ticker symbol, e.g. NVDA")) -> dict[str, Any]:
    """Resolve a ticker symbol to an IBKR contract ID (conid)."""
    m = _ibkr()
    status = await m.get_status()
    if not status.live:
        return {"ok": False, "live": False, "symbol": symbol, "conid": None, "error": status.error}

    conid = await m.search_conid(symbol)
    return {
        "ok": conid is not None,
        "live": True,
        "symbol": symbol.upper(),
        "conid": conid,
        "fetched_at_utc": _now_utc(),
    }


# ── Quotes ───────────────────────────────────────────────────────────────────

@router.get("/quotes")
async def ibkr_quotes(
    symbols: str = Query(..., description="Comma-separated tickers, e.g. SPY,QQQ,NVDA"),
) -> dict[str, Any]:
    """
    Fetch real-time quotes for up to 20 symbols.
    Resolves symbols → conids, then fetches market data snapshots.
    Falls back gracefully: returns live=false if session is not active.
    """
    m = _ibkr()
    status = await m.get_status()
    if not status.live:
        return {
            "ok": False,
            "live": False,
            "quotes": [],
            "error": status.error or "IBKR session not active",
            "fetched_at_utc": _now_utc(),
        }

    tickers = [s.strip().upper() for s in symbols.split(",") if s.strip()][:20]

    # Resolve all symbols to conids in parallel
    conid_tasks = [m.search_conid(t) for t in tickers]
    conid_results = await asyncio.gather(*conid_tasks)

    ticker_to_conid: dict[str, int] = {}
    for ticker, conid in zip(tickers, conid_results):
        if conid is not None:
            ticker_to_conid[ticker] = conid

    if not ticker_to_conid:
        return {
            "ok": False,
            "live": True,
            "quotes": [],
            "error": "No conids resolved for requested symbols",
            "fetched_at_utc": _now_utc(),
        }

    conid_to_ticker = {v: k for k, v in ticker_to_conid.items()}
    quotes_raw = await m.get_quotes(list(ticker_to_conid.values()))

    # Attach ticker symbol to each quote
    quotes: list[dict[str, Any]] = []
    for q in quotes_raw:
        conid = q.get("conid")
        ticker = conid_to_ticker.get(conid, str(conid))
        quotes.append({"ticker": ticker, **q})

    return {
        "ok": True,
        "live": True,
        "count": len(quotes),
        "quotes": quotes,
        "fetched_at_utc": _now_utc(),
    }


# ── News ─────────────────────────────────────────────────────────────────────

@router.get("/news")
async def ibkr_news(
    limit: int = Query(20, ge=1, le=50, description="Max headlines to return"),
) -> dict[str, Any]:
    """
    Fetch latest IBKR news headlines.
    Returns live=false and empty list if session is not active.
    """
    m = _ibkr()
    status = await m.get_status()
    if not status.live:
        return {
            "ok": False,
            "live": False,
            "headlines": [],
            "error": status.error or "IBKR session not active",
            "fetched_at_utc": _now_utc(),
        }

    headlines = await m.get_news(limit=limit)
    return {
        "ok": True,
        "live": True,
        "count": len(headlines),
        "headlines": headlines,
        "fetched_at_utc": _now_utc(),
    }


# ── Economic Calendar ────────────────────────────────────────────────────────

@router.get("/calendar")
async def ibkr_calendar() -> dict[str, Any]:
    """
    Fetch upcoming economic events from IBKR.
    Returns live=false and empty list if session is not active.
    """
    m = _ibkr()
    status = await m.get_status()
    if not status.live:
        return {
            "ok": False,
            "live": False,
            "events": [],
            "error": status.error or "IBKR session not active",
            "fetched_at_utc": _now_utc(),
        }

    events = await m.get_econ_calendar()
    return {
        "ok": True,
        "live": True,
        "count": len(events),
        "events": events,
        "fetched_at_utc": _now_utc(),
    }
