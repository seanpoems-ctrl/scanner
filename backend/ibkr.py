"""
ibkr.py — Interactive Brokers Client Portal API wrapper.

Connects to the IBKR Client Portal Gateway (a local JAR that runs on
localhost:5000 by default). All data calls go through this gateway after
the user has authenticated via their browser.

Session lifecycle:
  - Gateway must be running (java -jar clientportal.gw.jar)
  - User logs in at https://localhost:5000 once per ~24 hours
  - We tickle the session every 55s via /v1/api/tickle to keep it alive
  - On 401/503 → session expired → fall back to delayed sources in callers

Environment variables:
  IBKR_GATEWAY_URL   — default: https://localhost:5000
  IBKR_VERIFY_SSL    — "false" disables SSL verification (needed because the
                        gateway uses a self-signed cert); default: "false"

Public API:
  get_status()          → IbkrStatus (live, session info, error)
  get_quotes(conids)    → list of quote dicts
  search_conid(symbol)  → int | None  (contract ID for a symbol)
  get_news(limit)       → list of news headline dicts
  get_econ_calendar()   → list of economic event dicts
  tickle()              → keep session alive (called by background loop)
"""

from __future__ import annotations

import asyncio
import logging
import os
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────

GATEWAY_URL: str = os.environ.get("IBKR_GATEWAY_URL", "https://localhost:5000").rstrip("/")
_VERIFY_SSL: bool = os.environ.get("IBKR_VERIFY_SSL", "false").lower() not in ("false", "0", "no")

# httpx client kwargs — disables SSL verify for self-signed gateway cert
_CLIENT_KWARGS: dict[str, Any] = {
    "base_url": GATEWAY_URL,
    "timeout": 8.0,
    "verify": _VERIFY_SSL,
}

# ── Status dataclass ────────────────────────────────────────────────────────

@dataclass
class IbkrStatus:
    live: bool                        # True = gateway reachable + session valid
    authenticated: bool = False       # session authenticated flag from gateway
    competing: bool = False           # another session is competing
    connected: bool = False           # IB server connection from gateway
    server_info: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    checked_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )

    def as_dict(self) -> dict[str, Any]:
        return {
            "live": self.live,
            "authenticated": self.authenticated,
            "competing": self.competing,
            "connected": self.connected,
            "server_info": self.server_info,
            "error": self.error,
            "checked_at_utc": self.checked_at_utc,
            "gateway_url": GATEWAY_URL,
        }


# ── Internal helpers ────────────────────────────────────────────────────────

def _make_client() -> httpx.AsyncClient:
    """Return a configured async client for the IBKR gateway."""
    return httpx.AsyncClient(**_CLIENT_KWARGS)


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
    """
    GET from the gateway. Returns parsed JSON or raises on HTTP error / connectivity.
    Raises httpx.ConnectError if gateway is not running.
    """
    async with _make_client() as client:
        r = await client.get(path, params=params)
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


async def _post(path: str, json: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
    """POST to the gateway. Returns parsed JSON."""
    async with _make_client() as client:
        r = await client.post(path, json=json or {})
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


# ── Public API ──────────────────────────────────────────────────────────────

async def get_status() -> IbkrStatus:
    """
    Check whether the IBKR gateway is reachable and the session is valid.
    Calls GET /v1/api/iserver/auth/status.
    """
    try:
        data = await _get("/v1/api/iserver/auth/status")
        if not isinstance(data, dict):
            return IbkrStatus(live=False, error="Unexpected response shape from gateway")

        authenticated: bool = bool(data.get("authenticated", False))
        competing: bool = bool(data.get("competing", False))
        connected: bool = bool(data.get("connected", False))
        server_info: dict[str, Any] = data.get("serverInfo", {})

        return IbkrStatus(
            live=authenticated and connected and not competing,
            authenticated=authenticated,
            competing=competing,
            connected=connected,
            server_info=server_info,
        )
    except httpx.ConnectError:
        return IbkrStatus(live=False, error="IBKR gateway not reachable (is it running?)")
    except httpx.HTTPStatusError as e:
        return IbkrStatus(live=False, error=f"Gateway HTTP {e.response.status_code}")
    except Exception as e:
        return IbkrStatus(live=False, error=str(e))


async def tickle() -> bool:
    """
    Keep the IBKR session alive. Call every ~55 seconds.
    POST /v1/api/tickle — returns True on success.
    """
    try:
        await _post("/v1/api/tickle")
        return True
    except Exception as exc:
        logger.warning("ibkr tickle failed: %s", exc)
        return False


async def search_conid(symbol: str) -> int | None:
    """
    Resolve a ticker symbol to an IBKR contract ID (conid).
    Uses POST /v1/api/iserver/secdef/search.
    Returns the first US STK conid found, or None.
    """
    try:
        data = await _post(
            "/v1/api/iserver/secdef/search",
            json={"symbol": symbol.upper(), "name": False, "secType": "STK"},
        )
        if not isinstance(data, list) or not data:
            return None
        for item in data:
            if not isinstance(item, dict):
                continue
            # Prefer US primary listing
            sections = item.get("sections", [])
            for sec in sections:
                if isinstance(sec, dict) and sec.get("exchange") in ("NASDAQ", "NYSE", "BATS", "ARCA"):
                    conid_str = str(sec.get("conid", ""))
                    if conid_str.isdigit():
                        return int(conid_str)
            # Fallback: use top-level conid
            conid_raw = item.get("conid")
            if conid_raw and str(conid_raw).isdigit():
                return int(str(conid_raw))
        return None
    except Exception as exc:
        logger.warning("ibkr search_conid(%s) failed: %s", symbol, exc)
        return None


async def get_quotes(conids: list[int]) -> list[dict[str, Any]]:
    """
    Fetch real-time market data snapshots for a list of contract IDs.
    Uses GET /v1/api/iserver/marketdata/snapshot.

    Fields requested:
      31  = Last price
      84  = Bid
      86  = Ask
      85  = Bid size
      88  = Ask size
      7295 = Open
      7741 = Prior close
      7762 = Today's change %
      7282 = Market cap
      6119 = Exchange

    Returns a list of quote dicts, one per conid with parsed numeric values.
    """
    if not conids:
        return []

    # Gateway requires a warmup call before data arrives — first call often empty
    fields = "31,84,86,85,88,7295,7741,7762,7282,6119"
    conid_str = ",".join(str(c) for c in conids)

    try:
        # First call (warms up subscription)
        await _get("/v1/api/iserver/marketdata/snapshot", params={"conids": conid_str, "fields": fields})
        await asyncio.sleep(0.5)
        # Second call — data should be populated
        data = await _get("/v1/api/iserver/marketdata/snapshot", params={"conids": conid_str, "fields": fields})

        if not isinstance(data, list):
            return []

        quotes: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            conid = item.get("conid")

            def _num(key: str) -> float | None:
                v = item.get(key)
                if v is None:
                    return None
                try:
                    return float(str(v).replace(",", "").replace("C", "").replace("H", "").replace("O", ""))
                except ValueError:
                    return None

            quotes.append({
                "conid": conid,
                "last": _num("31"),
                "bid": _num("84"),
                "ask": _num("86"),
                "bid_size": _num("85"),
                "ask_size": _num("88"),
                "open": _num("7295"),
                "prior_close": _num("7741"),
                "change_pct": _num("7762"),
                "market_cap": _num("7282"),
                "exchange": item.get("6119"),
            })
        return quotes

    except Exception as exc:
        logger.warning("ibkr get_quotes failed: %s", exc)
        return []


async def get_news(limit: int = 20) -> list[dict[str, Any]]:
    """
    Fetch recent news headlines from IBKR.
    Uses GET /v1/api/iserver/news/latest.
    Returns a list of news dicts with keys: headline, id, provider, date_utc.
    """
    try:
        data = await _get("/v1/api/iserver/news/latest", params={"limit": limit})
        if not isinstance(data, list):
            return []

        results: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            ts = item.get("date")
            date_utc = None
            if ts:
                try:
                    date_utc = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
                except Exception:
                    pass
            results.append({
                "id": item.get("articleId") or item.get("id"),
                "headline": item.get("headline") or item.get("summary", ""),
                "provider": item.get("provider", ""),
                "date_utc": date_utc,
                "tickers": item.get("symbols", []),
            })
        return results

    except Exception as exc:
        logger.warning("ibkr get_news failed: %s", exc)
        return []


async def get_econ_calendar() -> list[dict[str, Any]]:
    """
    Fetch upcoming economic calendar events from IBKR.
    Uses GET /v1/api/calendar/events.
    Returns events sorted by scheduled time ascending.
    """
    try:
        data = await _get("/v1/api/calendar/events")
        if not isinstance(data, list):
            # Some gateway versions nest under a key
            if isinstance(data, dict):
                data = data.get("events", [])
            else:
                return []

        results: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            results.append({
                "event": item.get("eventName") or item.get("name", ""),
                "country": item.get("country", ""),
                "currency": item.get("currency", ""),
                "scheduled_utc": item.get("scheduledDate") or item.get("date"),
                "actual": item.get("actual"),
                "forecast": item.get("forecast"),
                "previous": item.get("previous"),
                "impact": item.get("importance") or item.get("impact"),
            })

        results.sort(key=lambda x: x.get("scheduled_utc") or "")
        return results

    except Exception as exc:
        logger.warning("ibkr get_econ_calendar failed: %s", exc)
        return []


# ── Background keep-alive loop ──────────────────────────────────────────────

_TICKLE_INTERVAL_S = 55  # seconds — IBKR sessions expire after ~10 min of inactivity

async def run_keepalive_loop() -> None:
    """
    Async task: tickle the IBKR session every 55 seconds.
    Start this in FastAPI startup only when IBKR integration is enabled.
    Runs indefinitely; cancel via task.cancel().
    """
    logger.info("ibkr: keep-alive loop started (interval=%ds)", _TICKLE_INTERVAL_S)
    while True:
        try:
            await asyncio.sleep(_TICKLE_INTERVAL_S)
            ok = await tickle()
            if not ok:
                logger.debug("ibkr: tickle returned falsy — session may be expired")
        except asyncio.CancelledError:
            logger.info("ibkr: keep-alive loop cancelled")
            return
        except Exception as exc:
            logger.warning("ibkr: keep-alive loop error: %s", exc)
