"""Fetch and parse skyte/rs-log CSVs (GitHub / jsDelivr) with in-process TTL cache."""

from __future__ import annotations

import asyncio
import csv
import io
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

TTL_SEC = max(60, int(os.environ.get("RS_SKYTE_CACHE_TTL_SECONDS", "900")))

URLS_INDUSTRIES = [
    "https://cdn.jsdelivr.net/gh/skyte/rs-log@main/output/rs_industries.csv",
    "https://raw.githubusercontent.com/skyte/rs-log/main/output/rs_industries.csv",
]
URLS_STOCKS = [
    "https://cdn.jsdelivr.net/gh/skyte/rs-log@main/output/rs_stocks.csv",
    "https://raw.githubusercontent.com/skyte/rs-log/main/output/rs_stocks.csv",
]

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


async def _fetch_text(urls: list[str], timeout: float = 30.0) -> str:
    last: str | None = None
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for url in urls:
            try:
                r = await client.get(url)
                if r.status_code == 200 and (r.text or "").strip():
                    return r.text
                last = f"HTTP {r.status_code}"
            except Exception as e:
                last = str(e)
    raise RuntimeError(f"skyte CSV fetch failed ({last})")


def _strip_bom(text: str) -> str:
    if text.startswith("\ufeff"):
        return text[1:]
    return text


def _parse_industries_csv(text: str) -> list[dict[str, Any]]:
    text = _strip_bom(text)
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    out: list[dict[str, Any]] = []
    for raw in reader:
        if not raw:
            continue
        try:
            ind = (raw.get("Industry") or "").strip()
            if not ind:
                continue
            out.append(
                {
                    "rank": int(str(raw.get("Rank", "")).strip() or 0),
                    "industry": ind,
                    "sector": (raw.get("Sector") or "").strip(),
                    "relative_strength": float(str(raw.get("Relative Strength", "")).strip()),
                    "percentile": int(float(str(raw.get("Percentile", "")).strip())),
                    "month_1_ago": _opt_int(raw.get("1 Month Ago")),
                    "month_3_ago": _opt_int(raw.get("3 Months Ago")),
                    "month_6_ago": _opt_int(raw.get("6 Months Ago")),
                    "tickers": (raw.get("Tickers") or "").strip(),
                }
            )
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _opt_int(v: Any) -> int | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(float(str(v).strip()))
    except ValueError:
        return None


def _parse_stocks_csv(text: str) -> list[dict[str, Any]]:
    text = _strip_bom(text)
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    out: list[dict[str, Any]] = []
    for raw in reader:
        if not raw:
            continue
        try:
            t = (raw.get("Ticker") or "").strip().upper()
            if not t:
                continue
            out.append(
                {
                    "rank": int(str(raw.get("Rank", "")).strip() or 0),
                    "ticker": t,
                    "sector": (raw.get("Sector") or "").strip(),
                    "industry": (raw.get("Industry") or "").strip(),
                    "exchange": (raw.get("Exchange") or "").strip(),
                    "relative_strength": float(str(raw.get("Relative Strength", "")).strip()),
                    "percentile": int(float(str(raw.get("Percentile", "")).strip())),
                    "month_1_ago": _opt_int(raw.get("1 Month Ago")),
                    "month_3_ago": _opt_int(raw.get("3 Months Ago")),
                    "month_6_ago": _opt_int(raw.get("6 Months Ago")),
                }
            )
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _payload(
    *,
    ok: bool,
    kind: str,
    rows: list[dict[str, Any]],
    detail: str | None = None,
    cache_hit: bool = False,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "ok": ok,
        "source": "skyte/rs-log",
        "kind": kind,
        "count": len(rows),
        "rows": rows,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": TTL_SEC,
        "cache_hit": cache_hit,
    }
    if detail:
        base["detail"] = detail
    return base


async def get_industries() -> dict[str, Any]:
    key = "industries"
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now < hit[0]:
        body = dict(hit[1])
        body["cache_hit"] = True
        return body
    try:
        text = await _fetch_text(URLS_INDUSTRIES)
        rows = await asyncio.to_thread(_parse_industries_csv, text)
        body = _payload(ok=True, kind="industries", rows=rows, cache_hit=False)
        _cache[key] = (time.monotonic() + TTL_SEC, dict(body))
    except Exception as e:
        body = _payload(ok=False, kind="industries", rows=[], detail=str(e), cache_hit=False)
    return body


async def get_stocks() -> dict[str, Any]:
    key = "stocks"
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now < hit[0]:
        body = dict(hit[1])
        body["cache_hit"] = True
        return body
    try:
        text = await _fetch_text(URLS_STOCKS)
        rows = await asyncio.to_thread(_parse_stocks_csv, text)
        body = _payload(ok=True, kind="stocks", rows=rows, cache_hit=False)
        _cache[key] = (time.monotonic() + TTL_SEC, dict(body))
    except Exception as e:
        body = _payload(ok=False, kind="stocks", rows=[], detail=str(e), cache_hit=False)
    return body
