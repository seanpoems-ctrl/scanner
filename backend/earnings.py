from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from time import monotonic
from zoneinfo import ZoneInfo

import yfinance as yf
from yfinance.exceptions import YFRateLimitError

NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class NextEarnings:
    ticker: str
    earnings_et_iso: str | None


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _extract_next_earnings_et_iso(ticker: str) -> str | None:
    """
    Best-effort: yfinance exposes earnings dates inconsistently across tickers.
    We try a few access patterns and normalize to an ET ISO timestamp.
    """
    tk = yf.Ticker(ticker)

    # 1) Preferred when available: get_earnings_dates()
    get_dates = getattr(tk, "get_earnings_dates", None)
    if callable(get_dates):
        try:
            df = get_dates(limit=8)
            if df is not None and getattr(df, "empty", True) is False:
                idx0 = getattr(df, "index", None)
                if idx0 is not None and len(idx0) > 0:
                    ts = idx0[0]
                    if hasattr(ts, "to_pydatetime"):
                        dt = ts.to_pydatetime()
                    else:
                        dt = ts  # may already be datetime
                    if isinstance(dt, datetime):
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return _iso(dt.astimezone(NY_TZ))
        except Exception:
            pass

    # 2) Fallback: calendar DataFrame
    try:
        cal = getattr(tk, "calendar", None)
        if cal is not None and getattr(cal, "empty", True) is False:
            for key in ("Earnings Date", "Earnings Date Start", "Earnings Date End"):
                try:
                    if key in cal.index:
                        raw = cal.loc[key][0] if hasattr(cal.loc[key], "__len__") else cal.loc[key]
                    elif key in getattr(cal, "columns", []):
                        raw = cal[key].iloc[0]
                    else:
                        continue
                except Exception:
                    continue

                if hasattr(raw, "to_pydatetime"):
                    raw = raw.to_pydatetime()
                if isinstance(raw, datetime):
                    dt = raw
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return _iso(dt.astimezone(NY_TZ))
    except Exception:
        pass

    return None


class EarningsCache:
    def __init__(self, ttl_sec: float = 6 * 60.0) -> None:
        self._ttl = float(ttl_sec)
        self._cache: dict[str, tuple[float, NextEarnings]] = {}

    def get(self, ticker: str) -> NextEarnings | None:
        now = monotonic()
        hit = self._cache.get(ticker)
        if hit is None:
            return None
        ts, val = hit
        if (now - ts) > self._ttl:
            return None
        return val

    def set(self, ticker: str, val: NextEarnings) -> None:
        self._cache[ticker] = (monotonic(), val)


async def next_earnings_for_tickers(
    tickers: list[str],
    *,
    cache: EarningsCache,
) -> list[dict]:
    cleaned = []
    for t in tickers:
        s = (t or "").strip().upper()
        if not s:
            continue
        if s not in cleaned:
            cleaned.append(s)

    out: list[NextEarnings] = []

    async def _one(t: str) -> None:
        hit = cache.get(t)
        if hit is not None:
            out.append(hit)
            return
        try:
            iso = await asyncio.to_thread(_extract_next_earnings_et_iso, t)
            val = NextEarnings(ticker=t, earnings_et_iso=iso)
            cache.set(t, val)
            out.append(val)
        except YFRateLimitError:
            val = NextEarnings(ticker=t, earnings_et_iso=None)
            cache.set(t, val)
            out.append(val)
        except Exception:
            val = NextEarnings(ticker=t, earnings_et_iso=None)
            cache.set(t, val)
            out.append(val)

    await asyncio.gather(*[_one(t) for t in cleaned[:30]])
    return [asdict(x) for x in out]

