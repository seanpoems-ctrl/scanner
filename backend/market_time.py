from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

try:
    import exchange_calendars as xcals
    XNYS = xcals.get_calendar("XNYS")
    _XCALS_OK = True
except Exception:
    xcals = None  # type: ignore[assignment]
    XNYS = None   # type: ignore[assignment]
    _XCALS_OK = False

NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class MarketStatus:
    now_et_iso: str
    is_trading_day: bool
    premarket_scan_active: bool
    session: str  # "closed" | "premarket" | "open" | "post"
    next_open_et_iso: str | None
    next_close_et_iso: str | None


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(timezone.utc)


def is_nyse_trading_day_et(dt: datetime) -> bool:
    """True if XNYS has a session for the ET date."""
    dt_et = dt.astimezone(NY_TZ)
    d = dt_et.date()
    if not _XCALS_OK or XNYS is None:
        return d.weekday() < 5  # Mon–Fri fallback
    # exchange_calendars expects session label as string YYYY-MM-DD
    return XNYS.is_session(str(d))


def market_status(now_utc: datetime | None = None) -> MarketStatus:
    now = now_utc or datetime.now(timezone.utc).replace(microsecond=0)
    now_et = now.astimezone(NY_TZ)
    d = now_et.date()
    if _XCALS_OK and XNYS is not None:
        is_td = XNYS.is_session(str(d))
    else:
        is_td = d.weekday() < 5

    # Pre-market scanner window start (ET). ZoneInfo("America/New_York") automatically
    # shifts between EST/EDT with DST.
    pre_start = datetime.combine(d, time(8, 13), tzinfo=NY_TZ)
    open_time = datetime.combine(d, time(9, 30), tzinfo=NY_TZ)
    close_time = datetime.combine(d, time(16, 0), tzinfo=NY_TZ)

    if not is_td:
        session = "closed"
        pre_active = False
    else:
        if now_et < pre_start:
            session = "closed"
            pre_active = False
        elif now_et < open_time:
            session = "premarket"
            pre_active = True
        elif now_et < close_time:
            session = "open"
            pre_active = False
        else:
            session = "post"
            pre_active = False

    next_open = None
    next_close = None
    try:
        if not _XCALS_OK or XNYS is None:
            raise RuntimeError("exchange_calendars unavailable")
        # Use exchange calendar for upcoming open/close timestamps.
        # Convert now to UTC for schedule lookup.
        now_u = _to_utc(now)
        # Look ahead a bit to find the next session.
        sched = XNYS.schedule(start=now_u.date(), end=(now_u + timedelta(days=10)).date())
        if not sched.empty:
            # Find current or next session row by index.
            # index is session date (UTC midnight); open/close are UTC timestamps.
            opens = sched["open"]
            closes = sched["close"]
            # Next open is the first open >= now
            nxt_open = opens[opens >= now_u].iloc[0] if (opens >= now_u).any() else None
            if nxt_open is not None:
                next_open = _iso(nxt_open.tz_convert(NY_TZ))
            # Next close is the first close >= now
            nxt_close = closes[closes >= now_u].iloc[0] if (closes >= now_u).any() else None
            if nxt_close is not None:
                next_close = _iso(nxt_close.tz_convert(NY_TZ))
    except Exception:
        next_open = None
        next_close = None

    return MarketStatus(
        now_et_iso=_iso(now_et),
        is_trading_day=bool(is_td),
        premarket_scan_active=bool(pre_active),
        session=session,
        next_open_et_iso=next_open,
        next_close_et_iso=next_close,
    )


def market_status_dict(now_utc: datetime | None = None) -> dict:
    return asdict(market_status(now_utc))

