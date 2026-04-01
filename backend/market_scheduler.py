"""
market_scheduler.py
===================
APScheduler-based cron that fires Market Intelligence briefs at precise ET times.

Schedule (Mon–Fri, NYSE trading days only):
  08:03 AM ET  →  Pre-Market brief
  04:55 PM ET  →  Post-Market brief

Holiday skipping uses pandas_market_calendars (exchange-calendars package is already
in requirements.txt).  Falls back to a simple weekday check if that import fails.

This module exposes:
  start_scheduler()  – create + start an AsyncIOScheduler; call once on FastAPI startup
  stop_scheduler()   – graceful shutdown; call on FastAPI shutdown

The scheduler runs inside the existing uvicorn event loop — no separate process needed.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, date

from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
NY_TZ = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Holiday / trading-day guard
# ---------------------------------------------------------------------------

def _is_trading_day(dt: date) -> bool:
    """Return True if `dt` is an NYSE trading day (handles market holidays)."""
    try:
        import pandas_market_calendars as mcal  # type: ignore[import]
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=dt.isoformat(),
            end_date=dt.isoformat(),
        )
        return not schedule.empty
    except Exception:
        # Fallback: accept any Mon–Fri
        return dt.weekday() < 5


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

async def _run_brief(brief_type: str) -> None:
    """Inner async coroutine that generates and saves a brief."""
    try:
        from market_intelligence import generate_and_save  # type: ignore[import]
    except ModuleNotFoundError:
        from backend.market_intelligence import generate_and_save  # type: ignore[import]

    today = datetime.now(NY_TZ).date()
    if not _is_trading_day(today):
        log.info("Scheduler: skipping %s brief — not a trading day (%s)", brief_type, today)
        return

    log.info("Scheduler: generating %s brief…", brief_type)
    for attempt in range(1, 4):
        try:
            data = await generate_and_save(brief_type)
            log.info(
                "Scheduler: %s brief done (Gen %s, attempt %d)",
                brief_type,
                data.get("gen_time_et"),
                attempt,
            )
            return
        except Exception as exc:
            log.warning("Scheduler: %s brief attempt %d failed: %s", brief_type, attempt, exc)
            if attempt < 3:
                await asyncio.sleep(30 * attempt)
    log.error("Scheduler: %s brief failed after 3 attempts — will retry at next scheduled run.", brief_type)


def _sync_run_brief(brief_type: str) -> None:
    """APScheduler calls sync functions; bridge to the async job."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Inside uvicorn's event loop — schedule as a task
            asyncio.ensure_future(_run_brief(brief_type))
        else:
            loop.run_until_complete(_run_brief(brief_type))
    except Exception as exc:
        log.error("Scheduler bridge error (%s): %s", brief_type, exc)


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

_scheduler = None


def start_scheduler() -> None:
    """Create and start the APScheduler.  Idempotent."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return

    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        log.warning(
            "apscheduler not installed — scheduled briefs disabled. "
            "Add 'apscheduler' to requirements.txt and redeploy."
        )
        return

    import pytz
    eastern = pytz.timezone("America/New_York")

    _scheduler = AsyncIOScheduler(timezone=eastern)

    # Pre-market: 08:03 AM ET, Mon–Fri
    _scheduler.add_job(
        _sync_run_brief,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=8,
            minute=3,
            second=0,
            timezone=eastern,
        ),
        args=["pre"],
        id="pre_market_brief",
        name="Pre-Market Intelligence Brief",
        replace_existing=True,
        misfire_grace_time=300,   # 5-min window in case server was briefly down
    )

    # Post-market: 04:55 PM ET, Mon–Fri
    _scheduler.add_job(
        _sync_run_brief,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=16,
            minute=55,
            second=0,
            timezone=eastern,
        ),
        args=["post"],
        id="post_market_brief",
        name="Post-Market Intelligence Brief",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.start()
    log.info(
        "Market scheduler started — pre @ 08:03 ET, post @ 16:55 ET (Mon-Fri, trading days only)"
    )


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("Market scheduler stopped.")
    _scheduler = None
