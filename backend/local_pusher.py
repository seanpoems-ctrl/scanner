"""
local_pusher.py — Runs on your Windows machine (residential IP).

Scrapes Finviz Themes + Industry leaderboard data and POSTs it to your
Render backend, bypassing Finviz's cloud-IP block completely.

Usage:
    python backend/local_pusher.py

Environment variables (set in .env or your shell):
    RENDER_URL   = https://<your-service>.onrender.com   (no trailing slash)
    PUSH_SECRET  = <same value you set in Render env vars>

Schedule via Windows Task Scheduler to run every 15 minutes on weekdays
between 07:30 and 18:00 ET.  A helper .bat file is provided in scripts/.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Allow running as `python backend/local_pusher.py` from the project root
# OR as `python local_pusher.py` from inside the backend folder.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_REPO_ROOT = _HERE.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import httpx

# Load .env from project root if present (python-dotenv is optional).
try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_REPO_ROOT / "pusher.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("local_pusher")

# ---------------------------------------------------------------------------
# Config — read from environment, with clear error messages.
# ---------------------------------------------------------------------------
RENDER_URL = os.environ.get("RENDER_URL", "").rstrip("/")
PUSH_SECRET = os.environ.get("PUSH_SECRET", "").strip()

if not RENDER_URL:
    log.error(
        "RENDER_URL is not set. "
        "Add it to your .env file or set it as an environment variable.\n"
        "Example: RENDER_URL=https://scanner-gules-rho.onrender.com"
    )
    sys.exit(1)

if not PUSH_SECRET:
    log.error(
        "PUSH_SECRET is not set. "
        "Generate a random secret, set it in Render env vars AND in your .env file.\n"
        "Example: PUSH_SECRET=mysupersecrettoken123"
    )
    sys.exit(1)

PUSH_URL = f"{RENDER_URL}/api/push/leaderboard"
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Referer": "https://finviz.com/",
}
FINVIZ_BASE = "https://finviz.com"
FINVIZ_MAP_PERF_BASE = f"{FINVIZ_BASE}/api/map_perf.ashx?t=themes"
FINVIZ_INDUSTRY_OV = f"{FINVIZ_BASE}/groups.ashx?g=industry&v=110&o=name&st=d1"
FINVIZ_INDUSTRY_PERF = f"{FINVIZ_BASE}/groups.ashx?g=industry&v=210&o=name&st=d1"

# ---------------------------------------------------------------------------
# Import the existing scraper helpers so we don't duplicate logic.
# ---------------------------------------------------------------------------
try:
    from backend.scraper import (
        build_finviz_themes_map_rows,
        build_finviz_industry_leaderboard_rows,
        fetch_vix_snapshot,
        fetch_tradingview_tape,
        INDUSTRY_THEME_MAP,
    )
except ModuleNotFoundError:
    from scraper import (                   # type: ignore[no-redef]
        build_finviz_themes_map_rows,
        build_finviz_industry_leaderboard_rows,
        fetch_vix_snapshot,
        fetch_tradingview_tape,
        INDUSTRY_THEME_MAP,
    )


# ---------------------------------------------------------------------------
# Push helpers
# ---------------------------------------------------------------------------

async def _push(client: httpx.AsyncClient, view: str, themes: list[dict[str, Any]]) -> bool:
    """POST one leaderboard view to Render. Returns True on success."""
    try:
        vix = await fetch_vix_snapshot()
    except Exception:
        vix = {"symbol": "^VIX", "close": 0.0, "change_pct": 0.0}

    try:
        tape = await fetch_tradingview_tape()
    except Exception:
        tape = []

    payload = {
        "view": view,
        "themes": themes,
        "vix": vix,
        "tape": tape,
        "leaderboardMeta": {
            "view": view,
            "source": "local_pusher",
        },
    }

    try:
        r = await client.post(
            PUSH_URL,
            json=payload,
            headers={"X-Push-Secret": PUSH_SECRET, "Content-Type": "application/json"},
            timeout=30.0,
        )
        if r.status_code == 200:
            data = r.json()
            log.info(
                "PUSH OK  view=%-10s rows=%d  pushed_at=%s",
                view, data.get("rows_stored", "?"), data.get("pushed_at", "?"),
            )
            return True
        else:
            log.error("PUSH FAIL view=%-10s status=%d  body=%s", view, r.status_code, r.text[:200])
            return False
    except Exception as exc:
        log.error("PUSH ERROR view=%-10s %s", view, exc)
        return False


async def run_push_cycle() -> None:
    """Scrape both views and push to Render. Runs once per invocation."""
    log.info("=== local_pusher starting push cycle ===")
    log.info("Target: %s", PUSH_URL)

    async with httpx.AsyncClient(timeout=40.0) as client:
        # --- Themes view ---
        try:
            log.info("Scraping Finviz Themes map…")
            themes_rows = await build_finviz_themes_map_rows()
            live_themes = [r for r in themes_rows if not r.get("seed")]
            if live_themes:
                log.info("Themes: %d live rows scraped.", len(live_themes))
                await _push(client, "themes", live_themes)
            else:
                log.warning("Themes: scraper returned only seed rows — skipping push.")
        except Exception as exc:
            log.error("Themes scrape failed: %s", exc, exc_info=True)

        # Small delay between requests to be polite to Finviz.
        await asyncio.sleep(3)

        # --- Industry view ---
        try:
            log.info("Scraping Finviz Industry groups…")
            industry_rows = await build_finviz_industry_leaderboard_rows()
            live_industry = [r for r in industry_rows if not r.get("seed")]
            if live_industry:
                log.info("Industry: %d live rows scraped.", len(live_industry))
                await _push(client, "industry", live_industry)
            else:
                log.warning("Industry: scraper returned only seed rows — skipping push.")
        except Exception as exc:
            log.error("Industry scrape failed: %s", exc, exc_info=True)

    log.info("=== push cycle complete ===\n")


if __name__ == "__main__":
    asyncio.run(run_push_cycle())
