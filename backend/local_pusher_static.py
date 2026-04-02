"""
local_pusher_static.py
----------------------
Scrapes Finviz from your local machine (residential IP) and writes the data
directly to frontend/public/ as static JSON files.

The frontend reads these files from its own Vercel domain — no Render needed.

Run:  py backend\local_pusher_static.py
Auto: set up Task Scheduler to run scripts\push_static.bat every 15 min on market days
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
)
log = logging.getLogger(__name__)

# ── resolve paths ────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT   = SCRIPT_DIR.parent
FRONTEND_PUBLIC = REPO_ROOT / "frontend" / "public"
FRONTEND_PUBLIC.mkdir(parents=True, exist_ok=True)

# Add backend to path so we can import scraper
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

try:
    from backend.scraper import (
        build_finviz_themes_map_rows,
        build_finviz_industry_leaderboard_rows,
        fetch_tradingview_tape,
    )
except ModuleNotFoundError:
    from scraper import (
        build_finviz_themes_map_rows,
        build_finviz_industry_leaderboard_rows,
        fetch_tradingview_tape,
    )


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, default=str), encoding="utf-8")
    log.info("Wrote %s (%d bytes)", path.name, path.stat().st_size)


async def run() -> None:
    log.info("=== local_pusher_static starting ===")
    log.info("Output dir: %s", FRONTEND_PUBLIC)

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    import inspect

    def _is_async(fn) -> bool:
        return inspect.iscoroutinefunction(fn)

    async def _call(fn, *args):
        """Call fn regardless of whether it is sync or async."""
        if _is_async(fn):
            return await fn(*args)
        return await asyncio.to_thread(fn, *args)

    # ── 1. Themes ─────────────────────────────────────────────────────────────
    log.info("Scraping Finviz Themes map…")
    try:
        theme_rows = await _call(build_finviz_themes_map_rows)
        live_themes = [r for r in theme_rows if not r.get("seed")]
        log.info("Themes: %d live rows scraped.", len(live_themes))
    except Exception as e:
        log.error("Themes scrape failed: %s", e)
        live_themes = []

    await asyncio.sleep(3)  # polite delay

    # ── 2. Industry ───────────────────────────────────────────────────────────
    log.info("Scraping Finviz Industry groups…")
    try:
        industry_rows = await _call(build_finviz_industry_leaderboard_rows)
        live_industry = [r for r in industry_rows if not r.get("seed")]
        log.info("Industry: %d live rows scraped.", len(live_industry))
    except Exception as e:
        log.error("Industry scrape failed: %s", e)
        live_industry = []

    await asyncio.sleep(2)

    # ── 3. VIX tape (optional enrichment) ─────────────────────────────────────
    try:
        tape = await _call(fetch_tradingview_tape)
    except Exception:
        tape = {}

    # ── 4. Write files ────────────────────────────────────────────────────────
    if live_themes:
        _write_json(
            FRONTEND_PUBLIC / "leaderboard-themes.json",
            {
                "themes": live_themes,
                "tape": tape,
                "meta": {"view": "themes", "source": "finviz", "updated_at": now_iso},
            },
        )

    if live_industry:
        _write_json(
            FRONTEND_PUBLIC / "leaderboard-industry.json",
            {
                "themes": live_industry,
                "tape": tape,
                "meta": {"view": "industry", "source": "finviz", "updated_at": now_iso},
            },
        )

    # ── 5. Git commit + push ──────────────────────────────────────────────────
    if live_themes or live_industry:
        log.info("Committing and pushing to GitHub…")
        os.chdir(str(REPO_ROOT))
        rc_add = os.system('git add frontend/public/leaderboard-themes.json frontend/public/leaderboard-industry.json')
        if rc_add == 0:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            rc_commit = os.system(f'git commit -m "data: leaderboard refresh {ts}" --allow-empty')
            if rc_commit == 0:
                rc_push = os.system("git push")
                if rc_push == 0:
                    log.info("Pushed to GitHub — Vercel will deploy in ~30s")
                else:
                    log.error("git push failed (rc=%d)", rc_push)
            else:
                log.warning("Nothing new to commit (data unchanged).")
        else:
            log.error("git add failed")
    else:
        log.warning("No live rows scraped — skipping git push.")

    log.info("=== push_static complete ===")


if __name__ == "__main__":
    asyncio.run(run())
