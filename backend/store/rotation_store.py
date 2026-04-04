"""
Daily RS / performance snapshots per theme (JSON file, no DB).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_BACKEND_DIR = Path(__file__).resolve().parent.parent
MAX_DAYS = 30


def _rotation_file() -> Path:
    raw = os.getenv("ROTATION_STORE_PATH", "").strip()
    if raw:
        return Path(raw)
    return _BACKEND_DIR / "data" / "rotation_history.json"


def _load() -> list[dict[str, Any]]:
    path = _rotation_file()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(records: list[dict[str, Any]]) -> None:
    path = _rotation_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")


def record_snapshot(themes: list[dict[str, Any]]) -> None:
    """
    Call when fresh theme leaderboard rows are available.
    One record per theme per UTC calendar day; keeps last MAX_DAYS days.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    records = _load()
    records = [r for r in records if r.get("date") != today]

    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=MAX_DAYS)).isoformat()
    records = [r for r in records if str(r.get("date", "")) >= cutoff]

    for t in themes:
        records.append(
            {
                "date": today,
                "theme": t.get("theme"),
                "sector": t.get("sector"),
                "rs1m": t.get("relativeStrength1M"),
                "perf1D": t.get("perf1D"),
                "perf1W": t.get("perf1W"),
                "perf1M": t.get("perf1M"),
            }
        )

    _save(records)


def get_snapshot(days: int = 10) -> dict[str, Any]:
    records = _load()
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    recent = [r for r in records if str(r.get("date", "")) >= cutoff]

    by_theme: dict[str, list[dict[str, Any]]] = {}
    for r in recent:
        theme = r.get("theme") or "Unknown"
        by_theme.setdefault(theme, []).append(r)

    results: list[dict[str, Any]] = []
    for theme, rows in by_theme.items():
        rows_sorted = sorted(rows, key=lambda x: str(x.get("date", "")))
        latest = rows_sorted[-1]
        oldest = rows_sorted[0]

        rs_latest = latest.get("rs1m")
        rs_oldest = oldest.get("rs1m")
        rs_delta = None
        if rs_latest is not None and rs_oldest is not None:
            try:
                rs_delta = round(float(rs_latest) - float(rs_oldest), 2)
            except (TypeError, ValueError):
                rs_delta = None

        results.append(
            {
                "theme": theme,
                "sector": latest.get("sector"),
                "rs1m": rs_latest,
                "perf1D": latest.get("perf1D"),
                "perf1W": latest.get("perf1W"),
                "perf1M": latest.get("perf1M"),
                "rs_delta": rs_delta,
                "history": rows_sorted,
            }
        )

    results.sort(key=lambda x: (x["rs_delta"] is not None, x["rs_delta"] or 0), reverse=True)

    return {
        "ok": True,
        "days": days,
        "themes": results,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
