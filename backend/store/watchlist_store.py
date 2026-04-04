"""
Server-side watchlist as a JSON file (single-worker friendly).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_BACKEND_DIR = Path(__file__).resolve().parent.parent


def _watchlist_file() -> Path:
    raw = os.getenv("WATCHLIST_STORE_PATH", "").strip()
    if raw:
        return Path(raw)
    return _BACKEND_DIR / "data" / "watchlist.json"


def _load() -> list[dict[str, Any]]:
    path = _watchlist_file()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(items: list[dict[str, Any]]) -> None:
    path = _watchlist_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(items, indent=2), encoding="utf-8")


def get_watchlist() -> list[dict[str, Any]]:
    return _load()


def add_ticker(
    ticker: str,
    theme: str | None = None,
    sector: str | None = None,
    grade: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("Ticker cannot be empty")

    items = _load()
    if any(i.get("ticker") == ticker for i in items):
        return {"ok": True, "ticker": ticker, "added": False, "reason": "already_exists"}

    items.append(
        {
            "ticker": ticker,
            "theme": theme,
            "sector": sector,
            "grade": grade,
            "note": note,
            "added_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    )
    _save(items)
    return {"ok": True, "ticker": ticker, "added": True}


def remove_ticker(ticker: str) -> dict[str, Any]:
    ticker = ticker.strip().upper()
    items = _load()
    before = len(items)
    items = [i for i in items if i.get("ticker") != ticker]
    if len(items) == before:
        return {"ok": False, "ticker": ticker, "removed": False, "reason": "not_found"}
    _save(items)
    return {"ok": True, "ticker": ticker, "removed": True}


def update_note(ticker: str, note: str) -> dict[str, Any]:
    ticker = ticker.strip().upper()
    items = _load()
    for item in items:
        if item.get("ticker") == ticker:
            item["note"] = note
            _save(items)
            return {"ok": True, "ticker": ticker, "updated": True}
    return {"ok": False, "ticker": ticker, "updated": False, "reason": "not_found"}
