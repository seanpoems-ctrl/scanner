"""
Industry sub-industry movers for the Thematic Spotlight panel.

HTTP route is registered in main.py as GET /api/industry/subindustry-movers.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote


async def get_subindustry_movers_payload(industry_raw: str, parent_raw: str | None) -> dict[str, Any]:
    industry = unquote(industry_raw or "").strip()
    if industry.startswith(" - "):
        industry = industry[3:].strip()
    parent: str | None = None
    if parent_raw is not None and str(parent_raw).strip():
        p = unquote(str(parent_raw)).strip()
        if p.startswith(" - "):
            p = p[3:].strip()
        parent = p or None

    try:
        from backend.scraper import fetch_industry_subindustry_movers as _fetch
    except ImportError:
        from scraper import fetch_industry_subindustry_movers as _fetch

    data = await _fetch(industry)
    data["parent_category"] = parent
    return data
