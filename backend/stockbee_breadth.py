"""
Stockbee Market Monitor — published Google Sheet embedded on stockbee.blogspot.com/p/mm.html.

The blog page loads an iframe; the grid HTML is at:
  /spreadsheets/d/<id>/pubhtml/sheet?headers=false&gid=<year_tab>
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

STOCKBEE_MM_PAGE = "https://stockbee.blogspot.com/p/mm.html"
STOCKBEE_SPREADSHEET_ID = "1O6OhS7ciA8zwfycBfGPbP2fWJnR0pn2UUvFZVDP9jpE"

# Gids from the published spreadsheet widget (one tab per year).
STOCKBEE_SHEET_GIDS: dict[int, str] = {
    2026: "1082103394",
    2025: "780188096",
    2024: "1146204629",
    2023: "632667710",
    2022: "1394777987",
    2021: "1981550515",
    2020: "2093835319",
    2019: "1089581064",
    2018: "280217788",
    2017: "1391207759",
    2016: "233732777",
    2015: "0",
    2014: "1622090416",
    2013: "299051502",
    2012: "2142678713",
    2011: "24026662",
    2010: "1622166415",
    2009: "1397702728",
    2008: "1269494253",
    2007: "908739106",
}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_DATE_CELL = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")


def _sheet_pub_url(gid: str) -> str:
    return (
        f"https://docs.google.com/spreadsheets/d/{STOCKBEE_SPREADSHEET_ID}/"
        f"pubhtml/sheet?headers=false&gid={gid}"
    )


def _parse_us_date(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _num_cell(x: str) -> float | None:
    t = (x or "").strip().replace(",", "").replace("%", "")
    if not t or t == "—" or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _int_cell(x: str) -> int | None:
    n = _num_cell(x)
    if n is None:
        return None
    return int(round(n))


def _pick_year_gid() -> tuple[int, str]:
    try:
        from zoneinfo import ZoneInfo

        y = datetime.now(ZoneInfo("America/New_York")).year
    except Exception:
        y = datetime.now().year
    for try_y in (y, y - 1, y - 2, y - 3):
        gid = STOCKBEE_SHEET_GIDS.get(try_y)
        if gid is not None:
            return try_y, gid
    return 2026, STOCKBEE_SHEET_GIDS[2026]


def _row_to_record(cells: list[str]) -> dict[str, Any] | None:
    """
    Parse one Stockbee spreadsheet row.

    Column layout (1-indexed after row# at cells[0]):
      Old layout (17 cols total): ..., dn13_34d[13], worden_universe[14], t2108[15], sp[16]
      New layout (19+ cols):      ..., dn13_34d[13], atr_10x[14], above50pct[15],
                                       worden_universe[16], t2108[17], sp[18]

    The new columns are added by the spreadsheet owner when they extend it.
    If the sheet only has 17 cols we still parse correctly, returning None for the new fields.
    The backend also injects computed values via breadth_ext.py for the current trading day.
    """
    if len(cells) < 17:
        return None
    date_raw = cells[1].strip()
    if not _DATE_CELL.match(date_raw):
        return None
    dt = _parse_us_date(date_raw)
    if dt is None:
        return None
    date_iso = dt.date().isoformat()

    # Detect whether the sheet has the extended columns (19+ cells per row)
    extended = len(cells) >= 19

    if extended:
        atr_10x_ext: int | None = _int_cell(cells[14])
        above_50dma_pct: float | None = _num_cell(cells[15])
        worden_universe: int | None = _int_cell(cells[16])
        t2108_raw = cells[17].strip().replace("%", "")
        sp = _num_cell(cells[18])
    else:
        atr_10x_ext = None
        above_50dma_pct = None
        worden_universe = _int_cell(cells[14])
        t2108_raw = cells[15].strip().replace("%", "")
        sp = _num_cell(cells[16])

    try:
        t2108: float | None = float(t2108_raw) if t2108_raw else None
    except ValueError:
        t2108 = None

    return {
        "date": date_iso,
        "date_display": date_raw,
        "up_4_pct": _int_cell(cells[2]),
        "down_4_pct": _int_cell(cells[3]),
        "ratio_5d": _num_cell(cells[4]),
        "ratio_10d": _num_cell(cells[5]),
        "up_25_q": _int_cell(cells[6]),
        "down_25_q": _int_cell(cells[7]),
        "up_25_m": _int_cell(cells[8]),
        "down_25_m": _int_cell(cells[9]),
        "up_50_m": _int_cell(cells[10]),
        "down_50_m": _int_cell(cells[11]),
        "up_13_34d": _int_cell(cells[12]),
        "down_13_34d": _int_cell(cells[13]),
        "atr_10x_ext": atr_10x_ext,       # computed by breadth_ext.py for today; None for history
        "above_50dma_pct": above_50dma_pct, # same
        "worden_universe": worden_universe,
        "t2108": t2108,
        "sp_index": sp,
    }


def fetch_stockbee_market_monitor_sync(*, timeout: float = 45.0) -> dict[str, Any]:
    """
    Download the current year's published sheet HTML and parse breadth rows.
    Returns JSON-serializable dict with rows sorted by date DESC.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    year, gid = _pick_year_gid()
    url = _sheet_pub_url(gid)

    try:
        with httpx.Client(timeout=timeout, headers=BROWSER_HEADERS, follow_redirects=True) as client:
            r = client.get(url)
            r.raise_for_status()
            html = r.text
    except Exception as e:
        logger.warning("Stockbee sheet fetch failed: %s", e)
        return {
            "ok": False,
            "rows": [],
            "sheet_year": year,
            "source_url": url,
            "blog_url": STOCKBEE_MM_PAGE,
            "detail": str(e),
            "fetched_at_utc": now,
        }

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return {
            "ok": False,
            "rows": [],
            "sheet_year": year,
            "source_url": url,
            "blog_url": STOCKBEE_MM_PAGE,
            "detail": "No table in published sheet HTML.",
            "fetched_at_utc": now,
        }

    rows_out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        rec = _row_to_record(cells)
        if rec is not None:
            rows_out.append(rec)

    rows_out.sort(key=lambda r: r["date"], reverse=True)

    return {
        "ok": True,
        "rows": rows_out,
        "sheet_year": year,
        "source_url": url,
        "blog_url": STOCKBEE_MM_PAGE,
        "detail": None,
        "fetched_at_utc": now,
    }
