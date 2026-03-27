"""
Core scraper module for POWER-THEME.

Data sources:
- Finviz (thematic candidates / screening pages)
- yfinance (price history and fundamentals)
- TradingView symbol snapshots (with yfinance ^GDAXI fallback for DAX when TV has no close)
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import re
from dataclasses import dataclass
from statistics import mean, median
from typing import Any

import httpx
import pandas as pd
import wordninja
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


FINVIZ_BASE_URL = "https://finviz.com"

# Initial-load speed: cap yfinance work per request (themes = distinct industries in this set).
MAX_TICKER_SNAPSHOTS = 30
MIN_AVG_DOLLAR_VOLUME = 100_000_000
MIN_THEME_COUNT = 3
MANUAL_OVERRIDE_TICKERS = ["AAOI", "AXTI", "NVDA", "RKLB", "TSLA"]

# Leader column selection rubric (user-defined).
LEADER_MIN_ADR_PCT = 4.0
LEADER_MIN_AVG_DOLLAR_VOLUME = 80_000_000
LEADER_MIN_MARKET_CAP = 2_000_000_000
LEADER_MIN_PRICE = 12.0
LEADER_MIN_RS_PCT = 85.0

# Episodic Pivot (EP) scanner gates (all must pass for ep_candidate).
EP_MIN_GAP_UP_PCT = 5.0
EP_MIN_OR_RVOL_RATIO = 3.0  # ≥3× median prior sessions’ first-30m volume
EP_GAP_GATE_FOR_OR_FETCH = 5.0  # only pull 5m history when gap exceeds this (saves calls)
THEME_URLS = {
    "ai": "/screener.ashx?v=111&f=ind_artificialintelligence,sec_technology&o=-perf4w",
    "space": "/screener.ashx?v=111&f=ind_aerospacedefense&o=-perf4w",
    "semiconductors": "/screener.ashx?v=111&f=ind_semiconductors&o=-perf4w",
    "nuclear": "/screener.ashx?v=111&f=ind_utilities&o=-perf4w",
}

FINVIZ_GROUPS_URL = (
    "https://finviz.com/groups.ashx"
    "?v=140&o=-perf4w"
)
FINVIZ_SECTOR_ROTATION_URL = "https://finviz.com/groups.ashx?g=sector&v=110&o=-perf1d"
# Industry groups (HTML tables); Performance tab adds multi-horizon %.
FINVIZ_INDUSTRY_OVERVIEW_URL = f"{FINVIZ_BASE_URL}/groups.ashx?g=industry&v=110&o=name&st=d1"
FINVIZ_INDUSTRY_PERF_URL = f"{FINVIZ_BASE_URL}/groups.ashx?g=industry&v=140&o=name&st=d1"
# Finviz Themes map: JSON performance per theme slug. `st` selects horizon (see `FINVIZ_MAP_PERF_ST`).
FINVIZ_MAP_PERF_THEMES_BASE = f"{FINVIZ_BASE_URL}/api/map_perf.ashx?t=themes"
# d1 session / 1D, w1 1W, w4 ~1M rolling, w13 ~3M, w26 ~6M (Finviz map API).
FINVIZ_MAP_PERF_ST = {"d1", "w1", "w4", "w13", "w26"}
TRADINGVIEW_SYMBOL_URL_BASE = "https://scanner.tradingview.com/symbol"
TRADINGVIEW_SYMBOL_FIELDS = "close,change,change_abs,description"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://finviz.com/",
}

SECTOR_FILTER_MAP = {
    "energy": "sec_energy",
    "technology": "sec_technology",
    "financial": "sec_financial",
    "consumer cyclical": "sec_consumercyclical",
    "healthcare": "sec_healthcare",
    "industrials": "sec_industrials",
    "basic materials": "sec_basicmaterials",
    "communication services": "sec_communicationservices",
    "utilities": "sec_utilities",
    "real estate": "sec_realestate",
    "consumer defensive": "sec_consumerdefensive",
}


@dataclass(slots=True)
class ThemeStock:
    ticker: str
    name: str
    sector: str
    industry: str
    market_cap: float
    avg_dollar_volume: float
    adr_pct: float
    close: float
    ema10: float
    ema20: float
    ema50: float
    ema200: float
    current_volume: float
    avg_volume_3m: float
    volume_buzz_pct: float
    prev_close: float
    prev_volume: float
    open_price: float
    gap_open_pct: float
    or_rvol_ratio: float | None
    today_return_pct: float
    month_return_pct: float
    ep_candidate: bool

    @property
    def qualifies_a_plus(self) -> bool:
        ema_stack_ok = self.close > self.ema10 > self.ema20 > self.ema50 > self.ema200
        return (
            ema_stack_ok
            and self.avg_dollar_volume > MIN_AVG_DOLLAR_VOLUME
            and self.adr_pct > 4.5
            and self.market_cap >= 2_000_000_000
            and self.volume_buzz_pct > 0
        )

    @property
    def qualifies_grade_a(self) -> bool:
        # Fallback grade when full A+ stack is scarce: price > 10EMA > 20EMA.
        ema_stack_10_20_ok = self.close > self.ema10 > self.ema20
        return (
            ema_stack_10_20_ok
            and self.avg_dollar_volume > MIN_AVG_DOLLAR_VOLUME
            and self.adr_pct > 4.5
            and self.market_cap >= 2_000_000_000
            and self.volume_buzz_pct > -25
        )

    @property
    def grade_label(self) -> str:
        if self.qualifies_a_plus:
            return "A+"
        if self.qualifies_grade_a:
            return "A"
        return "-"


@dataclass(slots=True)
class ThemeGroupPerformance:
    """Finviz sector row — multi-horizon % (Change ≈ session / 1D move)."""

    name: str
    perf_day_pct: float | None
    perf_week_pct: float | None
    perf_month_pct: float | None
    perf_quarter_pct: float | None
    perf_half_pct: float | None


def _return_pct_floor(values: list[float], pct: float) -> float:
    if not values:
        return float("inf")
    p = max(0.0, min(100.0, float(pct)))
    s = sorted(values)
    # nearest-rank percentile
    k = max(0, min(len(s) - 1, math.ceil((p / 100.0) * len(s)) - 1))
    return float(s[k])


def _passes_leader_rubric(stock: ThemeStock, *, rs_floor: float | None = None) -> bool:
    """Leader gate: ADR, trend, liquidity, and market cap."""
    # We store short/medium EMAs in `ema10`/`ema20` fields (close approximations of EMA9/EMA21).
    above_ema9_ema21_ema50 = stock.close > stock.ema10 and stock.close > stock.ema20 and stock.close > stock.ema50
    return (
        stock.adr_pct > LEADER_MIN_ADR_PCT
        and stock.close > LEADER_MIN_PRICE
        and above_ema9_ema21_ema50
        and stock.avg_dollar_volume > LEADER_MIN_AVG_DOLLAR_VOLUME
        and stock.market_cap > LEADER_MIN_MARKET_CAP
        and (rs_floor is None or stock.month_return_pct >= rs_floor)
    )


def _parse_compact_number(raw: str) -> float:
    cleaned = raw.strip().upper().replace("$", "").replace(",", "")
    if not cleaned:
        return 0.0
    if cleaned.endswith("B"):
        return float(cleaned[:-1]) * 1_000_000_000
    if cleaned.endswith("M"):
        return float(cleaned[:-1]) * 1_000_000
    if cleaned.endswith("K"):
        return float(cleaned[:-1]) * 1_000
    return float(cleaned)


def _parse_percent(raw: str) -> float:
    cleaned = raw.strip().replace("%", "").replace(",", "")
    return float(cleaned) if cleaned else 0.0


def _normalize_group_name(name: str) -> str:
    return " ".join(name.lower().strip().split())


def _safe_fast_info_value(info: Any, key: str) -> Any:
    if isinstance(info, dict):
        return info.get(key)
    try:
        return info.get(key)  # yfinance LazyDict-like objects support .get
    except Exception:
        return None


def _extract_tickers_from_html(html: str) -> list[str]:
    # Focus extraction on stock data rows only, then regex ticker links inside each row.
    soup = BeautifulSoup(html, "html.parser")
    data_rows = [row for row in soup.find_all("tr") if "quote.ashx?t=" in str(row)]

    # Finviz ticker links typically follow: quote.ashx?t=SYMBOL
    patterns = [
        # Standard format, supports tickers like BRK-B and BF.B
        r"quote\.ashx\?t=([A-Z][A-Z0-9\.-]*)",
        # Sometimes extra query params are present after ticker
        r"quote\.ashx\?t=([A-Z][A-Z0-9\.-]*)&",
        # Alternative encoded links where t= appears outside quote.ashx
        r"[?&]t=([A-Z][A-Z0-9\.-]*)",
    ]
    found: set[str] = set()
    row_fragments = [str(row) for row in data_rows]
    for pattern in patterns:
        for fragment in row_fragments:
            for match in re.findall(pattern, fragment):
                symbol = match.strip().upper()
                # Filter obviously invalid captures from broad query-string pattern.
                if 1 <= len(symbol) <= 8 and re.fullmatch(r"[A-Z][A-Z0-9\.-]*", symbol):
                    found.add(symbol)

    # Keep audit slots for common-stock style symbols (1-4 letters), skip 5-letter warrant-like symbols.
    tickers = sorted(
        symbol for symbol in found if 1 <= len(symbol) <= 4 and re.fullmatch(r"[A-Z]{1,4}", symbol)
    )
    logger.info("Regex found %d potential ticker symbols from %d data rows.", len(tickers), len(data_rows))
    return tickers


def parse_finviz_quote_short_float_pct(html: str) -> float | None:
    """
    Finviz quote snapshot: label cell 'Short Float' → value like '0.85%' (% of float short).
    Returns percent as a number (e.g. 0.85), not fraction.
    """
    soup = BeautifulSoup(html, "html.parser")
    tds = soup.select("td.snapshot-td2")
    for i, td in enumerate(tds):
        if td.get_text(strip=True) != "Short Float":
            continue
        if i + 1 >= len(tds):
            return None
        raw = tds[i + 1].get_text(strip=True)
        if not raw or raw in ("-", "N/A"):
            return None
        try:
            return _parse_percent(raw)
        except ValueError:
            return None
    return None


def extract_us_ticker_from_tv_scan_row(row: dict[str, Any]) -> str | None:
    """Normalize TradingView scanner ticker (e.g. NASDAQ:AAPL) to a US symbol Finviz understands."""
    raw = row.get("ticker")
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if ":" in s:
        s = s.rsplit(":", 1)[-1]
    s = s.strip().upper().replace(".", "-")
    if not s or not re.fullmatch(r"[A-Z][A-Z0-9-]{0,14}", s):
        return None
    return s


async def fetch_finviz_short_float_pct_batch(
    tickers: list[str],
    *,
    max_concurrent: int = 10,
) -> dict[str, float | None]:
    """One Finviz quote page per unique ticker; returns percent values (same units as the site)."""
    uniq: list[str] = []
    seen: set[str] = set()
    for t in tickers:
        u = t.strip().upper()
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    if not uniq:
        return {}

    sem = asyncio.Semaphore(max_concurrent)
    out: dict[str, float | None] = dict.fromkeys(uniq)

    async def one(client: httpx.AsyncClient, sym: str) -> None:
        async with sem:
            url = f"{FINVIZ_BASE_URL}/quote.ashx"
            try:
                r = await client.get(url, params={"t": sym}, timeout=25.0)
                r.raise_for_status()
                out[sym] = parse_finviz_quote_short_float_pct(r.text)
            except Exception as exc:
                logger.debug("Finviz Short Float fetch failed for %s: %s", sym, exc)
                out[sym] = None

    async with httpx.AsyncClient(headers=BROWSER_HEADERS, follow_redirects=True) as client:
        await asyncio.gather(*(one(client, sym) for sym in uniq))
    return out


def _yf_short_percent_of_float_pct_sync(ticker: str) -> float | None:
    """
    Yahoo (yfinance): returns Short % of Float as a percent number (e.g. 0.85), not a fraction.
    yfinance may return a fraction (0.0085) or percent depending on upstream; we normalize.
    """
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        return None
    raw = info.get("shortPercentOfFloat")
    if raw is None:
        raw = info.get("shortPercentFloat")
    if raw is None:
        return None
    try:
        v = float(raw)
    except Exception:
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    # Normalize: if it looks like a fraction (<=1), convert to percent.
    return v * 100.0 if v <= 1.0 else v


async def fetch_yfinance_short_percent_float_pct_batch(
    tickers: list[str],
    *,
    max_concurrent: int = 5,
) -> dict[str, float | None]:
    """
    Batch yfinance Short % of Float.
    This is per-symbol work and is susceptible to Yahoo throttling; keep concurrency conservative.
    """
    uniq: list[str] = []
    seen: set[str] = set()
    for t in tickers:
        u = t.strip().upper()
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    if not uniq:
        return {}

    sem = asyncio.Semaphore(max_concurrent)
    out: dict[str, float | None] = dict.fromkeys(uniq)
    rate_limited = {"hit": False}

    async def one(sym: str) -> None:
        async with sem:
            if rate_limited["hit"]:
                out[sym] = None
                return
            try:
                out[sym] = await asyncio.to_thread(_yf_short_percent_of_float_pct_sync, sym)
            except YFRateLimitError:
                rate_limited["hit"] = True
                out[sym] = None
            except Exception:
                out[sym] = None

    await asyncio.gather(*(one(sym) for sym in uniq))
    return out


async def _fetch_finviz_html(client: httpx.AsyncClient, path: str) -> str:
    # Preserve caller sort (e.g. o=-gap); only default to 4W perf when no o= present.
    if "o=" not in path:
        path = f"{path}&o=-perf4w" if "?" in path else f"{path}?o=-perf4w"
    url = f"{FINVIZ_BASE_URL}{path}"
    response = await client.get(url)
    response.raise_for_status()
    html = response.text
    logger.info("HTML Length: %d | path=%s", len(html), path)
    return html


# Finviz overview (v=111): US liquid names, gap-up, sorted by gap (desc).
PREMARKET_GAP_SCREENER_PATH = (
    "/screener.ashx?v=111&f=cap_midover,geo_usa,sh_avgvol_o500,ta_gap_u1&o=-gap"
)


def _parse_finviz_screener_overview_html(html: str) -> list[dict[str, Any]]:
    """Parse Finviz stock screener overview table into row dicts (snake_case headers)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.styled-table-new")
    if table is None:
        for t in soup.find_all("table"):
            if t.select_one("a[href*='quote.ashx?t=']"):
                table = t
                break
    if table is None:
        return []

    all_rows = table.select("tr")
    if not all_rows:
        return []

    def cells_of(r):
        return r.select("td, th")

    start_idx = 0
    header_cells = cells_of(all_rows[0])
    headers_raw = [c.get_text(" ", strip=True) for c in header_cells]

    if not any(h.lower() in ("ticker", "symbol") for h in headers_raw if h):
        for i, r in enumerate(all_rows[:4]):
            texts = [c.get_text(" ", strip=True) for c in cells_of(r)]
            if any(t.lower() == "ticker" for t in texts):
                header_cells = cells_of(r)
                headers_raw = texts
                start_idx = i + 1
                break

    headers: list[str] = []
    seen: dict[str, int] = {}
    for h in headers_raw:
        base = re.sub(r"[^a-z0-9]+", "_", (h or "col").lower()).strip("_") or "col"
        n = seen.get(base, 0)
        seen[base] = n + 1
        headers.append(base if n == 0 else f"{base}_{n}")

    out: list[dict[str, Any]] = []
    for r in all_rows[start_idx:]:
        cels = cells_of(r)
        if len(cels) < 2:
            continue
        vals: list[str] = []
        for c in cels:
            link = c.select_one("a[href*='quote.ashx?t=']")
            if link is not None:
                href = link.get("href") or ""
                m = re.search(r"[?&]t=([A-Za-z0-9.\-]+)", href)
                vals.append(m.group(1).upper() if m else (link.get_text(" ", strip=True) or ""))
            else:
                vals.append(c.get_text(" ", strip=True) or "")

        if not any(vals):
            continue
        if vals[0].lower() in ("ticker", "no", "#") or vals[0].startswith("Filters:"):
            continue

        row_dict: dict[str, Any] = {}
        for i, key in enumerate(headers):
            if i < len(vals):
                row_dict[key] = vals[i]

        tk = row_dict.get("ticker") or row_dict.get("symbol") or ""
        if isinstance(tk, str) and tk:
            parts = tk.split()
            tk_clean = parts[0].upper() if parts else ""
            if tk_clean and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,6}", tk_clean):
                row_dict["ticker"] = tk_clean.replace(".", "-")

        out.append(row_dict)
    return out


async def fetch_premarket_gappers(*, max_pages: int = 4) -> dict[str, Any]:
    """
    Pre-market style gap scan via Finviz (delayed; mirrors their gap-up universe).
    Paginates overview rows (~20 per page).
    """
    rows_all: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=35.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        for page in range(max(1, max_pages)):
            r_start = 1 + page * 20
            suffix = "" if page == 0 else f"&r={r_start}"
            path = f"{PREMARKET_GAP_SCREENER_PATH}{suffix}"
            html = await _fetch_finviz_html(client, path)
            page_rows = _parse_finviz_screener_overview_html(html)
            if not page_rows:
                break
            rows_all.extend(page_rows)
            if len(page_rows) < 15:
                break
    return {
        "source": "finviz",
        "screener_path": PREMARKET_GAP_SCREENER_PATH,
        "row_count": len(rows_all),
        "rows": rows_all,
    }


def _extract_top_sector_names(groups_html: str, limit: int = 3) -> list[str]:
    soup = BeautifulSoup(groups_html, "html.parser")
    table = (
        soup.select_one("table.groups_table")
        or soup.select_one("table.table-light")
        or soup.select_one("table[class*='table-light']")
    )
    if table is None:
        return []

    rows = table.select("tr")
    if not rows:
        return []

    header_cells = rows[0].select("td, th")
    headers_text = [cell.get_text(strip=True).lower() for cell in header_cells]
    name_idx = next((i for i, h in enumerate(headers_text) if h in {"name", "group", "sector"}), None)
    perf_month_idx = next((i for i, h in enumerate(headers_text) if h in {"perf month", "perf 1m"}), None)
    if name_idx is None:
        return []

    parsed: list[tuple[str, float]] = []
    for row in rows[1:]:
        cells = row.select("td")
        if len(cells) <= name_idx:
            continue
        raw_name = cells[name_idx].get_text(strip=True)
        if not raw_name:
            continue
        perf = float("-inf")
        if perf_month_idx is not None and len(cells) > perf_month_idx:
            try:
                perf = _parse_percent(cells[perf_month_idx].get_text(strip=True))
            except ValueError:
                perf = float("-inf")
        parsed.append((raw_name, perf))

    parsed.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in parsed[:limit]]


def _extract_first_sector_name(groups_html: str) -> str | None:
    """Extract top sector name from already-sorted sector groups HTML."""
    soup = BeautifulSoup(groups_html, "html.parser")
    table = (
        soup.select_one("table.groups_table")
        or soup.select_one("table.table-light")
        or soup.select_one("table[class*='table-light']")
        or soup.select_one("table[bgcolor='#d3d3d3']")
    )
    if table is None:
        return None

    rows = table.select("tr")
    if len(rows) < 2:
        return None

    header_cells = rows[0].select("td, th")
    headers_text = [cell.get_text(strip=True).lower() for cell in header_cells]
    name_idx = next((i for i, h in enumerate(headers_text) if h in {"name", "group", "sector"}), None)
    if name_idx is None:
        return None

    for row in rows[1:]:
        cells = row.select("td")
        if len(cells) <= name_idx:
            continue
        name = cells[name_idx].get_text(strip=True)
        if name:
            return name
    return None


async def fetch_top_sector_rotation() -> tuple[str | None, str | None]:
    """
    Fetch top-performing sector (1D) and return (sector_name, screener_path).
    """
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        response = await client.get(FINVIZ_SECTOR_ROTATION_URL)
        response.raise_for_status()
        html = response.text
        logger.info("HTML Length: %d | path=%s", len(html), FINVIZ_SECTOR_ROTATION_URL)

    sector_name = _extract_first_sector_name(html)
    if not sector_name:
        return None, None

    sector_key = _normalize_group_name(sector_name)
    finviz_filter = SECTOR_FILTER_MAP.get(sector_key)
    if not finviz_filter:
        return sector_name, None

    screener_path = f"/screener.ashx?v=111&f={finviz_filter}&o=-perf4w"
    return sector_name, screener_path


async def fetch_finviz_tickers(
    screener_path: str = "/screener.ashx?v=111&f=ind_artificialintelligence,sec_technology&o=-perf4w",
) -> list[str]:
    """
    Fetch ticker symbols from a Finviz screener page.

    Note: The exact screener URL can be adjusted by caller to target
    specific thematic buckets or filters.
    """
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        aggregated: set[str] = set()

        # Primary thematic entrypoint (AI) + additional user-focused themes.
        prioritized_paths = [screener_path, *THEME_URLS.values()]
        seen_paths: set[str] = set()
        for theme_path in prioritized_paths:
            if theme_path in seen_paths:
                continue
            seen_paths.add(theme_path)
            html = await _fetch_finviz_html(client, theme_path)
            tickers = _extract_tickers_from_html(html)
            logger.info("Finviz parsed [%s] | tickers found=%d", theme_path, len(tickers))
            aggregated.update(tickers)

        if aggregated:
            aggregated_list = sorted(aggregated)
            random.shuffle(aggregated_list)
            logger.info("Thematic aggregate tickers=%d", len(aggregated_list))
            logger.info("Finviz sample tickers [%s]", ", ".join(aggregated_list[:10]))
            return aggregated_list

        # Try pure-play thematic scans first.
        pure_play_paths = [
            "/screener.ashx?v=111&f=sec_technology,ind_artificialintelligence&o=-perf4w",
            "/screener.ashx?v=111&f=sec_industrials,ind_aerospacedefense",
            "/screener.ashx?v=111&s=ta_topgainers",
        ]
        for fallback_path in pure_play_paths:
            fallback_html = await _fetch_finviz_html(client, fallback_path)
            fallback_tickers = _extract_tickers_from_html(fallback_html)
            logger.info("Finviz fallback parsed [%s] | tickers found=%d", fallback_path, len(fallback_tickers))
            if fallback_tickers:
                logger.info("Using fallback screener [%s]", fallback_path)
                logger.info("Fallback sample tickers [%s]", ", ".join(fallback_tickers[:10]))
                return fallback_tickers

        # If pure-play scans are empty, fallback to top 3 performing sectors.
        sector_groups_path = "/groups.ashx?v=140&g=sector&o=-perf4w"
        sector_groups_html = await _fetch_finviz_html(client, sector_groups_path)
        top_sectors = _extract_top_sector_names(sector_groups_html, limit=3)
        logger.info("Top sector fallback candidates: %s", ", ".join(top_sectors) if top_sectors else "none")

        sector_tickers: list[str] = []
        for sector_name in top_sectors:
            sector_key = _normalize_group_name(sector_name)
            finviz_filter = SECTOR_FILTER_MAP.get(sector_key)
            if not finviz_filter:
                continue
            sector_path = f"/screener.ashx?v=111&f={finviz_filter}&o=-perf4w"
            sector_html = await _fetch_finviz_html(client, sector_path)
            parsed = _extract_tickers_from_html(sector_html)
            logger.info("Sector fallback parsed [%s] | tickers found=%d", sector_name, len(parsed))
            sector_tickers.extend(parsed)

        sector_tickers = sorted(set(sector_tickers))
        if sector_tickers:
            logger.info("Using top-sector fallback tickers (%d found).", len(sector_tickers))
            logger.info("Top-sector sample tickers [%s]", ", ".join(sector_tickers[:10]))
            return sector_tickers

    logger.warning("Finviz returned no tickers after primary + fallbacks.")
    return []


async def fetch_finviz_tickers_deterministic(
    screener_path: str,
    *,
    max_pages: int = 20,
) -> list[str]:
    """
    Deterministic ticker fetch for a single screener filter.

    Unlike `fetch_finviz_tickers`, this does NOT fan out to other theme URLs or fallbacks.
    It paginates using Finviz's `r=` offset when possible.
    """
    path = (screener_path or "").strip() or "/screener.ashx?v=111"
    tickers: set[str] = set()
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        for i in range(max_pages):
            # Finviz uses 1-indexed row offset: r=1,21,41,...
            offset = 1 + (i * 20)
            page_path = f"{path}&r={offset}" if "?" in path else f"{path}?r={offset}"
            html = ""
            for attempt in range(3):
                try:
                    html = await _fetch_finviz_html(client, page_path)
                    break
                except httpx.HTTPStatusError as e:
                    code = e.response.status_code if e.response is not None else None
                    if code == 429:
                        # Back off a bit; Finviz rate limits aggressively.
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    raise
            if not html:
                break
            page = _extract_tickers_from_html(html)
            if not page:
                break
            before = len(tickers)
            tickers.update(page)
            if len(tickers) == before:
                break
    return sorted(tickers)


async def fetch_finviz_industry_filter_map() -> dict[str, str]:
    """
    Map industry display name -> Finviz screener filter token (e.g. 'ind_oilgasdrilling').
    """
    html = await _fetch_finviz_document(FINVIZ_INDUSTRY_OVERVIEW_URL)
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, str] = {}
    for a in soup.select("a[href*='screener.ashx']"):
        href = a.get("href") or ""
        text = a.get_text(strip=True) or ""
        if not href or not text:
            continue
        # Look for f=ind_xxx within the href.
        m = re.search(r"[?&]f=([^&]+)", href)
        if not m:
            continue
        f = m.group(1)
        # f can contain multiple filters separated by commas; keep the industry token.
        tokens = [t.strip() for t in f.split(",") if t.strip()]
        ind = next((t for t in tokens if t.startswith("ind_")), None)
        if not ind:
            continue
        out[_normalize_group_name(text)] = ind
    return out


def _finviz_col_index(headers_text: list[str], *candidates: str) -> int | None:
    """First column whose header contains any candidate substring (lowercased)."""
    for i, h in enumerate(headers_text):
        for c in candidates:
            if c in h:
                return i
    return None


async def fetch_finviz_groups_performance() -> dict[str, ThemeGroupPerformance]:
    """
    Parse Finviz Groups performance table: Change (1D proxy), week / month / quarter / half.

    Rows are sectors (Energy, Technology, …). Match themes by yfinance `sector`, not industry.
    """
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        response = await client.get(FINVIZ_GROUPS_URL)
        response.raise_for_status()
        logger.info("HTML Length: %d | path=%s", len(response.text), FINVIZ_GROUPS_URL)

    soup = BeautifulSoup(response.text, "html.parser")
    table = (
        soup.select_one("table.groups_table")
        or soup.select_one("table.table-light")
        or soup.select_one("table[class*='table-light']")
        or soup.select_one("table[bgcolor='#d3d3d3']")
    )
    if table is None:
        return {}

    rows = table.select("tr")
    if not rows:
        return {}

    # Finviz uses <th> for header cells; <td>-only selection yields empty headers and {} (all theme RS null).
    header_cells = rows[0].select("td, th")
    headers_text = [cell.get_text(strip=True).lower() for cell in header_cells]
    name_idx = next((i for i, h in enumerate(headers_text) if h in {"name", "group", "sector"}), None)
    perf_month_idx = _finviz_col_index(headers_text, "perf month")
    perf_week_idx = _finviz_col_index(headers_text, "perf week")
    perf_quart_idx = _finviz_col_index(headers_text, "perf quart")
    perf_half_idx = _finviz_col_index(headers_text, "perf half")
    # "change" column (group % move); avoid matching unrelated headers.
    change_idx = next((i for i, h in enumerate(headers_text) if h == "change"), None)
    if name_idx is None or perf_month_idx is None:
        return {}

    def _cell_pct(cells: list[Any], idx: int | None) -> float | None:
        if idx is None or idx >= len(cells):
            return None
        raw = cells[idx].get_text(strip=True)
        if not raw:
            return None
        try:
            return _parse_percent(raw)
        except ValueError:
            return None

    parsed: dict[str, ThemeGroupPerformance] = {}
    for row in rows[1:]:
        cells = row.select("td, th")
        idx_needed = [name_idx, perf_month_idx]
        for x in (perf_week_idx, perf_quart_idx, perf_half_idx, change_idx):
            if x is not None:
                idx_needed.append(x)
        if len(cells) <= max(idx_needed):
            continue
        name = cells[name_idx].get_text(strip=True)
        if not name:
            continue
        perf_month = _cell_pct(cells, perf_month_idx)
        if perf_month is None:
            continue
        key = _normalize_group_name(name)
        parsed[key] = ThemeGroupPerformance(
            name=name,
            perf_day_pct=_cell_pct(cells, change_idx),
            perf_week_pct=_cell_pct(cells, perf_week_idx),
            perf_month_pct=perf_month,
            perf_quarter_pct=_cell_pct(cells, perf_quart_idx),
            perf_half_pct=_cell_pct(cells, perf_half_idx),
        )
    return parsed


async def _fetch_finviz_document(url: str) -> str:
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        logger.info("GET len=%d | %s", len(response.text), url)
        return response.text


def _parse_finviz_groups_data_rows(html: str) -> tuple[list[str], list[list[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    table = (
        soup.select_one("table.groups_table")
        or soup.select_one("table.table-light")
        or soup.select_one("table[class*='table-light']")
    )
    if table is None:
        return [], []

    rows = table.select("tr")
    if not rows:
        return [], []

    header_cells = rows[0].select("td, th")
    headers = [cell.get_text(strip=True).lower() for cell in header_cells]
    data: list[list[str]] = []
    for row in rows[1:]:
        cells = [c.get_text(strip=True) for c in row.select("td, th")]
        if cells and any(x.strip() for x in cells):
            data.append(cells)
    return headers, data


def _header_exact_idx(headers: list[str], label: str) -> int | None:
    for i, h in enumerate(headers):
        if h == label:
            return i
    return None


# Finviz theme slugs are `{bucket}{tail}` (no separators). Longest prefix wins → "Bucket · {human tail}".
_THEME_SLUG_PREFIXES: tuple[tuple[str, str], ...] = tuple(
    sorted(
        (
            ("entertainment", "Entertainment · "),
            ("environmental", "Environmental · "),
            ("transportation", "Transportation · "),
            ("cybersecurity", "Cybersecurity · "),
            ("agriculture", "Agriculture · "),
            ("automation", "Automation · "),
            ("autonomous", "Autonomous · "),
            ("blockchain", "Blockchain · "),
            ("realestate", "Real Estate · "),
            ("healthcare", "Healthcare · "),
            ("biometrics", "Biometrics · "),
            ("ecommerce", "E-commerce · "),
            ("education", "Education · "),
            ("nutrition", "Nutrition · "),
            ("smarthome", "Smart Home · "),
            ("vareality", "VR / AR · "),
            ("software", "Software · "),
            ("hardware", "Hardware · "),
            ("robotics", "Robotics · "),
            ("consumer", "Consumer · "),
            ("longevity", "Longevity · "),
            ("nanotech", "Nanotech · "),
            ("wearables", "Wearables · "),
            ("bigdata", "Big Data · "),
            ("defense", "Defense · "),
            ("fintech", "Fintech · "),
            ("quantum", "Quantum · "),
            ("telecom", "Telecom · "),
            ("energy", "Energy · "),
            ("social", "Social · "),
            ("semis", "Semiconductors · "),
            ("space", "Space · "),
            ("cloud", "Cloud · "),
            ("comm", "Commodities · "),
            ("evs", "EV · "),
            ("iot", "IoT · "),
        ),
        key=lambda x: -len(x[0]),
    )
)

# Exact `rest` after bucket strip (wordninja fails on these glued tokens).
_THEME_REST_OVERRIDES: dict[str, str] = {
    "hsaas": "Horizontal SaaS",
    "vsaas": "Vertical SaaS",
    "aiplatforms": "AI Platforms",
    "tokenization": "Tokenization",
    "immersive": "Immersive",
    "agingpharma": "Aging Pharma",
    "voiceai": "Voice AI",
    "agrisofts": "Agri Softs",
    "industrialiot": "Industrial IoT",
    "iot": "IoT",
    "dprinting": "3D Printing",
}

_TAIL_JOIN_FIXES: tuple[tuple[str, str], ...] = (
    ("hyper scale rs", "Hyperscalers"),
    ("hyper scale", "Hyperscale"),
    ("datacenter s", "data centers"),
    ("data center s", "data centers"),
    ("datacenters s", "data centers"),
    ("data centers s", "data centers"),
    ("devo ps", "DevOps"),
    ("industrial i ot", "industrial iot"),
    ("i ot", "iot"),
    ("d printing", "3d printing"),
    ("token iz ation", "tokenization"),
    ("i mmer sive", "immersive"),
    ("aging p harm a", "aging pharma"),
    ("voice a i", "voice ai"),
    ("a i platforms", "ai platforms"),
    ("agri soft s", "agri softs"),
    ("s at com", "satcom"),
)

# Title-case tweaks after splitting on spaces (lower key → display token).
_THEME_TAIL_ACRONYMS: dict[str, str] = {
    "iot": "IoT",
    "it": "IT",
    "3d": "3D",
    "ai": "AI",
    "agi": "AGI",
    "saas": "SaaS",
    "paas": "PaaS",
    "iaas": "IaaS",
    "iam": "IAM",
    "bi": "BI",
    "dtc": "DTC",
    "ar": "AR",
    "vr": "VR",
    "g": "5G",
    "satcom": "SatCom",
}


def _title_theme_tail_words(tail: str) -> str:
    parts: list[str] = []
    for raw in tail.split():
        if not raw:
            continue
        key = raw.lower()
        parts.append(_THEME_TAIL_ACRONYMS.get(key, raw.capitalize()))
    return " ".join(parts)


def _humanize_finviz_theme_slug(slug: str) -> str:
    s = slug.lower().strip()
    if not s:
        return slug
    rest = s
    prefix_label = ""
    if s.startswith("ai") and len(s) > 2:
        prefix_label = "AI · "
        rest = s[2:]
    else:
        for p, lab in _THEME_SLUG_PREFIXES:
            if rest.startswith(p):
                prefix_label = lab
                rest = rest[len(p) :]
                break
    if not prefix_label:
        prefix_label = "Themes · "
        rest = s

    if rest in _THEME_REST_OVERRIDES:
        tail = _THEME_REST_OVERRIDES[rest]
    else:
        tail_raw = " ".join(wordninja.split(rest)) if rest else ""
        tl = tail_raw.lower()
        for wrong, right in _TAIL_JOIN_FIXES:
            if wrong in tl:
                tl = tl.replace(wrong, right.lower())
        tail = _title_theme_tail_words(tl)
    # Some Finviz slugs encode `{bucket}{bucket}` (e.g. `automationautomation`) which becomes redundant.
    # Keep the bucket, normalize tail to a meaningful subtheme label.
    bucket = prefix_label.replace(" · ", "").strip().lower()
    if bucket and tail.strip().lower() == bucket:
        tail = "Core"
    out = (prefix_label + tail).strip()
    return out or slug


def _series_theme_dict(
    *,
    theme: str,
    sector: str,
    rs1m: float | None,
    perf1d: float | None,
    perf1w: float | None,
    perf1m: float | None = None,
    perf3m: float | None,
    perf6m: float | None,
    total_count: int,
    theme_dollar_volume: float,
) -> dict[str, Any]:
    return {
        "theme": theme,
        "sector": sector,
        "relativeStrength1M": None if rs1m is None else round(rs1m, 4),
        "perf1D": None if perf1d is None else round(perf1d, 2),
        "perf1W": None if perf1w is None else round(perf1w, 2),
        "perf1M": None if perf1m is None else round(perf1m, 2),
        "perf3M": None if perf3m is None else round(perf3m, 2),
        "perf6M": None if perf6m is None else round(perf6m, 2),
        "relativeStrengthQualifierRatio": 0.0,
        "leaders": [],
        "qualifiedCount": 0,
        "aPlusCount": 0,
        "gradeACount": 0,
        "totalCount": total_count,
        "themeDollarVolume": round(theme_dollar_volume, 2),
        "themePrevDollarVolume": 0.0,
        "themeAvg20DollarVolume": 0.0,
        "highLiquidity": False,
        "accumulation": False,
        "stocks": [],
    }


async def build_finviz_industry_leaderboard_rows() -> list[dict[str, Any]]:
    ov_html, perf_html = await asyncio.gather(
        _fetch_finviz_document(FINVIZ_INDUSTRY_OVERVIEW_URL),
        _fetch_finviz_document(FINVIZ_INDUSTRY_PERF_URL),
    )
    ind_filter_map = await fetch_finviz_industry_filter_map()
    h1, rows1 = _parse_finviz_groups_data_rows(ov_html)
    h2, rows2 = _parse_finviz_groups_data_rows(perf_html)
    ni1 = _header_exact_idx(h1, "name")
    ni2 = _header_exact_idx(h2, "name")
    if ni1 is None or ni2 is None:
        logger.warning("Industry table parse failed (name column).")
        return []

    stocks_i = _header_exact_idx(h1, "stocks")
    mcap_i = _header_exact_idx(h1, "market cap")
    ch1_i = _header_exact_idx(h1, "change")
    pw = _header_exact_idx(h2, "perf week")
    pm = _header_exact_idx(h2, "perf month")
    pq = _header_exact_idx(h2, "perf quart")
    ph = _header_exact_idx(h2, "perf half")
    ch2_i = _header_exact_idx(h2, "change")

    overview: dict[str, dict[str, Any]] = {}
    for cells in rows1:
        if len(cells) <= ni1:
            continue
        raw_name = cells[ni1].strip()
        if not raw_name:
            continue
        key = _normalize_group_name(raw_name)
        stocks = 0
        if stocks_i is not None and stocks_i < len(cells):
            try:
                stocks = int(cells[stocks_i].replace(",", ""))
            except ValueError:
                stocks = 0
        mcap_blob = cells[mcap_i] if mcap_i is not None and mcap_i < len(cells) else ""
        chg: float | None = None
        if ch1_i is not None and ch1_i < len(cells):
            try:
                chg = _parse_percent(cells[ch1_i])
            except ValueError:
                chg = None
        overview[key] = {
            "name": raw_name,
            "stocks": stocks,
            "market_cap_blob": mcap_blob,
            "themeDollarVolume": _parse_compact_number(mcap_blob) if mcap_blob else 0.0,
            "change_ov": chg,
        }

    perf_map: dict[str, dict[str, float | None]] = {}
    for cells in rows2:
        if len(cells) <= ni2:
            continue
        raw_name = cells[ni2].strip()
        if not raw_name:
            continue
        key = _normalize_group_name(raw_name)

        def cell_pct(idx: int | None) -> float | None:
            if idx is None or idx >= len(cells):
                return None
            raw = cells[idx].strip()
            if not raw:
                return None
            try:
                return _parse_percent(raw)
            except ValueError:
                return None

        perf_map[key] = {
            "pw": cell_pct(pw),
            "pm": cell_pct(pm),
            "pq": cell_pct(pq),
            "ph": cell_pct(ph),
            "ch": cell_pct(ch2_i),
        }

    themes: list[dict[str, Any]] = []
    for key, ov in overview.items():
        p = perf_map.get(key, {})
        ch_ov = ov.get("change_ov")
        ch_pr = p.get("ch")
        perf1d = ch_pr if ch_pr is not None else ch_ov
        pm = p.get("pm")
        themes.append(
            _series_theme_dict(
                theme=ov["name"],
                sector="Industry",
                rs1m=pm,
                perf1d=perf1d,
                perf1w=p.get("pw"),
                perf1m=pm,
                perf3m=p.get("pq"),
                perf6m=p.get("ph"),
                total_count=int(ov.get("stocks", 0)),
                theme_dollar_volume=float(ov.get("themeDollarVolume", 0.0)),
            )
        )

    themes.sort(
        key=lambda x: (
            x["relativeStrength1M"] is not None,
            x["relativeStrength1M"] if x["relativeStrength1M"] is not None else float("-inf"),
        ),
        reverse=True,
    )

    async def leaders_for_industry(name: str) -> list[str]:
        ind_token = ind_filter_map.get(_normalize_group_name(name))
        if not ind_token:
            return []
        screener_path = f"/screener.ashx?v=111&f={ind_token}&o=-volume"
        tickers = await fetch_finviz_tickers_deterministic(screener_path, max_pages=2)
        tickers = [t for t in tickers if t][:30]
        if not tickers:
            return []
        snaps = await asyncio.gather(*(asyncio.to_thread(_build_stock_snapshot, t) for t in tickers))
        stocks = [s for s in snaps if s is not None]
        rs_floor = _return_pct_floor([s.month_return_pct for s in stocks], LEADER_MIN_RS_PCT)
        passed = [s for s in stocks if _passes_leader_rubric(s, rs_floor=rs_floor)]
        pool = passed if passed else stocks
        pool.sort(key=lambda x: x.adr_pct, reverse=True)
        return [s.ticker for s in pool[:5]]

    enrich_n = min(25, len(themes))
    leaders_lists = await asyncio.gather(*(leaders_for_industry(themes[i]["theme"]) for i in range(enrich_n)))
    for i in range(enrich_n):
        themes[i]["leaders"] = leaders_lists[i]

    logger.info("Finviz industry leaderboard: %d rows", len(themes))
    return themes


def _parse_finviz_map_perf_nodes(payload: Any) -> dict[str, float]:
    nodes = payload.get("nodes") if isinstance(payload, dict) else None
    out: dict[str, float] = {}
    if not isinstance(nodes, dict):
        return out
    for slug, val in nodes.items():
        try:
            out[str(slug)] = float(val)
        except (TypeError, ValueError):
            continue
    return out


async def _fetch_finviz_theme_map_nodes(client: httpx.AsyncClient, st: str) -> dict[str, float]:
    st_key = (st or "d1").strip().lower()
    if st_key not in FINVIZ_MAP_PERF_ST:
        logger.warning("Unknown map_perf st=%r — skipping.", st_key)
        return {}
    url = f"{FINVIZ_MAP_PERF_THEMES_BASE}&st={st_key}"
    response = await client.get(url)
    response.raise_for_status()
    return _parse_finviz_map_perf_nodes(response.json())


async def build_finviz_themes_map_rows() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=25.0, headers=BROWSER_HEADERS, follow_redirects=True) as client:
        d1, w1, w4, w13, w26 = await asyncio.gather(
            _fetch_finviz_theme_map_nodes(client, "d1"),
            _fetch_finviz_theme_map_nodes(client, "w1"),
            _fetch_finviz_theme_map_nodes(client, "w4"),
            _fetch_finviz_theme_map_nodes(client, "w13"),
            _fetch_finviz_theme_map_nodes(client, "w26"),
        )
    all_slugs = set(d1) | set(w1) | set(w4) | set(w13) | set(w26)
    if not all_slugs:
        logger.warning("map_perf themes: no nodes across horizons.")
        return []

    def pick(m: dict[str, float], key: str) -> float | None:
        v = m.get(key)
        return None if v is None else float(v)

    themes: list[dict[str, Any]] = []
    for slug in sorted(all_slugs):
        d1_v = pick(d1, slug)
        w4_v = pick(w4, slug)
        if d1_v is None and w4_v is None:
            continue
        label = _humanize_finviz_theme_slug(str(slug))
        w1_v, w13_v, w26_v = pick(w1, slug), pick(w13, slug), pick(w26, slug)
        perf1d_v = round(d1_v, 2) if d1_v is not None else None
        rs1m = w4_v if w4_v is not None else d1_v
        themes.append(
            _series_theme_dict(
                theme=label,
                sector="Finviz Theme",
                rs1m=rs1m,
                perf1d=perf1d_v,
                perf1w=w1_v,
                perf1m=w4_v,
                perf3m=w13_v,
                perf6m=w26_v,
                total_count=0,
                theme_dollar_volume=0.0,
            )
        )
    themes.sort(
        key=lambda x: (
            x["relativeStrength1M"] is not None,
            x["relativeStrength1M"] if x["relativeStrength1M"] is not None else float("-inf"),
        ),
        reverse=True,
    )
    logger.info("Finviz themes map leaderboard: %d rows", len(themes))
    return themes


async def _assemble_finviz_style_payload(
    themes: list[dict[str, Any]], leaderboard_meta: dict[str, Any]
) -> dict[str, Any]:
    vix = await fetch_vix_snapshot()
    aggregate_current = sum(float(theme.get("themeDollarVolume", 0)) for theme in themes)
    market_flow_summary = {
        "aggregateDollarVolume": round(aggregate_current, 2),
        "previousAggregateDollarVolume": round(aggregate_current, 2),
        "aggregateAvg20DollarVolume": round(aggregate_current, 2),
        "flowTrend": "up",
        "conviction": "low",
    }
    return {
        "vix": vix,
        "themes": themes,
        "marketFlowSummary": market_flow_summary,
        "leaderboardMeta": leaderboard_meta,
    }


def _compute_adr_pct(highs: list[float], lows: list[float], closes: list[float]) -> float:
    ranges = []
    for high, low, close in zip(highs, lows, closes):
        if close:
            ranges.append(((high - low) / close) * 100)
    return mean(ranges) if ranges else 0.0


def _compute_or_rvol_ratio(ticker: str) -> float | None:
    """
    Opening-range RVOL: today's first 30m volume (6×5m bars) ÷ median of prior sessions' same window.
    """
    try:
        t = yf.Ticker(ticker)
        df = t.history(period="10d", interval="5m", prepost=False)
    except Exception:
        return None
    if df is None or df.empty or "Volume" not in df.columns:
        return None
    try:
        idx = df.index
        if idx.tz is None:
            idx = pd.DatetimeIndex(idx).tz_localize("UTC").tz_convert("America/New_York")
        else:
            idx = idx.tz_convert("America/New_York")
        df = df.copy()
        df.index = idx
    except Exception:
        return None

    vol_by_day: list[tuple[Any, float]] = []
    for _day, g in df.groupby(df.index.date):
        g = g.sort_index()
        if g.empty:
            continue
        first30 = float(g["Volume"].iloc[:6].sum())
        if first30 <= 0:
            continue
        vol_by_day.append((_day, first30))
    if len(vol_by_day) < 2:
        return None
    vol_by_day.sort(key=lambda x: x[0])
    today_vol = vol_by_day[-1][1]
    prior = [v for _, v in vol_by_day[:-1]]
    if not prior:
        return None
    baseline = float(median(prior))
    if baseline <= 0:
        return None
    return today_vol / baseline


def _top_decile_min_return(returns: list[float]) -> float:
    """Minimum 1M return among the top ~10% of the audited universe (proxy for market top decile)."""
    if not returns:
        return float("inf")
    n = len(returns)
    k = max(1, math.ceil(0.1 * n))
    return sorted(returns)[-k]


def _finalize_ep_candidates(stocks: list[ThemeStock]) -> None:
    returns = [s.month_return_pct for s in stocks]
    rs_floor = _top_decile_min_return(returns)
    for s in stocks:
        gap_ok = s.gap_open_pct > EP_MIN_GAP_UP_PCT
        or_ok = s.or_rvol_ratio is not None and s.or_rvol_ratio >= EP_MIN_OR_RVOL_RATIO
        rs_ok = s.month_return_pct >= rs_floor
        s.ep_candidate = bool(gap_ok and or_ok and rs_ok)


def _build_stock_snapshot(ticker: str) -> ThemeStock | None:
    logger.info("Auditing %s...", ticker)
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1y", interval="1d")
    if hist.empty or len(hist) < 210:
        logger.info("Skipping %s: insufficient history.", ticker)
        return None

    close_series = hist["Close"]
    volume_series = hist["Volume"]
    high_series = hist["High"]
    low_series = hist["Low"]

    close = float(close_series.iloc[-1])
    open_price = float(hist["Open"].iloc[-1])
    ema10 = float(close_series.ewm(span=10, adjust=False).mean().iloc[-1])
    ema20 = float(close_series.ewm(span=20, adjust=False).mean().iloc[-1])
    ema50 = float(close_series.ewm(span=50, adjust=False).mean().iloc[-1])
    ema200 = float(close_series.ewm(span=200, adjust=False).mean().iloc[-1])
    current_volume = float(volume_series.iloc[-1])
    prev_close = float(close_series.iloc[-2]) if len(close_series) > 1 else close
    prev_volume = float(volume_series.iloc[-2]) if len(volume_series) > 1 else current_volume
    avg_volume_3m = float(volume_series.tail(63).mean())
    volume_buzz_pct = ((current_volume / avg_volume_3m) - 1) * 100 if avg_volume_3m > 0 else 0.0
    gap_open_pct = ((open_price - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0
    today_return_pct = ((close - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0
    month_return_pct = 0.0
    if len(close_series) >= 22:
        month_return_pct = (float(close_series.iloc[-1]) / float(close_series.iloc[-22]) - 1.0) * 100.0
    or_rvol_ratio: float | None = None
    if gap_open_pct > EP_GAP_GATE_FOR_OR_FETCH:
        or_rvol_ratio = _compute_or_rvol_ratio(ticker)

    avg_dollar_volume = float((close_series * volume_series).tail(20).mean())
    adr_pct = _compute_adr_pct(
        highs=high_series.tail(20).tolist(),
        lows=low_series.tail(20).tolist(),
        closes=close_series.tail(20).tolist(),
    )

    fast_info = stock.fast_info if hasattr(stock, "fast_info") else {}
    info = stock.info if hasattr(stock, "info") else {}
    name = str(
        _safe_fast_info_value(fast_info, "shortName")
        or info.get("shortName")
        or info.get("longName")
        or ticker
    )

    market_cap_raw = _safe_fast_info_value(fast_info, "marketCap") or info.get("marketCap")
    if isinstance(market_cap_raw, str):
        market_cap = _parse_compact_number(market_cap_raw)
    else:
        market_cap = float(market_cap_raw or 0)

    industry = str(info.get("industry") or "Unknown")
    sector = str(info.get("sector") or "Unknown")

    snapshot = ThemeStock(
        ticker=ticker,
        name=name,
        sector=sector,
        industry=industry,
        market_cap=market_cap,
        avg_dollar_volume=avg_dollar_volume,
        adr_pct=adr_pct,
        close=close,
        ema10=ema10,
        ema20=ema20,
        ema50=ema50,
        ema200=ema200,
        current_volume=current_volume,
        avg_volume_3m=avg_volume_3m,
        volume_buzz_pct=volume_buzz_pct,
        prev_close=prev_close,
        prev_volume=prev_volume,
        open_price=open_price,
        gap_open_pct=gap_open_pct,
        or_rvol_ratio=or_rvol_ratio,
        today_return_pct=today_return_pct,
        month_return_pct=month_return_pct,
        ep_candidate=False,
    )
    ema_stack_ok = snapshot.close > snapshot.ema10 > snapshot.ema20 > snapshot.ema50 > snapshot.ema200
    ema_10_20_ok = snapshot.close > snapshot.ema10 > snapshot.ema20
    logger.info(
        "Audit %s | ADDV=$%.2fM | EMA stack=%s | EMA10/20=%s | VolBuzz=%.2f%% | ADR=%.2f%% | MCAP=$%.2fB | A+=%s | A=%s",
        snapshot.ticker,
        snapshot.avg_dollar_volume / 1_000_000,
        "PASS" if ema_stack_ok else "FAIL",
        "PASS" if ema_10_20_ok else "FAIL",
        snapshot.volume_buzz_pct,
        snapshot.adr_pct,
        snapshot.market_cap / 1_000_000_000,
        "PASS" if snapshot.qualifies_a_plus else "FAIL",
        "PASS" if snapshot.qualifies_grade_a else "FAIL",
    )
    return snapshot


async def fetch_vix_snapshot() -> dict[str, Any]:
    """Fetch current VIX snapshot from TradingView."""
    data = await fetch_tradingview_symbol_snapshot("CBOE:VIX")
    return {
        "symbol": "CBOE:VIX",
        "close": data.get("close"),
        "change_pct": data.get("change"),
        "change_abs": data.get("change_abs"),
        "description": data.get("description", "CBOE Volatility Index"),
    }


async def fetch_tradingview_symbol_snapshot(symbol: str) -> dict[str, Any]:
    sym = (symbol or "").strip()
    if not sym:
        return {}
    url = f"{TRADINGVIEW_SYMBOL_URL_BASE}?symbol={httpx.QueryParams({'symbol': sym})['symbol']}&fields={httpx.QueryParams({'fields': TRADINGVIEW_SYMBOL_FIELDS})['fields']}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        if response.status_code == 404:
            # TradingView uses 404 for unknown symbols.
            return {}
        response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _tv_aliases(sym: str) -> list[str]:
    """
    Best-effort TradingView symbol aliases.
    Some series are present under different namespaces depending on data vendor.
    """
    s = (sym or "").strip()
    if not s:
        return []
    # User-requested series sometimes live under alternate namespaces.
    # Keep this list short to reduce request volume.
    aliases: dict[str, list[str]] = {
        "ICE:BAMLH0A0HYM2": ["TVC:BAMLH0A0HYM2", "FRED:BAMLH0A0HYM2"],
        "INDEX:S5FI": ["TVC:S5FI"],
        "INDEX:MMTH": ["TVC:MMTH"],
    }
    return aliases.get(s, [])


async def fetch_tradingview_symbol_snapshot_any(symbol: str) -> tuple[str, dict[str, Any]]:
    """
    Try the provided symbol plus a few aliases; returns (resolved_symbol, payload_dict).
    """
    primary = (symbol or "").strip()
    candidates = [primary, *_tv_aliases(primary)]
    seen: set[str] = set()
    for sym in candidates:
        if not sym or sym in seen:
            continue
        seen.add(sym)
        data = await fetch_tradingview_symbol_snapshot(sym)
        if data:
            return sym, data
    return primary, {}


async def _fetch_fred_csv_last_two(series_id: str) -> tuple[float | None, float | None]:
    """
    Fetch last two observations from FRED public CSV (no API key).
    Returns (latest, previous).
    """
    sid = (series_id or "").strip().upper()
    if not sid:
        return None, None
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"
    def _download_sync() -> str | None:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*;q=0.8"}
        for attempt in range(4):
            try:
                r = httpx.get(url, timeout=25.0, follow_redirects=True, headers=headers)
                if r.status_code != 200:
                    return None
                return r.text
            except (httpx.ReadError, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout):
                import time

                time.sleep(0.7 * (attempt + 1))
                continue
            except Exception:
                return None
        return None

    text = await asyncio.to_thread(_download_sync)
    if not text:
        return None, None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 3:
        return None, None
    # Keep last two data rows (skip header).
    rows = lines[-2:]
    vals: list[float] = []
    for row in rows:
        parts = row.split(",", 1)
        if len(parts) != 2:
            continue
        raw = parts[1].strip()
        if raw in (".", ""):
            continue
        try:
            vals.append(float(raw))
        except ValueError:
            continue
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], None
    return vals[-1], vals[-2]


def _snapshot_close_is_usable(v: Any) -> bool:
    """TradingView sometimes returns a dict with null/invalid close; treat as missing for fallbacks."""
    if v is None:
        return False
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def _yf_dax_snapshot_sync() -> dict[str, Any]:
    """
    German DAX cash index via Yahoo Finance (^GDAXI).
    Shape matches TradingView symbol snapshot: close, change (%), description.
    """
    ticker = "^GDAXI"
    desc = "DAX Performance Index (yfinance ^GDAXI)"

    def _from_hist(tk: Any, period: str) -> dict[str, Any] | None:
        hist = tk.history(period=period, interval="1d", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        close_s = hist["Close"].dropna()
        if len(close_s) < 1:
            return None
        last = float(close_s.iloc[-1])
        prev = float(close_s.iloc[-2]) if len(close_s) >= 2 else last
        chg_pct = ((last - prev) / prev) * 100.0 if prev else 0.0
        return {"close": last, "change": chg_pct, "description": desc}

    try:
        tk = yf.Ticker(ticker)
        for period in ("5d", "1mo", "3mo"):
            out = _from_hist(tk, period)
            if out is not None:
                return out
        fi = getattr(tk, "fast_info", None) or {}
        last = fi.get("last_price") or fi.get("regular_market_price")
        prev = fi.get("previous_close")
        if last is None:
            return {}
        last_f = float(last)
        prev_f = float(prev) if prev is not None else last_f
        chg_pct = ((last_f - prev_f) / prev_f) * 100.0 if prev_f else 0.0
        return {"close": last_f, "change": chg_pct, "description": desc}
    except Exception:
        return {}


async def fetch_series_snapshot(symbol: str) -> tuple[str, dict[str, Any]]:
    """
    Unified snapshot fetch:
    - Try TradingView scanner symbol endpoint
    - For TVC:DAX, fallback to yfinance ^GDAXI when TV has no usable close
    - Fallback to FRED CSV for known economic series
    Returns (resolved_symbol, payload)
    """
    resolved, data = await fetch_tradingview_symbol_snapshot_any(symbol)
    sym_u = (symbol or "").strip().upper()
    tv_close_ok = isinstance(data, dict) and _snapshot_close_is_usable(data.get("close"))
    if sym_u == "TVC:DAX" and not tv_close_ok:
        yf_data = await asyncio.to_thread(_yf_dax_snapshot_sync)
        if isinstance(yf_data, dict) and _snapshot_close_is_usable(yf_data.get("close")):
            logger.info("DAX: using yfinance ^GDAXI (TradingView close missing or invalid)")
            return "YFINANCE:^GDAXI", yf_data
        return resolved, {}
    if data:
        return resolved, data
    # FRED fallback for HY credit spread series (TradingView may not expose via scanner endpoint).
    if (symbol or "").strip().upper() in {"ICE:BAMLH0A0HYM2", "FRED:BAMLH0A0HYM2", "BAMLH0A0HYM2"}:
        latest, prev = await _fetch_fred_csv_last_two("BAMLH0A0HYM2")
        if latest is None:
            return resolved, {}
        chg = None
        if prev is not None and prev != 0:
            chg = ((latest - prev) / prev) * 100.0
        return "FRED:BAMLH0A0HYM2", {"close": latest, "change": chg, "description": "ICE BofA US High Yield OAS (FRED)"}
    return resolved, {}


async def fetch_tradingview_tape() -> list[dict[str, Any]]:
    """
    Header tape quotes from TradingView.
    Order and symbols reflect the dashboard's top ribbon.
    """
    symbols = [
        ("NQ1!", "CME_MINI:NQ1!"),
        ("ES1!", "CME_MINI:ES1!"),
        ("RTY", "CME_MINI:RTY1!"),
        ("IWM", "AMEX:IWM"),
        ("BTC", "CME:BTC1!"),
        ("GC1!", "COMEX:GC1!"),
        ("OIL", "NYMEX:CL1!"),
        ("VIX", "CBOE:VIX"),
    ]
    snaps = await asyncio.gather(*(fetch_series_snapshot(sym) for _label, sym in symbols), return_exceptions=True)
    out: list[dict[str, Any]] = []
    for (label, sym), resolved in zip(symbols, snaps, strict=False):
        if isinstance(resolved, Exception):
            resolved_sym, data = sym, {}
        else:
            resolved_sym, data = resolved
        out.append(
            {
                "label": label,
                "symbol": sym,
                "resolved_symbol": resolved_sym,
                "close": data.get("close"),
                "change_pct": data.get("change"),
            }
        )
    return out


async def _build_leaderboard_screener_audit(
    screener_path: str = "/screener.ashx?v=111&f=ind_artificialintelligence,sec_technology&o=-perf4w",
) -> dict[str, Any]:
    """
    Legacy path: Finviz screener tickers + yfinance audits, grouped by yfinance industry.
    """
    (top_sector_name, rotation_screener_path), groups_rs = await asyncio.gather(
        fetch_top_sector_rotation(),
        fetch_finviz_groups_performance(),
    )

    effective_screener_path = rotation_screener_path or screener_path
    tickers = await fetch_finviz_tickers(screener_path=effective_screener_path)
    all_candidates = sorted(set([*tickers, *MANUAL_OVERRIDE_TICKERS]))
    sample_size = min(MAX_TICKER_SNAPSHOTS, len(all_candidates))
    sampled = random.sample(all_candidates, sample_size) if sample_size > 0 else []
    forced = [t for t in MANUAL_OVERRIDE_TICKERS if t in all_candidates]
    candidate_tickers = sorted(set([*sampled, *forced]))
    logger.info(
        "Rotating to top sector: %s | Auditing %d leaders.",
        top_sector_name or "Unknown",
        len(candidate_tickers),
    )
    logger.info("Manual override tickers included: %s", ", ".join(MANUAL_OVERRIDE_TICKERS))

    snapshots = await asyncio.gather(
        *(asyncio.to_thread(_build_stock_snapshot, ticker) for ticker in candidate_tickers)
    )
    stocks = [s for s in snapshots if s is not None]
    logger.info("Completed ticker audits. Valid snapshots: %d", len(stocks))
    _finalize_ep_candidates(stocks)
    ep_tickers = [s.ticker for s in stocks if s.ep_candidate]
    if ep_tickers:
        logger.info("EP candidates (gap + OR-RVOL + RS decile): %s", ", ".join(ep_tickers))

    groups: dict[str, list[ThemeStock]] = {}
    for stock in stocks:
        groups.setdefault(stock.industry, []).append(stock)

    themes = []
    for industry, members in groups.items():
        if not members:
            continue
        logger.info("Scanning [%s]...", industry)
        ranked_members = sorted(members, key=lambda x: x.adr_pct, reverse=True)
        rs_floor = _return_pct_floor([m.month_return_pct for m in ranked_members], LEADER_MIN_RS_PCT)
        leader_candidates = [m for m in ranked_members if _passes_leader_rubric(m, rs_floor=rs_floor)]
        leaders_for_display = leader_candidates if leader_candidates else ranked_members
        qualifying_a_plus = [m for m in ranked_members if m.qualifies_a_plus]
        qualifying_grade_a = [m for m in ranked_members if m.qualifies_grade_a]
        qualifying_for_display = qualifying_a_plus if qualifying_a_plus else qualifying_grade_a
        qualifier_ratio = round((len(qualifying_for_display) / len(members)) * 100, 2)
        theme_current_dollar_volume = sum(m.close * m.current_volume for m in ranked_members)
        theme_prev_dollar_volume = sum(m.prev_close * m.prev_volume for m in ranked_members)
        theme_avg20_dollar_volume = sum(m.avg_dollar_volume for m in ranked_members)
        high_liquidity = theme_current_dollar_volume > 500_000_000
        accumulation = theme_current_dollar_volume > (theme_avg20_dollar_volume * 1.2)
        sector = ranked_members[0].sector if ranked_members else "Unknown"
        finviz_group = groups_rs.get(_normalize_group_name(sector))
        rs_score_1m = finviz_group.perf_month_pct if finviz_group else None
        perf_1d = finviz_group.perf_day_pct if finviz_group else None
        perf_1w = finviz_group.perf_week_pct if finviz_group else None
        perf_3m = finviz_group.perf_quarter_pct if finviz_group else None
        perf_6m = finviz_group.perf_half_pct if finviz_group else None
        logger.info(
            "Theme [%s] sector=%s | 1M RS=%s | A+ %d | A %d | Display %d/%d (%.2f%%)",
            industry,
            sector,
            f"{rs_score_1m:.2f}%" if rs_score_1m is not None else "N/A",
            len(qualifying_a_plus),
            len(qualifying_grade_a),
            len(qualifying_for_display),
            len(members),
            qualifier_ratio,
        )
        themes.append(
            {
                "theme": industry,
                "sector": sector,
                "relativeStrength1M": rs_score_1m,
                "perf1D": None if perf_1d is None else round(perf_1d, 2),
                "perf1W": None if perf_1w is None else round(perf_1w, 2),
                "perf1M": None if rs_score_1m is None else round(rs_score_1m, 2),
                "perf3M": None if perf_3m is None else round(perf_3m, 2),
                "perf6M": None if perf_6m is None else round(perf_6m, 2),
                "relativeStrengthQualifierRatio": qualifier_ratio,
                "leaders": [m.ticker for m in leaders_for_display[:5]],
                "qualifiedCount": len(qualifying_for_display),
                "aPlusCount": len(qualifying_a_plus),
                "gradeACount": len(qualifying_grade_a),
                "totalCount": len(members),
                "themeDollarVolume": round(theme_current_dollar_volume, 2),
                "themePrevDollarVolume": round(theme_prev_dollar_volume, 2),
                "themeAvg20DollarVolume": round(theme_avg20_dollar_volume, 2),
                "highLiquidity": high_liquidity,
                "accumulation": accumulation,
                "stocks": [
                    {
                        "ticker": m.ticker,
                        "name": m.name,
                        "adr_pct": round(m.adr_pct, 2),
                        "avg_dollar_volume": round(m.avg_dollar_volume, 2),
                        "market_cap": round(m.market_cap, 2),
                        "close": round(m.close, 2),
                        "ema10": round(m.ema10, 2),
                        "ema20": round(m.ema20, 2),
                        "ema50": round(m.ema50, 2),
                        "ema200": round(m.ema200, 2),
                        "current_volume": round(m.current_volume, 2),
                        "avg_volume_3m": round(m.avg_volume_3m, 2),
                        "volume_buzz_pct": round(m.volume_buzz_pct, 2),
                        "qualifies_a_plus": m.qualifies_a_plus,
                        "qualifies_grade_a": m.qualifies_grade_a,
                        "gradeLabel": m.grade_label,
                        "gap_open_pct": round(m.gap_open_pct, 2),
                        "or_rvol_ratio": None if m.or_rvol_ratio is None else round(m.or_rvol_ratio, 2),
                        "today_return_pct": round(m.today_return_pct, 2),
                        "month_return_pct": round(m.month_return_pct, 2),
                        "ep_candidate": m.ep_candidate,
                    }
                    for m in ranked_members[:8]
                ],
            }
        )

    themes.sort(
        key=lambda x: (
            x["relativeStrength1M"] is not None,
            x["relativeStrength1M"] if x["relativeStrength1M"] is not None else float("-inf"),
            x["relativeStrengthQualifierRatio"],
        ),
        reverse=True,
    )
    if len(themes) < MIN_THEME_COUNT:
        # Guarantee minimum payload depth so the frontend always has objects to render.
        all_ranked = sorted(stocks, key=lambda x: x.adr_pct, reverse=True)
        idx = 1
        while len(themes) < MIN_THEME_COUNT and all_ranked:
            chunk = all_ranked[: min(5, len(all_ranked))]
            if not chunk:
                break
            rs_floor = _return_pct_floor([s.month_return_pct for s in chunk], LEADER_MIN_RS_PCT)
            chunk_leaders = [s for s in chunk if _passes_leader_rubric(s, rs_floor=rs_floor)]
            themes.append(
                {
                    "theme": f"Rotation Watchlist {idx}",
                    "sector": chunk[0].sector if chunk else "Unknown",
                    "relativeStrength1M": None,
                    "perf1D": None,
                    "perf1W": None,
                    "perf1M": None,
                    "perf3M": None,
                    "perf6M": None,
                    "relativeStrengthQualifierRatio": round(
                        (len([s for s in chunk if s.qualifies_grade_a]) / len(chunk)) * 100, 2
                    ),
                    "leaders": [s.ticker for s in (chunk_leaders if chunk_leaders else chunk)],
                    "qualifiedCount": len([s for s in chunk if s.qualifies_grade_a]),
                    "aPlusCount": len([s for s in chunk if s.qualifies_a_plus]),
                    "gradeACount": len([s for s in chunk if s.qualifies_grade_a]),
                    "totalCount": len(chunk),
                    "themeDollarVolume": round(sum(s.close * s.current_volume for s in chunk), 2),
                    "themePrevDollarVolume": round(sum(s.prev_close * s.prev_volume for s in chunk), 2),
                    "themeAvg20DollarVolume": round(sum(s.avg_dollar_volume for s in chunk), 2),
                    "highLiquidity": sum(s.close * s.current_volume for s in chunk) > 500_000_000,
                    "accumulation": sum(s.close * s.current_volume for s in chunk) > (sum(s.avg_dollar_volume for s in chunk) * 1.2),
                    "stocks": [
                        {
                            "ticker": s.ticker,
                            "name": s.name,
                            "adr_pct": round(s.adr_pct, 2),
                            "avg_dollar_volume": round(s.avg_dollar_volume, 2),
                            "market_cap": round(s.market_cap, 2),
                            "close": round(s.close, 2),
                            "ema10": round(s.ema10, 2),
                            "ema20": round(s.ema20, 2),
                            "ema50": round(s.ema50, 2),
                            "ema200": round(s.ema200, 2),
                            "current_volume": round(s.current_volume, 2),
                            "avg_volume_3m": round(s.avg_volume_3m, 2),
                            "volume_buzz_pct": round(s.volume_buzz_pct, 2),
                            "qualifies_a_plus": s.qualifies_a_plus,
                            "qualifies_grade_a": s.qualifies_grade_a,
                            "gradeLabel": s.grade_label,
                            "gap_open_pct": round(s.gap_open_pct, 2),
                            "or_rvol_ratio": None if s.or_rvol_ratio is None else round(s.or_rvol_ratio, 2),
                            "today_return_pct": round(s.today_return_pct, 2),
                            "month_return_pct": round(s.month_return_pct, 2),
                            "ep_candidate": s.ep_candidate,
                        }
                        for s in chunk
                    ],
                }
            )
            all_ranked = all_ranked[len(chunk) :]
            idx += 1
    vix = await fetch_vix_snapshot()
    # Aggregate flow summary for Market Liquidity & Flow box.
    aggregate_current = sum(float(theme.get("themeDollarVolume", 0)) for theme in themes)
    aggregate_prev = sum(float(theme.get("themePrevDollarVolume", 0)) for theme in themes)
    aggregate_avg20 = sum(float(theme.get("themeAvg20DollarVolume", 0)) for theme in themes)
    flow_trend = "up" if aggregate_current > aggregate_prev else "down"
    conviction = "high" if aggregate_current > aggregate_avg20 else "low"
    market_flow_summary = {
        "aggregateDollarVolume": round(aggregate_current, 2),
        "previousAggregateDollarVolume": round(aggregate_prev, 2),
        "aggregateAvg20DollarVolume": round(aggregate_avg20, 2),
        "flowTrend": flow_trend,
        "conviction": conviction,
    }
    logger.info("Leaderboard (screener audit) ready. Returning %d themes.", len(themes))
    return {
        "vix": vix,
        "themes": themes,
        "marketFlowSummary": market_flow_summary,
        "leaderboardMeta": {
            "view": "scanner",
            "source": "finviz_screener_yfinance",
        },
    }


async def build_theme_leaderboard(
    leader_view: str = "themes",
    screener_path: str = "/screener.ashx?v=111&f=ind_artificialintelligence,sec_technology&o=-perf4w",
) -> dict[str, Any]:
    """
    Leaderboard payload by UI mode:
    - themes: Finviz map_perf.ashx?t=themes with st=d1,w1,w4,w13,w26 (1D…6M-style %).
    - industry: Finviz industry groups v=110 + v=140 tables.
    """
    mode = (leader_view or "themes").strip().lower()
    if mode == "industry":
        rows = await build_finviz_industry_leaderboard_rows()
        return await _assemble_finviz_style_payload(
            rows,
            {
                "view": "industry",
                "source": "finviz_groups_industry",
                "urls": {
                    "overview": FINVIZ_INDUSTRY_OVERVIEW_URL,
                    "performance": FINVIZ_INDUSTRY_PERF_URL,
                },
            },
        )
    if mode == "themes":
        rows = await build_finviz_themes_map_rows()
        return await _assemble_finviz_style_payload(
            rows,
            {
                "view": "themes",
                "source": "finviz_map_perf",
                "url": f"{FINVIZ_BASE_URL}/map.ashx?t=themes",
                "apiUrl": FINVIZ_MAP_PERF_THEMES_BASE,
                "perfNote": (
                    "Finviz map_perf horizons: 1D=st:d1, 1W=w1, 1M≈w4, 3M≈w13, 6M≈w26. "
                    "1M RS uses the ~1M (w4) reading when present."
                ),
            },
        )
    if mode in ("scanner", "legacy", "audit"):
        return await _build_leaderboard_screener_audit(screener_path)
    logger.warning("Unknown leader_view %r — using screener audit.", leader_view)
    return await _build_leaderboard_screener_audit(screener_path)

