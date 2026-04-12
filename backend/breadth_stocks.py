"""
breadth_stocks.py — Drill-down stock lists for Stockbee Market Monitor breadth filters.

Maps each Stockbee breadth filter key to a Finviz screener URL, paginates the results,
filters by market cap, enriches with 14-day ADR% from yfinance, and returns a sorted list.

Cache: keyed by (filter_key, date_et) with a 30-min TTL before 4 PM ET
       and a 4-hour TTL post-close to avoid redundant Finviz + yfinance calls.

Industry is read from the Finviz v=111 overview table (column 4) — it is already available
there and accurate. yfinance fast_info does not expose an industry field, so we do not
make per-ticker yfinance info calls.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from time import monotonic
from typing import Any

import httpx
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# INDUSTRY_THEME_MAP — maps Finviz industry name → thematic label
# ---------------------------------------------------------------------------
try:
    from backend.scraper import INDUSTRY_THEME_MAP
except ImportError:
    from scraper import INDUSTRY_THEME_MAP  # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# THEMATIC_TO_PARENT — maps thematic label → broad leaderboard parent sector
#
# Parent categories align with Finviz/GICS sector vocabulary so that
# leaderboard_parent can be used as a `parent=` value on the industry
# sub-industry movers endpoint.
# ---------------------------------------------------------------------------
THEMATIC_TO_PARENT: dict[str, str] = {
    # Technology
    "AI & Semiconductors":            "Technology",
    "AI & SaaS":                      "Technology",
    "Cloud & Cyber":                  "Technology",
    # Healthcare
    "Biotech":                        "Healthcare",
    "Medtech":                        "Healthcare",
    "Healthcare Services":            "Healthcare",
    # Energy
    "Energy Transition":              "Energy",
    "Energy — Oil & Gas":             "Energy",
    # Materials
    "Energy — Commodities":           "Materials",
    "Industrials — Chemicals":        "Materials",
    "Industrials — Materials":        "Materials",
    "Agriculture & AgriTech":         "Materials",
    # Industrials
    "Defense & Aerospace":            "Industrials",
    "Industrials":                    "Industrials",
    "Industrials — Logistics":        "Industrials",
    "Industrials — Precision":        "Industrials",
    # Financials
    "Financials — Asset Management":  "Financials",
    "Financials — FinTech":           "Financials",
    "Financials — Insurance":         "Financials",
    # Real Estate
    "Financials — Real Estate":       "Real Estate",
    "Real Estate":                    "Real Estate",
    # Consumer Discretionary
    "Consumer — Retail":              "Consumer Discretionary",
    "Consumer — Lifestyle & Retail":  "Consumer Discretionary",
    "Consumer — Autos":               "Consumer Discretionary",
    "Consumer — Travel":              "Consumer Discretionary",
    "Consumer — Services":            "Consumer Discretionary",
    "Consumer — E-Commerce":          "Consumer Discretionary",
    # Consumer Staples
    "Consumer — Staples":             "Consumer Staples",
    # Communication Services
    "Consumer — Marketing & Media":   "Communication Services",
    "Digital Entertainment":          "Communication Services",
    "Media & Publishing":             "Communication Services",
    "Media & Telecom":                "Communication Services",
    # Education (standalone parent)
    "Education & Services":           "Education & Services",
    # Utilities
    "Utilities":                      "Utilities",
    # Anything not listed above falls to "Other" via _get_leaderboard_parent
}

# ---------------------------------------------------------------------------
# Filter map: filter_key → (screener_path, sort_ascending)
# sort_ascending=True  → worst performers first (dn* filters)
# sort_ascending=False → best performers first (up* filters)
# ---------------------------------------------------------------------------
_BASE = "geo_usa,sh_avgvol_o100,sh_price_o5,cap_midover"

_FILTER_MAP: dict[str, tuple[str, bool]] = {
    # sort_ascending=False → best performers first (up* filters)
    # sort_ascending=True  → worst performers first (dn* filters)
    "up4":     (f"/screener.ashx?v=111&f={_BASE},ta_change_u4&o=-change",      False),
    "dn4":     (f"/screener.ashx?v=111&f={_BASE},ta_change_d-4&o=change",       True),
    "up25q":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_q25o&o=-perf13w",     False),
    "dn25q":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_q-25u&o=perf13w",      True),
    "up25m":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_m25o&o=-perf4w",      False),
    "dn25m":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_m-25u&o=perf4w",       True),
    "up50m":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_m50o&o=-perf4w",      False),
    "dn50m":   (f"/screener.ashx?v=111&f={_BASE},ta_perf_m-50u&o=perf4w",       True),
    "up13_34": (f"/screener.ashx?v=111&f={_BASE},ta_perf34d_13o&o=-perf",      False),
    "dn13_34": (f"/screener.ashx?v=111&f={_BASE},ta_perf34d_-13u&o=perf",       True),
}

VALID_FILTERS: frozenset[str] = frozenset(_FILTER_MAP)

# Post-fetch change_pct guard thresholds for up/dn filters.
# Stocks that slip below/above these thresholds after intraday moves are dropped.
# Keys match filter_key; None means no threshold enforcement.
_CHANGE_THRESHOLD: dict[str, tuple[str, float]] = {
    # (direction, threshold)  direction: "min" → change_pct >= threshold
    #                                    "max" → change_pct <= threshold
    "up4":     ("min",  3.0),   # must still be up ≥3 % (allows minor intraday drift)
    "dn4":     ("max", -3.0),   # must still be down ≤-3 %
}

_FINVIZ_BASE = "https://finviz.com"
_MAX_PAGES = 25          # up to 500 stocks
_YFINANCE_BATCH_CAP = 300

# Matches scraper.py BROWSER_HEADERS — kept in sync manually.
_BROWSER_HEADERS: dict[str, str] = {
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

_TTL_PRE_CLOSE  = 30 * 60        # 30 minutes during market hours
_TTL_POST_CLOSE = 4 * 60 * 60    # 4 hours after 4 PM ET

# Cache: (filter_key, date_et) → (monotonic_timestamp, stock_list)
_CACHE: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
_CACHE_LOCK = asyncio.Lock()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _get_leaderboard_parent(finviz_industry: str | None) -> str:
    """
    Resolve a Finviz industry string → leaderboard parent sector.

    Resolution order:
    1. Exact lookup in INDUSTRY_THEME_MAP → thematic label.
    2. Normalized whitespace fallback (handles minor spacing differences).
    3. Lookup thematic label in THEMATIC_TO_PARENT.
    4. Returns "Other" if any step misses.
    """
    if not finviz_industry:
        return "Other"
    thematic = INDUSTRY_THEME_MAP.get(finviz_industry, "")
    if not thematic:
        # Normalized fallback: collapse whitespace, compare case-insensitively
        norm = " ".join(finviz_industry.lower().strip().split())
        for k, v in INDUSTRY_THEME_MAP.items():
            if " ".join(k.lower().strip().split()) == norm:
                thematic = v
                break
    return THEMATIC_TO_PARENT.get(thematic, "Other")


def _today_et() -> str:
    """Current date in US/Eastern as YYYY-MM-DD."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


def _cache_ttl() -> float:
    """TTL in seconds: 30 min before 4 PM ET, 4 hours after."""
    try:
        from zoneinfo import ZoneInfo
        hour = datetime.now(ZoneInfo("America/New_York")).hour
    except Exception:
        hour = datetime.utcnow().hour  # rough fallback
    return _TTL_POST_CLOSE if hour >= 16 else _TTL_PRE_CLOSE


def _parse_market_cap_b(raw: str) -> float | None:
    """Parse '1.23B', '456.78M', '1.2T', '500K' → float in billions. Returns None on failure."""
    s = raw.strip().replace(",", "")
    if not s or s in ("-", ""):
        return None
    try:
        if s.endswith("T"):
            return float(s[:-1]) * 1_000.0
        if s.endswith("B"):
            return float(s[:-1])
        if s.endswith("M"):
            return float(s[:-1]) / 1_000.0
        if s.endswith("K"):
            return float(s[:-1]) / 1_000_000.0
        return float(s) / 1e9
    except ValueError:
        return None


def _parse_pct(raw: str) -> float | None:
    """Parse '+5.23%', '-2.14%', '25.00%' → float. Returns None on failure."""
    t = raw.strip().lstrip("+").replace("%", "")
    if not t or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_price(raw: str) -> float | None:
    t = raw.strip().replace(",", "")
    if not t or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_volume_shares(raw: str) -> int | None:
    """Parse comma-formatted share volume string → int."""
    t = raw.strip().replace(",", "")
    if not t or t == "-":
        return None
    try:
        return int(float(t))
    except ValueError:
        return None


def _fmt_dollar_vol(price: float, vol_shares: int) -> str:
    """Format price × volume as a human-readable dollar string."""
    dv = price * vol_shares
    if dv >= 1e9:
        return f"${dv / 1e9:.1f}B"
    if dv >= 1e6:
        return f"${dv / 1e6:.0f}M"
    if dv >= 1e3:
        return f"${dv / 1e3:.0f}K"
    return f"${dv:.0f}"


# ---------------------------------------------------------------------------
# Finviz HTML parsing
# ---------------------------------------------------------------------------

def _parse_screener_rows(html: str) -> list[dict[str, Any]]:
    """
    Parse a Finviz v=111 (overview) screener page.

    Expected column order (0-indexed <td> cells per data row):
      0: row #
      1: ticker link  → <a href="/quote.ashx?t=TICKER">
      2: company name
      3: sector
      4: industry
      5: country
      6: market cap   (e.g. "1.23B")
      7: P/E
      8: price
      9: change %     (e.g. "+5.23%")
     10: volume       (share volume, comma-formatted)

    IMPORTANT: use recursive=False when collecting <td> children of each <tr>.
    Finviz wraps the screener in nested tables; recursive=True would pull in
    inner-table cells and shift column indices, causing wrong company names,
    prices, and change values.

    Returns a list of raw row dicts. All string values are unvalidated — callers
    must apply type-parsing and filtering.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []

    for tr in soup.find_all("tr"):
        # Use recursive=False to get only direct <td> children.
        # Recursive search pulls nested-table cells and shifts column offsets.
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 11:
            continue

        # Cell 1 must contain a quote link to be a data row (not a header).
        ticker_link = tds[1].find("a", href=lambda h: h and "quote.ashx?t=" in h)
        if not ticker_link:
            continue

        href = ticker_link.get("href", "")
        sym = href.split("t=")[-1].split("&")[0].strip().upper()
        if not sym or not (1 <= len(sym) <= 8):
            continue

        rows.append({
            "ticker":          sym,
            "company":         tds[2].get_text(strip=True),
            "sector":          tds[3].get_text(strip=True),
            "industry":        tds[4].get_text(strip=True),
            "market_cap_raw":  tds[6].get_text(strip=True),
            "price_raw":       tds[8].get_text(strip=True),
            "change_raw":      tds[9].get_text(strip=True),
            "volume_raw":      tds[10].get_text(strip=True),
        })

    return rows


# ---------------------------------------------------------------------------
# Finviz HTTP fetch (with 429 backoff, matching scraper.py pattern)
# ---------------------------------------------------------------------------

async def _fetch_finviz_page(
    client: httpx.AsyncClient,
    path: str,
    *,
    retries: int = 3,
    backoff_s: float = 1.5,
) -> str:
    """Fetch one Finviz screener page; retry on 429 with linear backoff."""
    url = f"{_FINVIZ_BASE}{path}"
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            r = await client.get(url)
            if r.status_code == 429:
                wait = backoff_s * (attempt + 1)
                logger.warning("breadth_stocks: 429 on %s — sleeping %.1fs", url, wait)
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            return r.text
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                await asyncio.sleep(backoff_s * (attempt + 1))
    raise RuntimeError(f"Finviz fetch failed after {retries} attempts: {last_exc}")


# ---------------------------------------------------------------------------
# yfinance ADR% computation (sync — run via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _compute_adr_batch_sync(tickers: list[str]) -> dict[str, float | None]:
    """
    Compute 14-day ADR% for each ticker using yfinance batch download.
    ADR% = mean((High - Low) / Close * 100) over last 14 trading sessions.

    Caps input at _YFINANCE_BATCH_CAP to avoid Yahoo throttling.
    Returns a dict mapping ticker → adr_pct (None on error).

    yfinance ≥1.0 always returns a MultiIndex DataFrame with Level-0 = metric
    (Close/High/Low/…) and Level-1 = ticker, regardless of how many tickers are
    downloaded.  Access pattern: df["High"][tkr]  (metric first, then ticker).
    """
    import pandas as pd  # local import — pandas is always available

    result: dict[str, float | None] = {t: None for t in tickers}
    if not tickers:
        return result

    # Chunk into batches of _YFINANCE_BATCH_CAP
    chunks = [
        tickers[i: i + _YFINANCE_BATCH_CAP]
        for i in range(0, len(tickers), _YFINANCE_BATCH_CAP)
    ]

    for chunk in chunks:
        try:
            df = yf.download(
                tickers=chunk,
                period="30d",
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
        except Exception as exc:
            logger.warning("breadth_stocks: yfinance download failed for chunk: %s", exc)
            continue

        if df is None or getattr(df, "empty", True):
            continue

        # Detect column layout:
        #   MultiIndex (metric, ticker)  → yfinance ≥1.0 style; access df["High"][tkr]
        #   MultiIndex (ticker, metric)  → old group_by="ticker" style; access df[tkr]["High"]
        #   Flat Index                   → single-ticker, older yfinance; access df["High"]
        is_multi = isinstance(df.columns, pd.MultiIndex)
        if is_multi:
            l0 = set(df.columns.get_level_values(0))
            l1 = set(df.columns.get_level_values(1))
            # yfinance ≥1.0: L0 = metrics, L1 = tickers
            metric_first = "High" in l0
        else:
            metric_first = None  # flat columns

        for tkr in chunk:
            try:
                if not is_multi:
                    # Flat: single-ticker flat DataFrame (very old yfinance / edge case)
                    high  = df["High"].dropna()
                    low   = df["Low"].dropna()
                    close = df["Close"].dropna()
                elif metric_first:
                    # yfinance ≥1.0: (metric, ticker)
                    if tkr not in l1:
                        continue
                    high  = df["High"][tkr].dropna()
                    low   = df["Low"][tkr].dropna()
                    close = df["Close"][tkr].dropna()
                else:
                    # Legacy group_by="ticker": (ticker, metric)
                    if tkr not in l0:
                        continue
                    high  = df[tkr]["High"].dropna()
                    low   = df[tkr]["Low"].dropna()
                    close = df[tkr]["Close"].dropna()

                n = min(len(high), len(low), len(close), 14)
                if n < 5:
                    continue

                adr = float(
                    ((high.iloc[-n:] - low.iloc[-n:]) / close.iloc[-n:] * 100.0).mean()
                )
                result[tkr] = round(adr, 1)
            except Exception:
                pass  # Leave as None

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_breadth_stock_list(
    filter_key: str,
    min_cap_b: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetch, filter, and enrich the Finviz screener for the given breadth filter key.

    Steps:
    1. Paginate Finviz (up to 25 pages / 500 stocks).
    2. Parse each row for ticker, company, market cap, price, change, volume, industry.
    3. Filter rows where market_cap_b >= min_cap_b.
    4. Stop pagination early if a full page yields zero qualifying rows.
    5. Batch-fetch 14-day ADR% from yfinance for the qualifying set only.
    6. Sort by change_pct descending (up* filters) or ascending (dn* filters).
    7. Cache result by (filter_key, date_et) with TTL.

    Returns a list of enriched stock dicts.
    """
    if filter_key not in _FILTER_MAP:
        raise ValueError(f"Unknown filter key '{filter_key}'. Valid: {sorted(VALID_FILTERS)}")

    today = _today_et()
    cache_key = (filter_key, today)
    ttl = _cache_ttl()
    now = monotonic()

    async with _CACHE_LOCK:
        if cache_key in _CACHE:
            ts, cached = _CACHE[cache_key]
            if (now - ts) < ttl:
                logger.debug("breadth_stocks: cache hit for %s / %s", filter_key, today)
                return cached

        logger.info("breadth_stocks: fetching %s (min_cap_b=%.2f)", filter_key, min_cap_b)
        path, sort_asc = _FILTER_MAP[filter_key]

        # ── 1-4: Finviz pagination ──────────────────────────────────────────
        qualifying: list[dict[str, Any]] = []
        seen_tickers: set[str] = set()

        async with httpx.AsyncClient(
            timeout=25.0, headers=_BROWSER_HEADERS, follow_redirects=True
        ) as client:
            for page in range(_MAX_PAGES):
                offset = 1 + page * 20
                page_path = f"{path}&r={offset}"

                try:
                    html = await _fetch_finviz_page(client, page_path)
                except Exception as exc:
                    logger.warning("breadth_stocks: page %d fetch error: %s", page + 1, exc)
                    break

                raw_rows = _parse_screener_rows(html)
                if not raw_rows:
                    break  # Exhausted screener

                for row in raw_rows:
                    tkr = row["ticker"]
                    if tkr in seen_tickers:
                        continue
                    seen_tickers.add(tkr)

                    # cap_midover in the Finviz URL applies a broad server-side
                    # floor (~$300M).  We enforce the stricter min_cap_b here.
                    cap_b = _parse_market_cap_b(row["market_cap_raw"])
                    if cap_b is not None and cap_b < min_cap_b:
                        continue
                    price = _parse_price(row["price_raw"])
                    change = _parse_pct(row["change_raw"])
                    vol = _parse_volume_shares(row["volume_raw"])

                    dollar_vol = (
                        _fmt_dollar_vol(price, vol)
                        if price is not None and vol is not None
                        else "—"
                    )

                    industry = row["industry"] or None
                    qualifying.append({
                        "ticker":            tkr,
                        "company":           row["company"],
                        "market_cap_b":      round(cap_b, 3) if cap_b is not None else None,
                        "price":             price,
                        "change_pct":        change,
                        "dollar_volume":     dollar_vol,
                        "adr_pct":           None,   # filled in below
                        "industry":          industry,
                        "thematic_label":    INDUSTRY_THEME_MAP.get(industry, "") if industry else "",
                        "leaderboard_parent": _get_leaderboard_parent(industry),
                    })

                await asyncio.sleep(0.25)  # polite delay

                # Early stop: partial page means we've hit the last page
                if len(raw_rows) < 20:
                    break

        logger.info(
            "breadth_stocks: %d stocks fetched (filter=%s)",
            len(qualifying), filter_key,
        )

        # ── 5: ADR% enrichment ──────────────────────────────────────────────
        if qualifying:
            tickers_for_adr = [s["ticker"] for s in qualifying]
            adr_map = await asyncio.to_thread(_compute_adr_batch_sync, tickers_for_adr)
            for stock in qualifying:
                stock["adr_pct"] = adr_map.get(stock["ticker"])

        # ── 6: Sort ─────────────────────────────────────────────────────────
        qualifying.sort(
            key=lambda s: (s["change_pct"] is None, s["change_pct"] or 0.0),
            reverse=not sort_asc,
        )

        # ── 6b: Post-fetch threshold guard ───────────────────────────────────
        # Finviz applies its change filter at page-render time; intraday moves
        # can cause a stock to drift across the 4 % line by the time we display
        # it.  We also guard against any residual parsing artefacts.
        threshold_rule = _CHANGE_THRESHOLD.get(filter_key)
        if threshold_rule:
            direction, threshold = threshold_rule
            before = len(qualifying)
            if direction == "min":
                qualifying = [
                    s for s in qualifying
                    if s["change_pct"] is None or s["change_pct"] >= threshold
                ]
            else:  # "max"
                qualifying = [
                    s for s in qualifying
                    if s["change_pct"] is None or s["change_pct"] <= threshold
                ]
            dropped = before - len(qualifying)
            if dropped:
                logger.debug(
                    "breadth_stocks: threshold guard dropped %d stocks for %s "
                    "(change_pct %s %.1f%%)",
                    dropped, filter_key, direction, threshold,
                )

        # ── 7: Cache ─────────────────────────────────────────────────────────
        _CACHE[cache_key] = (monotonic(), qualifying)

        return qualifying


# ---------------------------------------------------------------------------
# Grouped view
# ---------------------------------------------------------------------------

def group_stocks_by_parent(stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Collapse a flat stock list into leaderboard parent-sector groups.

    Each group dict:
      {
        "group_name":     "Technology",
        "count":          87,
        "avg_change_pct": 9.2,
        "tickers":        ["NVDA", "AMD", ...],
        "sub_industries": [
          {
            "industry":      "Semiconductors",
            "thematic_label":"AI & Semiconductors",
            "count":         34,
            "tickers":       ["ADI", "ALAB", ...]
          },
          ...
        ]
      }

    Groups are sorted by count descending.
    Sub-industries within each group are also sorted by count descending.
    """
    from collections import defaultdict

    parent_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for stock in stocks:
        parent = stock.get("leaderboard_parent") or "Other"
        parent_buckets[parent].append(stock)

    groups: list[dict[str, Any]] = []
    for parent_name, members in parent_buckets.items():
        # avg change_pct across members with a value
        changes = [m["change_pct"] for m in members if m.get("change_pct") is not None]
        avg_change = round(sum(changes) / len(changes), 2) if changes else None

        # Sub-industries: group by (industry, thematic_label)
        sub_buckets: dict[tuple[str, str], list[str]] = defaultdict(list)
        for m in members:
            ind = m.get("industry") or "Unknown"
            thematic = m.get("thematic_label") or ""
            sub_buckets[(ind, thematic)].append(m["ticker"])

        sub_industries = sorted(
            [
                {
                    "industry":       ind,
                    "thematic_label": thematic,
                    "count":          len(tickers),
                    "tickers":        tickers,
                }
                for (ind, thematic), tickers in sub_buckets.items()
            ],
            key=lambda x: x["count"],
            reverse=True,
        )

        groups.append({
            "group_name":     parent_name,
            "count":          len(members),
            "avg_change_pct": avg_change,
            "tickers":        [m["ticker"] for m in members],
            "sub_industries": sub_industries,
        })

    groups.sort(key=lambda g: g["count"], reverse=True)
    return groups
