"""
breadth_ext.py — Post-close computed breadth columns for the Stockbee Market Monitor.

Computes two values from today's "up 4%+" universe (fetched from Finviz screener):
  - atr_10x_ext:     Count of stocks where (price - SMA50) >= 10 × ATR14
  - above_50dma_pct: % of stocks above their 50-day SMA

Results are cached for the calendar trading date (ET). After 4 PM ET the cache
is still valid for the rest of the evening (no continuous refetching).
Resets automatically the next calendar day.

Usage:
    from backend.breadth_ext import get_breadth_ext_today
    result = await get_breadth_ext_today()
    # → {"atr_10x_ext": 42, "above_50dma_pct": 61.3, "computed_date": "2026-04-11",
    #    "universe_count": 187, "ok": True}
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Finviz screener: US stocks up >= 4% today, liquid (avg vol > 100k), any cap.
# sh_avgvol_o100 = avg volume over 100k; ta_changeopen_u4 = change from open > 4%.
# We use geo_usa to keep it US-only and sh_price_o5 to avoid sub-penny stocks.
_FINVIZ_UP4_PATH = (
    "/screener.ashx?v=111"
    "&f=geo_usa,sh_avgvol_o100,sh_price_o5,ta_change_u4"
    "&o=-change"
)
_FINVIZ_BASE = "https://finviz.com"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Cache: (computed_date_str, result_dict)
_CACHE: tuple[str, dict[str, Any]] | None = None
_CACHE_LOCK = asyncio.Lock()


def _today_et() -> str:
    """Current date in US/Eastern as YYYY-MM-DD string."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


def _extract_tickers_from_html(html: str) -> list[str]:
    """Pull ticker symbols from a Finviz screener overview page."""
    soup = BeautifulSoup(html, "html.parser")
    tickers: list[str] = []
    # Finviz screener: ticker links are <a> tags with href="/quote.ashx?t=TICKER"
    for a in soup.select("a[href*='quote.ashx']"):
        href = a.get("href") or ""
        if "t=" not in href:
            continue
        sym = href.split("t=")[-1].split("&")[0].strip().upper()
        if sym and 1 <= len(sym) <= 6 and sym.isalpha():
            tickers.append(sym)
    # De-dupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


async def _fetch_up4_tickers(max_pages: int = 15) -> list[str]:
    """
    Paginate through Finviz screener to collect all tickers up ≥4% today.
    Finviz paginates at 20 rows per page using r= offset.
    """
    all_tickers: list[str] = []
    seen: set[str] = set()
    async with httpx.AsyncClient(
        timeout=25.0, headers=_BROWSER_HEADERS, follow_redirects=True
    ) as client:
        for page in range(max_pages):
            offset = 1 + page * 20
            url = f"{_FINVIZ_BASE}{_FINVIZ_UP4_PATH}&r={offset}"
            try:
                r = await client.get(url)
                r.raise_for_status()
                page_tickers = _extract_tickers_from_html(r.text)
            except Exception as exc:
                logger.warning("breadth_ext: Finviz page %d fetch failed: %s", page + 1, exc)
                break
            if not page_tickers:
                break
            new = [t for t in page_tickers if t not in seen]
            if not new:
                break  # no new tickers = we've exhausted the screener
            for t in new:
                seen.add(t)
            all_tickers.extend(new)
            await asyncio.sleep(0.3)  # be polite to Finviz
    logger.info("breadth_ext: fetched %d up-4%% tickers from Finviz", len(all_tickers))
    return all_tickers


def _compute_atr_and_50dma_sync(tickers: list[str]) -> dict[str, Any]:
    """
    For each ticker, compute:
      - SMA50 (50-day simple moving average of Close)
      - ATR14 (14-day average true range)
      - Whether price > SMA50
      - Whether (price - SMA50) >= 10 * ATR14   ← 10x ATR extension

    Uses yfinance batch download for efficiency. Caps at 500 tickers to avoid
    Yahoo throttling on a single call. For larger universes, chunk into 250.

    Returns dict with aggregated counts.
    """
    if not tickers:
        return {"atr_10x_ext": 0, "above_50dma_pct": None, "universe_count": 0, "ok": True}

    cap = min(len(tickers), 500)
    sample = tickers[:cap]

    try:
        # Download ~4 months of daily OHLC — enough for SMA50 + ATR14
        df = yf.download(
            tickers=sample,
            period="4mo",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
    except Exception as exc:
        logger.error("breadth_ext: yfinance download failed: %s", exc)
        return {"atr_10x_ext": None, "above_50dma_pct": None, "universe_count": cap, "ok": False}

    if df is None or getattr(df, "empty", True):
        return {"atr_10x_ext": None, "above_50dma_pct": None, "universe_count": cap, "ok": False}

    is_single = len(sample) == 1

    def _get_series(panel: str, tkr: str):
        try:
            return df[panel].dropna() if is_single else df[tkr][panel].dropna()
        except Exception:
            return None

    atr_10x_count = 0
    above_50_count = 0
    usable = 0

    for tkr in sample:
        close = _get_series("Close", tkr)
        high = _get_series("High", tkr)
        low = _get_series("Low", tkr)

        if close is None or high is None or low is None:
            continue
        if len(close) < 52:  # need at least 50 closes + 2 for ATR prev-close
            continue

        # SMA50
        sma50_series = close.rolling(50).mean()
        sma50 = float(sma50_series.iloc[-1])
        if sma50 != sma50:  # NaN guard
            continue

        last_close = float(close.iloc[-1])

        # ATR14: TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        import pandas as pd  # local import — pandas is already a dep via yfinance
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr14 = float(tr.rolling(14).mean().iloc[-1])
        if atr14 != atr14 or atr14 <= 0:
            continue

        usable += 1

        if last_close > sma50:
            above_50_count += 1

        extension = last_close - sma50
        if extension >= 10 * atr14:
            atr_10x_count += 1

    above_50dma_pct = round(above_50_count / usable * 100.0, 1) if usable > 0 else None

    logger.info(
        "breadth_ext: usable=%d  atr_10x=%d  above50=%d (%.1f%%)",
        usable, atr_10x_count, above_50_count, above_50dma_pct or 0,
    )

    return {
        "atr_10x_ext": atr_10x_count,
        "above_50dma_pct": above_50dma_pct,
        "universe_count": len(sample),
        "usable_count": usable,
        "ok": True,
    }


async def get_breadth_ext_today() -> dict[str, Any]:
    """
    Public entry point. Returns cached result if already computed today (ET date).
    Otherwise fetches Finviz up-4%+ universe and runs yfinance computation.

    Safe to call from multiple FastAPI endpoints concurrently — uses an asyncio lock.
    """
    global _CACHE

    today = _today_et()

    async with _CACHE_LOCK:
        if _CACHE is not None and _CACHE[0] == today:
            logger.debug("breadth_ext: serving from cache (date=%s)", today)
            return {**_CACHE[1], "computed_date": today, "cached": True}

        logger.info("breadth_ext: computing fresh for date=%s", today)
        try:
            tickers = await _fetch_up4_tickers()
            if not tickers:
                result: dict[str, Any] = {
                    "atr_10x_ext": None,
                    "above_50dma_pct": None,
                    "universe_count": 0,
                    "ok": False,
                    "detail": "No up-4%+ tickers found in Finviz screener.",
                }
            else:
                result = await asyncio.to_thread(_compute_atr_and_50dma_sync, tickers)
        except Exception as exc:
            logger.error("breadth_ext: computation failed: %s", exc)
            result = {
                "atr_10x_ext": None,
                "above_50dma_pct": None,
                "universe_count": 0,
                "ok": False,
                "detail": str(exc),
            }

        _CACHE = (today, result)
        return {**result, "computed_date": today, "cached": False}
