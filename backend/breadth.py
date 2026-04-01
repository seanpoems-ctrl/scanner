from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import yfinance as yf


UNIVERSE_PATH = os.path.join(os.path.dirname(__file__), "data", "theme_universe.json")


@dataclass(frozen=True, slots=True)
class BreadthSnapshot:
    universe_tickers: int
    sample_tickers: int
    above_50dma_pct: float | None
    above_200dma_pct: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "universe_tickers": self.universe_tickers,
            "sample_tickers": self.sample_tickers,
            "above_50dma_pct": self.above_50dma_pct,
            "above_200dma_pct": self.above_200dma_pct,
        }


@dataclass(frozen=True, slots=True)
class OceanSnapshot:
    """Market Ocean regime snapshot."""
    s5fi: float | None                       # % of S&P 500 stocks above 50-day MA (0-100)
    speedboat_count: int | None              # count of universe stocks up >4% w/ vol>100k, price>$5
    s5fi_history: list[dict[str, Any]] = field(default_factory=list)      # 10-day [{date, value}]
    speedboat_history: list[dict[str, Any]] = field(default_factory=list) # 10-day [{date, value}]
    universe_size: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "s5fi": self.s5fi,
            "speedboat_count": self.speedboat_count,
            "s5fi_history": self.s5fi_history,
            "speedboat_history": self.speedboat_history,
            "universe_size": self.universe_size,
        }


def _load_universe_tickers(path: str = UNIVERSE_PATH) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f) or {}
    tickers: set[str] = set()
    for t in raw.get("themes", []) or []:
        for sym in (t.get("tickers") or []) if isinstance(t, dict) else []:
            s = str(sym or "").upper().strip()
            if not s:
                continue
            # Keep it simple: US-style tickers only (avoid ETFs with special chars are still OK, e.g. BRK-B).
            tickers.add(s)
    return sorted(tickers)


def compute_theme_universe_breadth_sync(*, max_tickers: int = 250) -> BreadthSnapshot:
    """
    Compute a simple, stable "MMTH-like" breadth snapshot from the persisted theme-universe tickers:
    - % of sampled tickers above 50DMA
    - % of sampled tickers above 200DMA

    Notes:
    - We cap tickers to keep it fast and reduce Yahoo throttling risk.
    - Sampling is deterministic (alphabetical) for repeatability.
    """
    tickers = _load_universe_tickers()
    universe_n = len(tickers)
    if universe_n == 0:
        return BreadthSnapshot(universe_tickers=0, sample_tickers=0, above_50dma_pct=None, above_200dma_pct=None)

    sample = tickers[: max(1, min(max_tickers, universe_n))]

    # ~1y of dailies is enough to cover 200DMA for most tickers.
    df = yf.download(
        tickers=sample,
        period="1y",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if df is None or getattr(df, "empty", True):
        return BreadthSnapshot(universe_tickers=universe_n, sample_tickers=len(sample), above_50dma_pct=None, above_200dma_pct=None)

    above_50 = 0
    above_200 = 0
    usable_50 = 0
    usable_200 = 0

    def _series_close(tkr: str):
        try:
            if len(sample) == 1:
                return df["Close"].dropna()
            return df[tkr]["Close"].dropna()
        except Exception:
            return None

    for tkr in sample:
        closes = _series_close(tkr)
        if closes is None or len(closes) < 60:
            continue
        last = float(closes.iloc[-1])
        sma50 = closes.rolling(50).mean().iloc[-1]
        if sma50 == sma50:  # not NaN
            usable_50 += 1
            if last > float(sma50):
                above_50 += 1
        if len(closes) >= 220:
            sma200 = closes.rolling(200).mean().iloc[-1]
            if sma200 == sma200:
                usable_200 += 1
                if last > float(sma200):
                    above_200 += 1

    above_50_pct = (above_50 / usable_50 * 100.0) if usable_50 else None
    above_200_pct = (above_200 / usable_200 * 100.0) if usable_200 else None

    return BreadthSnapshot(
        universe_tickers=universe_n,
        sample_tickers=len(sample),
        above_50dma_pct=above_50_pct,
        above_200dma_pct=above_200_pct,
    )


# ---------------------------------------------------------------------------
# S5FI proxy: % of S&P 500 components above their 50-day SMA
# ---------------------------------------------------------------------------

# Representative 100-stock proxy for S&P 500 breadth — covers all 11 sectors.
# Updating this list is enough to improve accuracy; no structural code changes needed.
_SP500_PROXY = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "JPM", "V",
    "UNH", "XOM", "LLY", "JNJ", "PG", "MA", "HD", "MRK", "AVGO", "CVX",
    "PEP", "KO", "ABBV", "COST", "MCD", "CSCO", "ACN", "BAC", "CRM", "WMT",
    "TMO", "ABT", "NFLX", "LIN", "DHR", "TXN", "NKE", "CMCSA", "DIS", "VZ",
    "INTC", "AMD", "QCOM", "HON", "LOW", "UPS", "CAT", "SBUX", "GS", "MS",
    "RTX", "BMY", "AMGN", "T", "MDT", "CVS", "ISRG", "GILD", "REGN", "VRTX",
    "SYK", "CI", "HUM", "DE", "GE", "MMM", "BA", "LMT", "NOC", "ETN",
    "AXP", "BLK", "SPGI", "ICE", "CME", "AON", "USB", "WFC", "PNC", "TFC",
    "NEE", "DUK", "SO", "SRE", "D", "EXC", "AEP", "PLD", "AMT", "EQIX",
    "PSA", "SPG", "O", "WY", "FCX", "NEM", "VMC", "MLM", "NUE", "ADM",
]


def _fetch_s5fi_sync(*, history_days: int = 10) -> tuple[float | None, list[dict[str, Any]]]:
    """
    Compute S5FI: % of _SP500_PROXY stocks above their 50-day SMA on each of the
    last `history_days` trading sessions. Returns (today_value, history_list).
    """
    try:
        df = yf.download(
            tickers=_SP500_PROXY,
            period="90d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
        if df is None or getattr(df, "empty", True):
            return None, []

        # Build a date-indexed series of "% above 50SMA" for the last history_days sessions.
        # We use the Close panel across all tickers.
        import pandas as pd  # local import to avoid top-level overhead

        try:
            closes_panel = df["Close"] if len(_SP500_PROXY) > 1 else df[["Close"]].rename(columns={"Close": _SP500_PROXY[0]})
        except Exception:
            return None, []

        closes_panel = closes_panel.dropna(how="all")
        if len(closes_panel) < 55:
            return None, []

        history: list[dict[str, Any]] = []
        trading_dates = closes_panel.index[-history_days:]

        for dt in trading_dates:
            # For each date, count how many tickers have close > 50SMA computed up to that date
            slice_df = closes_panel.loc[:dt]
            if len(slice_df) < 52:
                continue
            above = 0
            total = 0
            for col in closes_panel.columns:
                col_series = slice_df[col].dropna()
                if len(col_series) < 52:
                    continue
                sma50 = col_series.rolling(50).mean().iloc[-1]
                last_close = col_series.iloc[-1]
                if sma50 != sma50:  # NaN check
                    continue
                total += 1
                if float(last_close) > float(sma50):
                    above += 1
            if total > 0:
                pct = round(above / total * 100.0, 1)
                history.append({"date": str(dt.date()) if hasattr(dt, "date") else str(dt)[:10], "value": pct})

        today_val = history[-1]["value"] if history else None
        return today_val, history

    except Exception:
        return None, []


# ---------------------------------------------------------------------------
# Speedboat count — institutional-grade elite momentum filter
# Criteria (ALL must be met):
#   Price          > $12
#   Avg $ Vol 30d  > $100 M/day
#   Daily change   > +4%
#   ADR% 20d       > 4%   (average daily range as % of price)
#   Market cap     > $2 B  (yfinance fast_info, best-effort)
# ---------------------------------------------------------------------------

# Minimum market-cap tiers (yfinance fast_info.market_cap is best-effort).
_SPEEDBOAT_MIN_PRICE       = 12.0
_SPEEDBOAT_MIN_AVG_DVOL_M  = 100.0   # million USD
_SPEEDBOAT_MIN_CHG_PCT     = 4.0
_SPEEDBOAT_MIN_ADR_PCT     = 4.0
_SPEEDBOAT_MIN_MCAP_B      = 2.0     # billion USD


def _fetch_speedboat_count_sync(
    tickers: list[str],
    *,
    history_days: int = 10,
) -> tuple[int | None, list[dict[str, Any]]]:
    """
    Elite Speedboat count: universe stocks that clear ALL institutional filters
    (price >$12, avg daily dollar vol >$100M, change >4%, ADR% >4%, mktcap >$2B)
    on each of the last `history_days` trading sessions.
    """
    if not tickers:
        return None, []
    try:
        sample = tickers[:300]  # cap to keep Yahoo calls manageable

        # Need ~55 days to compute 30d avg-dvol and 20d ADR
        df = yf.download(
            tickers=sample,
            period="90d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
        if df is None or getattr(df, "empty", True):
            return None, []

        is_single = len(sample) == 1

        def _col(panel_key: str, tkr: str):
            try:
                if is_single:
                    return df[panel_key].dropna()
                return df[tkr][panel_key].dropna()
            except Exception:
                return None

        # Market-cap pre-filter via yfinance fast_info (best-effort; skip on failure)
        mcap_ok: set[str] = set()
        for tkr in sample:
            try:
                mc = yf.Ticker(tkr).fast_info.get("market_cap") or 0
                if mc >= _SPEEDBOAT_MIN_MCAP_B * 1e9:
                    mcap_ok.add(tkr)
            except Exception:
                mcap_ok.add(tkr)  # don't exclude on error

        # Determine common trading dates
        try:
            all_closes = (
                df["Close"] if not is_single
                else df[["Close"]].rename(columns={"Close": sample[0]})
            )
        except Exception:
            return None, []
        all_closes = all_closes.dropna(how="all")
        if len(all_closes) < 3:
            return None, []

        history: list[dict[str, Any]] = []
        trading_dates = all_closes.index[-history_days:]

        for dt in trading_dates:
            count = 0
            for tkr in sample:
                if tkr not in mcap_ok:
                    continue
                closes_s = _col("Close", tkr)
                volumes_s = _col("Volume", tkr)
                highs_s   = _col("High",  tkr)
                lows_s    = _col("Low",   tkr)
                if closes_s is None or volumes_s is None or highs_s is None or lows_s is None:
                    continue
                closes_s  = closes_s.loc[:dt]
                volumes_s = volumes_s.loc[:dt]
                highs_s   = highs_s.loc[:dt]
                lows_s    = lows_s.loc[:dt]
                if len(closes_s) < 32:  # need 30d for avg-dvol + prev close
                    continue

                today_c = float(closes_s.iloc[-1])
                prev_c  = float(closes_s.iloc[-2])
                if prev_c <= 0 or today_c <= 0:
                    continue

                # Price filter
                if today_c <= _SPEEDBOAT_MIN_PRICE:
                    continue

                # Daily change filter
                chg_pct = (today_c - prev_c) / prev_c * 100.0
                if chg_pct <= _SPEEDBOAT_MIN_CHG_PCT:
                    continue

                # Avg daily dollar volume (last 30 sessions)
                dvol_series = closes_s.iloc[-30:] * volumes_s.iloc[-30:]
                avg_dvol = float(dvol_series.mean())
                if avg_dvol < _SPEEDBOAT_MIN_AVG_DVOL_M * 1e6:
                    continue

                # ADR% — 20-day average of (High-Low)/Close
                if len(highs_s) >= 20 and len(lows_s) >= 20:
                    adr_series = (highs_s.iloc[-20:] - lows_s.iloc[-20:]) / closes_s.iloc[-20:] * 100.0
                    adr_pct = float(adr_series.mean())
                else:
                    continue
                if adr_pct <= _SPEEDBOAT_MIN_ADR_PCT:
                    continue

                count += 1

            history.append({
                "date": str(dt.date()) if hasattr(dt, "date") else str(dt)[:10],
                "value": count,
            })

        today_count = history[-1]["value"] if history else None
        return today_count, history

    except Exception:
        return None, []


def compute_market_ocean_sync(*, history_days: int = 10) -> OceanSnapshot:
    """
    Full Market Ocean snapshot:
    - S5FI (% of S&P 500 proxy above 50SMA) with 10-day history
    - Elite Speedboat count with institutional filters, with 10-day history
    """
    tickers = _load_universe_tickers()
    s5fi, s5fi_hist = _fetch_s5fi_sync(history_days=history_days)
    speedboat_count, speedboat_hist = _fetch_speedboat_count_sync(tickers, history_days=history_days)
    return OceanSnapshot(
        s5fi=s5fi,
        speedboat_count=speedboat_count,
        s5fi_history=s5fi_hist,
        speedboat_history=speedboat_hist,
        universe_size=len(tickers),
    )

