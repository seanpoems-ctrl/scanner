from __future__ import annotations

import json
import os
from dataclasses import dataclass
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

