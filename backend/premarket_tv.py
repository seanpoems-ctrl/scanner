"""
Pre-market gappers via TradingView's public scanner endpoint, built with
tradingview-screener (Query / col) — same criteria family as the TV desktop screener.

See: https://github.com/shner-elmo/TradingView-Screener
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from tradingview_screener import Query, col

TV_SELECT = (
    "name",
    "close",
    "change",
    "premarket_gap",
    "premarket_volume",
    "volume",
    "relative_volume_10d_calc",
    "average_volume_10d_calc",
    "Volatility.D",
    "float_shares_outstanding",
    "market_cap_basic",
    "sector",
    "industry",
    "description",
)


@dataclass(frozen=True, slots=True)
class PremarketTvParams:
    min_gap_pct: float = 0.0
    min_pm_vol_k: float = 0.0
    min_price: float = 0.0
    min_avg_vol_10d_k: float = 0.0
    min_mkt_cap_b: float = 0.0
    min_avg_dollar_vol_m: float = 0.0
    limit: int = 100


def _df_to_rows(df: pd.DataFrame | None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    records = df.replace({np.nan: None}).to_dict("records")
    out: list[dict[str, Any]] = []
    for r in records:
        clean: dict[str, Any] = {}
        for k, v in r.items():
            key = str(k)
            if v is None:
                clean[key] = None
            elif isinstance(v, (np.floating, float)) and (math.isnan(float(v)) or math.isinf(float(v))):
                clean[key] = None
            elif isinstance(v, (np.integer, np.floating)):
                clean[key] = float(v) if isinstance(v, np.floating) else int(v)
            else:
                clean[key] = v
        out.append(clean)
    return out


def run_premarket_tv_scan_sync(params: PremarketTvParams) -> dict[str, Any]:
    """Synchronous scan; call from asyncio.to_thread."""
    lim = max(10, min(500, int(params.limit)))
    q = (
        Query()
        .set_markets("america")
        .select(*TV_SELECT)
        .order_by("premarket_gap", ascending=False)
        .limit(lim)
    )

    conditions: list[Any] = []
    if params.min_gap_pct > 0:
        conditions.append(col("premarket_gap") >= float(params.min_gap_pct))
    if params.min_pm_vol_k > 0:
        conditions.append(col("premarket_volume") >= float(params.min_pm_vol_k) * 1000.0)
    if params.min_price > 0:
        conditions.append(col("close") >= float(params.min_price))
    if params.min_avg_vol_10d_k > 0:
        conditions.append(col("average_volume_10d_calc") >= float(params.min_avg_vol_10d_k) * 1000.0)
    if params.min_mkt_cap_b > 0:
        conditions.append(col("market_cap_basic") >= float(params.min_mkt_cap_b) * 1_000_000_000.0)

    if conditions:
        q = q.where(*conditions)

    total_raw, df = q.get_scanner_data()
    if df is None:
        df = pd.DataFrame()

    pre_dollar_n = len(df)
    if params.min_avg_dollar_vol_m > 0 and not df.empty:
        if "average_volume_10d_calc" in df.columns and "close" in df.columns:
            dv = pd.to_numeric(df["average_volume_10d_calc"], errors="coerce") * pd.to_numeric(
                df["close"], errors="coerce"
            )
            df = df[dv >= float(params.min_avg_dollar_vol_m) * 1_000_000.0].copy()

    if not df.empty and "premarket_volume" in df.columns and "average_volume_10d_calc" in df.columns:
        df = df.copy()
        pm = pd.to_numeric(df["premarket_volume"], errors="coerce")
        avg = pd.to_numeric(df["average_volume_10d_calc"], errors="coerce")
        df["premarket_rvol_calc"] = (pm / avg).where((avg.notna()) & (avg > 0))

    rows = _df_to_rows(df)
    return {
        "source": "tradingview-screener",
        "market": "america",
        "total_matched_scanner": int(total_raw) if isinstance(total_raw, (int, float)) else total_raw,
        "rows_after_avg_dollar_filter": len(rows),
        "rows_tv_page": pre_dollar_n,
        "filters": {
            "min_gap_pct": params.min_gap_pct,
            "min_pm_vol_k": params.min_pm_vol_k,
            "min_price": params.min_price,
            "min_avg_vol_10d_k": params.min_avg_vol_10d_k,
            "min_mkt_cap_b": params.min_mkt_cap_b,
            "min_avg_dollar_vol_m": params.min_avg_dollar_vol_m,
            "limit": lim,
        },
        "row_count": len(rows),
        "rows": rows,
    }
