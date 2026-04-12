"""
Tests for backend/breadth_stocks.py and GET /api/market-breadth/stocks.

Strategy
--------
- All Finviz HTTP calls are mocked with a fake HTML fixture.
- All yfinance calls are mocked with a minimal DataFrame fixture.
- No network I/O, no disk I/O.
- Cache state is reset between tests via module-level _CACHE manipulation.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_finviz_html(rows: list[dict[str, str]]) -> str:
    """
    Build a minimal Finviz v=111 overview HTML page with the given row data.
    Column order: #, ticker, company, sector, industry, country, mktcap, pe, price, change, volume
    """
    trs = ""
    for i, r in enumerate(rows, start=1):
        trs += f"""
        <tr>
          <td>{i}</td>
          <td><a href="/quote.ashx?t={r['ticker']}&ty=c">{r['ticker']}</a></td>
          <td>{r.get('company', 'Acme Corp')}</td>
          <td>{r.get('sector', 'Technology')}</td>
          <td>{r.get('industry', 'Software')}</td>
          <td>USA</td>
          <td>{r.get('mktcap', '2.50B')}</td>
          <td>25.0</td>
          <td>{r.get('price', '50.00')}</td>
          <td>{r.get('change', '+5.00%')}</td>
          <td>{r.get('volume', '1,000,000')}</td>
        </tr>"""

    return f"<html><body><table>{trs}</table></body></html>"


def _make_yf_df(tickers: list[str], *, high: float = 55.0, low: float = 45.0, close: float = 50.0) -> pd.DataFrame:
    """
    Build a minimal yfinance-style multi-ticker DataFrame for 20 rows.
    group_by='ticker' produces a MultiIndex column: (ticker, OHLCV).
    """
    import numpy as np
    dates = pd.date_range("2026-03-01", periods=20, freq="B")
    if len(tickers) == 1:
        df = pd.DataFrame(
            {
                "High":   [high] * 20,
                "Low":    [low] * 20,
                "Close":  [close] * 20,
                "Open":   [close] * 20,
                "Volume": [1_000_000] * 20,
            },
            index=dates,
        )
    else:
        arrays = []
        for tkr in tickers:
            for col in ["High", "Low", "Close", "Open", "Volume"]:
                arrays.append((tkr, col))
        idx = pd.MultiIndex.from_tuples(arrays)
        data = {}
        for tkr in tickers:
            data[(tkr, "High")]   = [high] * 20
            data[(tkr, "Low")]    = [low] * 20
            data[(tkr, "Close")]  = [close] * 20
            data[(tkr, "Open")]   = [close] * 20
            data[(tkr, "Volume")] = [1_000_000] * 20
        df = pd.DataFrame(data, index=dates)
        df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_breadth_stocks_cache():
    """Reset module-level cache before each test to prevent cross-test pollution."""
    import breadth_stocks
    breadth_stocks._CACHE.clear()
    # Also reset the asyncio lock to a fresh one in case of event loop issues.
    breadth_stocks._CACHE_LOCK = asyncio.Lock()
    yield
    breadth_stocks._CACHE.clear()


@pytest.fixture()
def client():
    from main import app
    return TestClient(app)


# ── Unit tests: parsing helpers ───────────────────────────────────────────────

class TestParseMarketCapB:
    def test_billions(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("1.23B") == pytest.approx(1.23)

    def test_millions(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("500M") == pytest.approx(0.5)

    def test_trillions(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("2T") == pytest.approx(2000.0)

    def test_dash_returns_none(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("-") is None

    def test_empty_returns_none(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("") is None

    def test_commas_stripped(self):
        from breadth_stocks import _parse_market_cap_b
        assert _parse_market_cap_b("1,234.56M") == pytest.approx(1.23456)


class TestParsePct:
    def test_positive(self):
        from breadth_stocks import _parse_pct
        assert _parse_pct("+5.23%") == pytest.approx(5.23)

    def test_negative(self):
        from breadth_stocks import _parse_pct
        assert _parse_pct("-2.14%") == pytest.approx(-2.14)

    def test_no_sign(self):
        from breadth_stocks import _parse_pct
        assert _parse_pct("25.00%") == pytest.approx(25.0)

    def test_dash_returns_none(self):
        from breadth_stocks import _parse_pct
        assert _parse_pct("-") is None


class TestFmtDollarVol:
    def test_billions(self):
        from breadth_stocks import _fmt_dollar_vol
        assert _fmt_dollar_vol(100.0, 20_000_000) == "$2.0B"

    def test_millions(self):
        from breadth_stocks import _fmt_dollar_vol
        assert _fmt_dollar_vol(50.0, 5_000_000) == "$250M"

    def test_thousands(self):
        from breadth_stocks import _fmt_dollar_vol
        assert _fmt_dollar_vol(10.0, 50_000) == "$500K"


class TestParseScreenerRows:
    def test_parses_valid_rows(self):
        from breadth_stocks import _parse_screener_rows
        html = _make_finviz_html([
            {"ticker": "AAPL", "mktcap": "3.0T", "price": "175.00", "change": "+1.50%", "volume": "50,000,000"},
            {"ticker": "NVDA", "mktcap": "2.0T", "price": "850.00", "change": "+2.30%", "volume": "30,000,000"},
        ])
        rows = _parse_screener_rows(html)
        assert len(rows) == 2
        assert rows[0]["ticker"] == "AAPL"
        assert rows[1]["ticker"] == "NVDA"
        assert rows[0]["market_cap_raw"] == "3.0T"
        assert rows[0]["change_raw"] == "+1.50%"

    def test_skips_header_rows(self):
        from breadth_stocks import _parse_screener_rows
        html = "<html><table><tr><th>No.</th><th>Ticker</th></tr></table></html>"
        assert _parse_screener_rows(html) == []

    def test_skips_rows_without_quote_link(self):
        from breadth_stocks import _parse_screener_rows
        html = (
            "<html><table>"
            "<tr><td>1</td><td><a href='/other'>FOO</a></td>"
            "<td>Foo</td><td>X</td><td>Y</td><td>US</td>"
            "<td>1B</td><td>-</td><td>10</td><td>+1%</td><td>100</td></tr>"
            "</table></html>"
        )
        assert _parse_screener_rows(html) == []


# ── Unit tests: ADR% computation ─────────────────────────────────────────────

class TestComputeAdrBatchSync:
    def test_single_ticker(self):
        from breadth_stocks import _compute_adr_batch_sync
        df = _make_yf_df(["AAPL"], high=55.0, low=45.0, close=50.0)
        # ADR% = (55-45)/50*100 = 20.0 per day → mean = 20.0
        with patch("yfinance.download", return_value=df):
            result = _compute_adr_batch_sync(["AAPL"])
        assert result["AAPL"] == pytest.approx(20.0, abs=0.1)

    def test_multi_ticker(self):
        from breadth_stocks import _compute_adr_batch_sync
        df = _make_yf_df(["AAPL", "MSFT"], high=60.0, low=40.0, close=50.0)
        with patch("yfinance.download", return_value=df):
            result = _compute_adr_batch_sync(["AAPL", "MSFT"])
        # ADR% = (60-40)/50*100 = 40.0
        assert result["AAPL"] == pytest.approx(40.0, abs=0.1)
        assert result["MSFT"] == pytest.approx(40.0, abs=0.1)

    def test_empty_input(self):
        from breadth_stocks import _compute_adr_batch_sync
        assert _compute_adr_batch_sync([]) == {}

    def test_yfinance_failure_returns_none(self):
        from breadth_stocks import _compute_adr_batch_sync
        with patch("yfinance.download", side_effect=RuntimeError("throttled")):
            result = _compute_adr_batch_sync(["AAPL"])
        assert result["AAPL"] is None


# ── Integration tests: fetch_breadth_stock_list ───────────────────────────────

SAMPLE_ROWS = [
    {"ticker": "BIGCAP", "company": "Big Cap Co",   "industry": "Software",   "mktcap": "5.0B",  "price": "50.00", "change": "+8.00%", "volume": "2,000,000"},
    {"ticker": "MIDCAP", "company": "Mid Cap Co",   "industry": "Hardware",   "mktcap": "1.5B",  "price": "30.00", "change": "+5.00%", "volume": "1,500,000"},
    {"ticker": "SMACAP", "company": "Small Cap Co", "industry": "Biotech",    "mktcap": "300M",  "price": "10.00", "change": "+20.00%","volume": "5,000,000"},
]


def _make_mock_response(html: str) -> AsyncMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = html
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.anyio
async def test_fetch_filters_by_min_cap():
    """Stocks below min_cap_b should be excluded."""
    html = _make_finviz_html(SAMPLE_ROWS)
    adr_df = _make_yf_df(["BIGCAP", "MIDCAP"])

    with (
        patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
        patch("yfinance.download", return_value=adr_df),
    ):
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        # Return real HTML for page 1, empty for page 2 (stop pagination)
        empty_html = "<html><table></table></html>"
        mock_client.get = AsyncMock(side_effect=[
            _make_mock_response(html),
            _make_mock_response(empty_html),
        ])
        mock_client_cls.return_value = mock_client

        from breadth_stocks import fetch_breadth_stock_list
        results = await fetch_breadth_stock_list("up4", min_cap_b=1.0)

    tickers = [r["ticker"] for r in results]
    assert "BIGCAP" in tickers
    assert "MIDCAP" in tickers
    assert "SMACAP" not in tickers  # 300M < 1.0B


@pytest.mark.anyio
async def test_fetch_up_filter_sorts_descending():
    """up* filter → sorted change_pct descending (best first)."""
    html = _make_finviz_html(SAMPLE_ROWS)
    adr_df = _make_yf_df(["BIGCAP", "MIDCAP"])

    with (
        patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
        patch("yfinance.download", return_value=adr_df),
    ):
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=[
            _make_mock_response(html),
            _make_mock_response("<html><table></table></html>"),
        ])
        mock_client_cls.return_value = mock_client

        from breadth_stocks import fetch_breadth_stock_list
        results = await fetch_breadth_stock_list("up4", min_cap_b=1.0)

    changes = [r["change_pct"] for r in results if r["change_pct"] is not None]
    assert changes == sorted(changes, reverse=True)


@pytest.mark.anyio
async def test_fetch_dn_filter_sorts_ascending():
    """dn* filter → sorted change_pct ascending (worst first)."""
    rows = [
        {"ticker": "AA", "mktcap": "2B", "price": "20.00", "change": "-3.00%", "volume": "1,000,000"},
        {"ticker": "BB", "mktcap": "3B", "price": "30.00", "change": "-8.00%", "volume": "2,000,000"},
    ]
    html = _make_finviz_html(rows)
    adr_df = _make_yf_df(["AA", "BB"])

    with (
        patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
        patch("yfinance.download", return_value=adr_df),
    ):
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=[
            _make_mock_response(html),
            _make_mock_response("<html><table></table></html>"),
        ])
        mock_client_cls.return_value = mock_client

        from breadth_stocks import fetch_breadth_stock_list
        results = await fetch_breadth_stock_list("dn4", min_cap_b=1.0)

    changes = [r["change_pct"] for r in results if r["change_pct"] is not None]
    assert changes == sorted(changes)  # ascending


@pytest.mark.anyio
async def test_fetch_cache_hit_skips_network():
    """Second call with same filter+date should return from cache without hitting Finviz."""
    html = _make_finviz_html(SAMPLE_ROWS)
    adr_df = _make_yf_df(["BIGCAP", "MIDCAP"])

    with (
        patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
        patch("yfinance.download", return_value=adr_df),
    ):
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=[
            _make_mock_response(html),
            _make_mock_response("<html><table></table></html>"),
        ])
        mock_client_cls.return_value = mock_client

        from breadth_stocks import fetch_breadth_stock_list
        r1 = await fetch_breadth_stock_list("up4", min_cap_b=1.0)
        r2 = await fetch_breadth_stock_list("up4", min_cap_b=1.0)

    assert r1 == r2
    # The early-stop logic breaks as soon as a page returns fewer than 20 rows,
    # so the 3-row test fixture causes exactly 1 fetch (not 2).
    # The second call to fetch_breadth_stock_list should return from cache —
    # proving that get() was NOT called a second time (still 1, not 2).
    assert mock_client.get.call_count == 1


@pytest.mark.anyio
async def test_fetch_invalid_filter_raises():
    from breadth_stocks import fetch_breadth_stock_list
    with pytest.raises(ValueError, match="Unknown filter key"):
        await fetch_breadth_stock_list("bogus", min_cap_b=1.0)


# ── API endpoint tests ─────────────────────────────────────────────────────────

class TestMarketBreadthStocksEndpoint:
    def test_invalid_filter_returns_422(self, client):
        r = client.get("/api/market-breadth/stocks?filter=invalid")
        assert r.status_code == 422

    def test_valid_filter_shape(self, client):
        html = _make_finviz_html(SAMPLE_ROWS)
        adr_df = _make_yf_df(["BIGCAP", "MIDCAP"])

        with (
            patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
            patch("yfinance.download", return_value=adr_df),
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(side_effect=[
                _make_mock_response(html),
                _make_mock_response("<html><table></table></html>"),
            ])
            mock_client_cls.return_value = mock_client

            r = client.get("/api/market-breadth/stocks?filter=up4&min_cap_b=1.0")

        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["filter"] == "up4"
        assert body["min_cap_b"] == 1.0
        assert isinstance(body["count"], int)
        assert isinstance(body["stocks"], list)
        assert "fetched_at_utc" in body

    def test_stock_record_fields(self, client):
        """Each stock dict must contain the required fields."""
        html = _make_finviz_html([SAMPLE_ROWS[0]])  # BIGCAP only
        adr_df = _make_yf_df(["BIGCAP"])

        with (
            patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
            patch("yfinance.download", return_value=adr_df),
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(side_effect=[
                _make_mock_response(html),
                _make_mock_response("<html><table></table></html>"),
            ])
            mock_client_cls.return_value = mock_client

            r = client.get("/api/market-breadth/stocks?filter=up4&min_cap_b=1.0")

        body = r.json()
        assert body["count"] >= 1
        stock = body["stocks"][0]
        for field in ("ticker", "company", "market_cap_b", "price", "change_pct",
                      "dollar_volume", "adr_pct", "industry"):
            assert field in stock, f"Missing field: {field}"

    def test_default_min_cap_is_one_billion(self, client):
        """When min_cap_b is omitted, default of 1.0 is used (SMACAP filtered out)."""
        html = _make_finviz_html(SAMPLE_ROWS)
        adr_df = _make_yf_df(["BIGCAP", "MIDCAP"])

        with (
            patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
            patch("yfinance.download", return_value=adr_df),
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(side_effect=[
                _make_mock_response(html),
                _make_mock_response("<html><table></table></html>"),
            ])
            mock_client_cls.return_value = mock_client

            r = client.get("/api/market-breadth/stocks?filter=up4")

        tickers = [s["ticker"] for s in r.json()["stocks"]]
        assert "SMACAP" not in tickers

    def test_all_filter_keys_accepted(self, client):
        """Every valid filter key should return 200, not 422."""
        valid = ["up4", "dn4", "up25q", "dn25q", "up25m", "dn25m",
                 "up50m", "dn50m", "up13_34", "dn13_34"]
        empty_html = "<html><table></table></html>"
        for fk in valid:
            with (
                patch("breadth_stocks.httpx.AsyncClient") as mock_client_cls,
                patch("yfinance.download", return_value=pd.DataFrame()),
            ):
                mock_client = AsyncMock()
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client.get = AsyncMock(return_value=_make_mock_response(empty_html))
                mock_client_cls.return_value = mock_client

                r = client.get(f"/api/market-breadth/stocks?filter={fk}")
            assert r.status_code == 200, f"Expected 200 for filter={fk}, got {r.status_code}"
