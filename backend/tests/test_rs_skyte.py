"""RS skyte CSV proxy — smoke test with mocked fetch."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app


SAMPLE_IND = "Rank,Industry,Sector,Relative Strength,Percentile,1 Month Ago,3 Months Ago,6 Months Ago,Tickers\n1,Test Industry,Technology,100.5,88,80,70,60,\"AAA,BBB\"\n"


@pytest.fixture
def client():
    return TestClient(app)


def test_rs_skyte_industries_mocked(monkeypatch, client: TestClient):
    import rs_skyte as mod

    async def fake_get():
        rows = mod._parse_industries_csv(SAMPLE_IND)
        return {
            "ok": True,
            "source": "skyte/rs-log",
            "kind": "industries",
            "count": len(rows),
            "rows": rows,
            "ttl_seconds": mod.TTL_SEC,
            "cache_hit": False,
            "fetched_at_utc": "1970-01-01T00:00:00+00:00",
        }

    monkeypatch.setattr("routers.rs_skyte.get_industries", fake_get)
    r = client.get("/api/rs/skyte/industries")
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j["count"] == 1
    assert j["rows"][0]["industry"] == "Test Industry"
    assert j["rows"][0]["percentile"] == 88


def test_rs_skyte_stocks_mocked(monkeypatch, client: TestClient):
    async def fake_stocks():
        return {"ok": True, "source": "skyte/rs-log", "kind": "stocks", "count": 0, "rows": [], "ttl_seconds": 900, "cache_hit": False, "fetched_at_utc": "1970-01-01T00:00:00+00:00"}

    monkeypatch.setattr("routers.rs_skyte.get_stocks", fake_stocks)
    r = client.get("/api/rs/skyte/stocks")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["rows"] == []
