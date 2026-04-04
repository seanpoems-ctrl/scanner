"""Watchlist API tests (isolated JSON path via env)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHLIST_STORE_PATH", str(tmp_path / "watchlist.json"))
    return TestClient(app)


def test_watchlist_crud(client: TestClient):
    r = client.get("/api/watchlist")
    assert r.status_code == 200
    assert r.json()["items"] == []

    r = client.post("/api/watchlist", json={"ticker": "nvda", "theme": "AI"})
    assert r.status_code == 200
    assert r.json()["added"] is True
    assert r.json()["ticker"] == "NVDA"

    r = client.post("/api/watchlist", json={"ticker": "NVDA"})
    assert r.status_code == 200
    assert r.json()["added"] is False

    r = client.get("/api/watchlist")
    assert r.status_code == 200
    assert len(r.json()["items"]) == 1

    r = client.patch("/api/watchlist/NVDA/note", json={"note": "Watching for EP"})
    assert r.status_code == 200
    assert r.json()["updated"] is True

    r = client.delete("/api/watchlist/NVDA")
    assert r.status_code == 200
    assert r.json()["removed"] is True

    r = client.get("/api/watchlist")
    assert r.status_code == 200
    assert r.json()["items"] == []
