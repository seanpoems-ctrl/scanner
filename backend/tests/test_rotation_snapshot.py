"""Rotation snapshot API (empty store)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("ROTATION_STORE_PATH", str(tmp_path / "rotation_history.json"))
    return TestClient(app)


def test_rotation_snapshot_empty(client: TestClient):
    r = client.get("/api/rotation/snapshot?days=7")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["days"] == 7
    assert body["themes"] == []
    assert "generated_at_utc" in body
