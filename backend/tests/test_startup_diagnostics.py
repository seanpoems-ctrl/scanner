"""Lightweight API smoke tests (no network)."""

from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_startup_diagnostics_shape():
    r = client.get("/api/startup-diagnostics")
    assert r.status_code == 200
    body = r.json()
    assert "ok" in body
    assert isinstance(body["ok"], bool)
    assert "errors" in body
    assert isinstance(body["errors"], list)
