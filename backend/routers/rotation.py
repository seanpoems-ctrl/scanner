from __future__ import annotations

from fastapi import APIRouter, Query

from store.rotation_store import get_snapshot

router = APIRouter(prefix="/api/rotation", tags=["rotation"])


@router.get("/snapshot")
async def rotation_snapshot(days: int = Query(default=10, ge=2, le=30)):
    """Last `days` of theme RS history, sorted by rs_delta (momentum) descending."""
    return get_snapshot(days=days)
