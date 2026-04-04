from __future__ import annotations

from fastapi import APIRouter

from rs_skyte import get_industries, get_stocks

router = APIRouter(prefix="/api/rs/skyte", tags=["rs-skyte"])


@router.get("/industries")
async def rs_skyte_industries():
    """Cached skyte/rs-log industry RS snapshot (CSV via jsDelivr / GitHub)."""
    return await get_industries()


@router.get("/stocks")
async def rs_skyte_stocks():
    """Cached skyte/rs-log per-stock RS snapshot (CSV via jsDelivr / GitHub)."""
    return await get_stocks()
