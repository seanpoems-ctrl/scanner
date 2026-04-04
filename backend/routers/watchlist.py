from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from store.watchlist_store import add_ticker, get_watchlist, remove_ticker, update_note

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class AddTickerRequest(BaseModel):
    ticker: str
    theme: str | None = None
    sector: str | None = None
    grade: str | None = None
    note: str | None = None


class UpdateNoteRequest(BaseModel):
    note: str


@router.get("")
async def get_watchlist_endpoint():
    return {"ok": True, "items": get_watchlist()}


@router.post("")
async def add_ticker_endpoint(body: AddTickerRequest):
    try:
        return add_ticker(
            ticker=body.ticker,
            theme=body.theme,
            sector=body.sector,
            grade=body.grade,
            note=body.note,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.delete("/{ticker}")
async def remove_ticker_endpoint(ticker: str):
    return remove_ticker(ticker)


@router.patch("/{ticker}/note")
async def update_note_endpoint(ticker: str, body: UpdateNoteRequest):
    return update_note(ticker, body.note)
