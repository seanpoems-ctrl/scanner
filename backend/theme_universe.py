from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yfinance as yf

from backend.scraper import fetch_finviz_tickers_deterministic

DATA_DIR = Path(__file__).resolve().parent / "data"
UNIVERSE_PATH = DATA_DIR / "theme_universe.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class ThemeUniverseTheme:
    slug: str
    label: str
    bucket: str = "Themes"
    tickers: list[str] = field(default_factory=list)
    source: dict[str, Any] | None = None
    notes: str | None = None
    updated_at_utc: str | None = None


class ThemeUniverseStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or UNIVERSE_PATH
        self._lock = asyncio.Lock()
        self._themes: list[ThemeUniverseTheme] = []
        self._updated_at_utc: str | None = None
        self._movers_cache: dict[str, dict] = {}

    async def load(self) -> None:
        async with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                self._themes = []
                self._updated_at_utc = _utc_now_iso()
                await self._save_locked()
                return
            try:
                raw = self.path.read_text(encoding="utf-8").strip()
            except Exception:
                raw = ""
            if not raw:
                self._themes = []
                self._updated_at_utc = _utc_now_iso()
                await self._save_locked()
                return
            try:
                data = json.loads(raw)
            except Exception:
                data = {}
            rows = data.get("themes") if isinstance(data, dict) else []
            parsed: list[ThemeUniverseTheme] = []
            if isinstance(rows, list):
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    slug = str(r.get("slug") or "").strip()
                    label = str(r.get("label") or "").strip()
                    if not slug or not label:
                        continue
                    tickers = []
                    for t in (r.get("tickers") or []):
                        s = str(t or "").strip().upper()
                        if s and s not in tickers:
                            tickers.append(s)
                    parsed.append(
                        ThemeUniverseTheme(
                            slug=slug,
                            label=label,
                            bucket=str(r.get("bucket") or "Themes"),
                            tickers=tickers,
                            source=r.get("source") if isinstance(r.get("source"), dict) else None,
                            notes=str(r.get("notes")) if r.get("notes") is not None else None,
                            updated_at_utc=str(r.get("updated_at_utc")) if r.get("updated_at_utc") else None,
                        )
                    )
            self._themes = parsed
            self._updated_at_utc = str(data.get("updated_at_utc")) if isinstance(data, dict) and data.get("updated_at_utc") else _utc_now_iso()

    async def _save_locked(self) -> None:
        payload = {
            "updated_at_utc": self._updated_at_utc or _utc_now_iso(),
            "themes": [asdict(t) for t in self._themes],
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    async def list_themes(self) -> list[ThemeUniverseTheme]:
        async with self._lock:
            return [
                ThemeUniverseTheme(
                    slug=t.slug,
                    label=t.label,
                    bucket=t.bucket,
                    tickers=list(t.tickers),
                    source=dict(t.source) if isinstance(t.source, dict) else None,
                    notes=t.notes,
                    updated_at_utc=t.updated_at_utc,
                )
                for t in self._themes
            ]

    async def find_by_label(self, label: str) -> ThemeUniverseTheme | None:
        q = " ".join((label or "").strip().lower().split())
        if not q:
            return None
        async with self._lock:
            for t in self._themes:
                if " ".join(t.label.lower().split()) == q:
                    return ThemeUniverseTheme(
                        slug=t.slug,
                        label=t.label,
                        bucket=t.bucket,
                        tickers=list(t.tickers),
                        source=dict(t.source) if isinstance(t.source, dict) else None,
                        notes=t.notes,
                        updated_at_utc=t.updated_at_utc,
                    )
        return None

    async def get_cached_movers(self, theme: ThemeUniverseTheme) -> dict | None:
        return self._movers_cache.get(theme.slug)

    async def refresh_movers(self, theme: ThemeUniverseTheme) -> dict:
        tickers = [str(t or "").strip().upper() for t in (theme.tickers or []) if str(t or "").strip()]
        tickers = list(dict.fromkeys(tickers))[:60]
        rows: list[dict[str, Any]] = []
        if tickers:
            rows = await asyncio.to_thread(_snapshot_today_returns, tickers)
        rows.sort(key=lambda x: float(x.get("today_return_pct") or 0.0), reverse=True)
        payload = {
            "label": theme.label,
            "slug": theme.slug,
            "updated_at": _utc_now_iso(),
            "best": rows[:8],
            "worst": sorted(rows, key=lambda x: float(x.get("today_return_pct") or 0.0))[:8],
        }
        self._movers_cache[theme.slug] = payload
        return payload

    async def refresh_all_movers(self) -> dict:
        themes = await self.list_themes()
        refreshed = 0
        for th in themes:
            if not th.tickers:
                continue
            try:
                await self.refresh_movers(th)
                refreshed += 1
            except Exception:
                # best-effort only
                continue
        return {"refreshed": refreshed, "total_themes": len(themes), "updated_at_utc": _utc_now_iso()}

    async def rebuild_all_tickers(self) -> dict:
        async with self._lock:
            updated = 0
            for t in self._themes:
                src = t.source if isinstance(t.source, dict) else None
                if not src or str(src.get("type") or "") != "finviz_screener":
                    continue
                path = str(src.get("path") or "").strip()
                if not path:
                    continue
                max_pages = src.get("max_pages")
                try:
                    mp = int(max_pages) if max_pages is not None else 10
                except Exception:
                    mp = 10
                mp = max(1, min(mp, 40))
                try:
                    tickers = await fetch_finviz_tickers_deterministic(path, max_pages=mp)
                except Exception:
                    tickers = []
                normalized = list(dict.fromkeys([str(x).strip().upper() for x in tickers if str(x).strip()]))
                t.tickers = normalized
                t.updated_at_utc = _utc_now_iso()
                updated += 1
            self._updated_at_utc = _utc_now_iso()
            await self._save_locked()
            return {"updated": updated, "total": len(self._themes), "updated_at": self._updated_at_utc}

    async def upsert_themes(self, upserts: list[dict]) -> dict:
        added = 0
        updated = 0
        async with self._lock:
            by_slug = {t.slug: t for t in self._themes}
            for row in upserts:
                if not isinstance(row, dict):
                    continue
                slug = str(row.get("slug") or "").strip()
                label = str(row.get("label") or "").strip()
                if not slug or not label:
                    continue
                bucket = str(row.get("bucket") or "Themes")
                source = row.get("source") if isinstance(row.get("source"), dict) else None
                existing = by_slug.get(slug)
                if existing is None:
                    th = ThemeUniverseTheme(
                        slug=slug,
                        label=label,
                        bucket=bucket,
                        tickers=[],
                        source=source,
                        updated_at_utc=_utc_now_iso(),
                    )
                    self._themes.append(th)
                    by_slug[slug] = th
                    added += 1
                else:
                    existing.label = label
                    existing.bucket = bucket
                    existing.source = source
                    existing.updated_at_utc = _utc_now_iso()
                    updated += 1
            self._updated_at_utc = _utc_now_iso()
            await self._save_locked()
            return {"added": added, "updated": updated, "total": len(self._themes), "updated_at": self._updated_at_utc}


def _snapshot_today_returns(tickers: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in tickers:
        try:
            tk = yf.Ticker(t)
            hist = tk.history(period="5d", interval="1d")
            if hist is None or hist.empty or len(hist) < 2:
                continue
            close = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2])
            if prev == 0:
                continue
            ret = ((close - prev) / prev) * 100.0
            out.append({"ticker": t, "today_return_pct": round(ret, 2)})
        except Exception:
            continue
    return out


async def scheduled_refresh_loop(store: ThemeUniverseStore, every_sec: int = 30 * 60) -> None:
    # Keep a warm movers cache for spotlight API.
    await asyncio.sleep(1.0)
    while True:
        try:
            await store.refresh_all_movers()
        except asyncio.CancelledError:
            raise
        except Exception:
            # never crash loop
            pass
        await asyncio.sleep(max(60, int(every_sec)))

