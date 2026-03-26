from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from backend.scraper import fetch_finviz_tickers_deterministic

UNIVERSE_PATH = os.path.join(os.path.dirname(__file__), "data", "theme_universe.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_label(s: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else " " for ch in (s or ""))
    return " ".join(out.split()).strip()


def _slugify(s: str) -> str:
    norm = _normalize_label(s)
    return "-".join(norm.split())[:80] or "theme"


@dataclass(frozen=True, slots=True)
class ThemeUniverseTheme:
    slug: str
    label: str
    bucket: str
    tickers: list[str]
    # Optional: deterministic regeneration source (enables auto add/remove).
    source: dict[str, Any] | None = None


class ThemeUniverseStore:
    """
    Persisted theme universe + in-memory movers cache.

    Full auto add/remove requires each theme to have a deterministic `source`.
    Without `source`, scheduled updates will still refresh movers for the tickers listed.
    """

    def __init__(self, path: str = UNIVERSE_PATH) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._raw: dict[str, Any] = {}
        self._themes: list[ThemeUniverseTheme] = []
        self._movers_cache: dict[str, dict[str, Any]] = {}
        self._loaded = False

    async def load(self) -> None:
        async with self._lock:
            if self._loaded:
                return
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            if not os.path.exists(self.path):
                self._raw = {"updated_at": _utc_now_iso(), "themes": []}
                self._themes = []
                self._write_unlocked()
            else:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._raw = json.load(f)
                self._themes = [self._parse_theme(t) for t in self._raw.get("themes", [])]
            self._loaded = True

    def _write_unlocked(self) -> None:
        self._raw["updated_at"] = _utc_now_iso()
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._raw, f, indent=2, sort_keys=False)

    @staticmethod
    def _parse_theme(obj: dict[str, Any]) -> ThemeUniverseTheme:
        slug = str(obj.get("slug") or "").strip()
        label = str(obj.get("label") or "").strip()
        bucket = str(obj.get("bucket") or "").strip()
        tickers = [str(t).upper().strip() for t in (obj.get("tickers") or []) if str(t).strip()]
        source = obj.get("source")
        return ThemeUniverseTheme(slug=slug, label=label, bucket=bucket, tickers=tickers, source=source)

    async def list_themes(self) -> list[ThemeUniverseTheme]:
        await self.load()
        return list(self._themes)

    async def find_by_label(self, label: str) -> ThemeUniverseTheme | None:
        await self.load()
        want = _normalize_label(label)
        for t in self._themes:
            if _normalize_label(t.label) == want:
                return t
        return None

    async def refresh_movers(self, theme: ThemeUniverseTheme) -> dict[str, Any]:
        """
        Compute top/bottom movers by today_return_pct for this theme's tickers.
        """
        await self.load()
        tickers = [t for t in theme.tickers if t]
        if not tickers:
            payload = {"label": theme.label, "slug": theme.slug, "updated_at": _utc_now_iso(), "best": [], "worst": []}
            async with self._lock:
                self._movers_cache[theme.slug] = payload
            return payload

        def _download() -> Any:
            return yf.download(
                tickers=tickers,
                period="5d",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )

        df = await asyncio.to_thread(_download)

        movers: list[dict[str, Any]] = []
        for tkr in tickers:
            try:
                closes = df["Close"].dropna() if len(tickers) == 1 else df[tkr]["Close"].dropna()
                if len(closes) < 2:
                    continue
                close = float(closes.iloc[-1])
                prev = float(closes.iloc[-2])
                if prev <= 0:
                    continue
                chg = ((close - prev) / prev) * 100.0
                movers.append({"ticker": tkr, "today_return_pct": round(chg, 2)})
            except Exception:
                continue

        movers.sort(key=lambda x: x["today_return_pct"])
        worst = movers[:3]
        best = list(reversed(movers[-3:]))
        payload = {"label": theme.label, "slug": theme.slug, "updated_at": _utc_now_iso(), "best": best, "worst": worst}
        async with self._lock:
            self._movers_cache[theme.slug] = payload
        return payload

    async def get_cached_movers(self, theme: ThemeUniverseTheme) -> dict[str, Any] | None:
        await self.load()
        async with self._lock:
            return self._movers_cache.get(theme.slug)

    async def refresh_all_movers(self) -> dict[str, Any]:
        """
        Refresh movers for every theme (tickers-only automation).
        """
        await self.load()
        themes = list(self._themes)
        results = await asyncio.gather(*(self.refresh_movers(t) for t in themes))
        return {"updated_at": _utc_now_iso(), "count": len(results)}

    async def rebuild_tickers(self, theme: ThemeUniverseTheme) -> dict[str, Any]:
        """
        Deterministically rebuild `tickers` for a theme from its `source`.
        Supported sources:
          - {"type": "finviz_screener", "path": "/screener.ashx?..."}
        """
        await self.load()
        src = theme.source or {}
        if not isinstance(src, dict):
            return {"slug": theme.slug, "label": theme.label, "rebuilt": False, "reason": "invalid source"}
        if src.get("type") != "finviz_screener":
            return {"slug": theme.slug, "label": theme.label, "rebuilt": False, "reason": "unsupported source type"}

        path = str(src.get("path") or "").strip()
        if not path:
            return {"slug": theme.slug, "label": theme.label, "rebuilt": False, "reason": "missing source.path"}

        max_pages = src.get("max_pages")
        try:
            max_pages_i = int(max_pages) if max_pages is not None else 10
        except Exception:
            max_pages_i = 10
        max_pages_i = max(1, min(50, max_pages_i))

        tickers = await fetch_finviz_tickers_deterministic(path, max_pages=max_pages_i)

        async with self._lock:
            raw_themes = self._raw.get("themes", [])
            if not isinstance(raw_themes, list):
                return {"slug": theme.slug, "label": theme.label, "rebuilt": False, "reason": "bad universe format"}
            updated = False
            for obj in raw_themes:
                if not isinstance(obj, dict):
                    continue
                if str(obj.get("slug") or "").strip() != theme.slug:
                    continue
                obj["tickers"] = tickers
                updated = True
                break
            if updated:
                self._themes = [self._parse_theme(x) for x in raw_themes if isinstance(x, dict)]
                self._write_unlocked()
        return {"slug": theme.slug, "label": theme.label, "rebuilt": True, "tickers": len(tickers)}

    async def rebuild_all_tickers(self) -> dict[str, Any]:
        await self.load()
        themes = [t for t in self._themes if isinstance(t.source, dict) and t.source.get("type")]
        sem = asyncio.Semaphore(2)

        async def run_one(t: ThemeUniverseTheme) -> dict[str, Any]:
            async with sem:
                try:
                    return await self.rebuild_tickers(t)
                except Exception as e:
                    return {"slug": t.slug, "label": t.label, "rebuilt": False, "reason": str(e)}

        results = await asyncio.gather(*(run_one(t) for t in themes))
        rebuilt = sum(1 for r in results if r.get("rebuilt"))
        failed = sum(1 for r in results if not r.get("rebuilt"))
        return {
            "updated_at": _utc_now_iso(),
            "themes_with_source": len(themes),
            "rebuilt": rebuilt,
            "failed": failed,
        }

    async def upsert_themes(self, themes: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Merge themes into `theme_universe.json` by `slug`.
        Preserves existing tickers/source when the incoming object omits them.
        """
        await self.load()
        async with self._lock:
            existing = {str(t.get("slug") or ""): t for t in self._raw.get("themes", []) if isinstance(t, dict)}
            added = 0
            updated = 0
            for t in themes:
                slug = str(t.get("slug") or "").strip() or _slugify(str(t.get("label") or ""))
                incoming = {
                    "slug": slug,
                    "label": str(t.get("label") or "").strip(),
                    "bucket": str(t.get("bucket") or "").strip(),
                    "tickers": t.get("tickers"),
                    "source": t.get("source"),
                }
                prior = existing.get(slug)
                if prior is None:
                    existing[slug] = {
                        "slug": slug,
                        "label": incoming["label"],
                        "bucket": incoming["bucket"],
                        "tickers": incoming["tickers"] if isinstance(incoming["tickers"], list) else [],
                        "source": incoming["source"] if isinstance(incoming["source"], dict) else None,
                    }
                    added += 1
                    continue

                # Update label/bucket always; keep tickers/source unless explicitly provided.
                prior["label"] = incoming["label"] or prior.get("label")
                prior["bucket"] = incoming["bucket"] or prior.get("bucket")
                if isinstance(incoming["tickers"], list):
                    prior["tickers"] = incoming["tickers"]
                if isinstance(incoming["source"], dict) or incoming["source"] is None:
                    if "source" in t:
                        prior["source"] = incoming["source"]
                updated += 1

            merged = list(existing.values())
            merged.sort(key=lambda x: (str(x.get("bucket") or ""), str(x.get("label") or "")))
            self._raw["themes"] = merged
            self._themes = [self._parse_theme(x) for x in merged]
            self._write_unlocked()
            return {"added": added, "updated": updated, "total": len(merged), "updated_at": self._raw["updated_at"]}


async def scheduled_refresh_loop(store: ThemeUniverseStore, every_sec: float) -> None:
    """
    Simple in-process scheduler. Runs forever.

    Note: this keeps movers fresh; auto add/remove requires you to populate theme `source` rules.
    """
    await store.load()
    while True:
        try:
            await store.rebuild_all_tickers()
            await store.refresh_all_movers()
        except Exception:
            # Keep loop alive; errors show up in server logs.
            pass
        await asyncio.sleep(every_sec)

