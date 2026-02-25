import asyncio
import time
from typing import Optional


_INSTR_CACHE: dict[str, dict] = {}
_INSTR_CACHE_TS: float = 0.0
_INSTR_LOCK = asyncio.Lock()
_INSTR_TTL_SEC = 60.0


def _get_adapter(adapter=None):
    if adapter is not None:
        return adapter
    from api import core
    return core.adapter


def get_instruments_cache() -> dict[str, dict]:
    return _INSTR_CACHE


def get_instruments_cache_ts() -> float:
    return _INSTR_CACHE_TS


def get_instruments_lock():
    return _INSTR_LOCK


def get_instruments_ttl_sec() -> float:
    return _INSTR_TTL_SEC


async def load_ticker_map_for_types(inst_types: list[str], adapter=None) -> dict[str, dict]:
    ad = _get_adapter(adapter)
    results = await asyncio.gather(
        *(ad.list_tickers(inst_type=it) for it in inst_types),
        return_exceptions=True,
    )
    merged: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        if r:
            merged.extend(r)
    return {t.get("symbol"): t for t in merged if t.get("symbol")}


async def refresh_instr_cache(adapter=None) -> None:
    ad = _get_adapter(adapter)
    raw_all: list[dict] = []

    for inst_type in ("SPOT", "FUTURES", "SWAP"):
        try:
            part = await ad.list_instruments(inst_type=inst_type)
            if part:
                raw_all.extend(part)
        except Exception:
            continue

    new_map = {x.get("symbol"): x for x in (raw_all or []) if x.get("symbol")}
    _INSTR_CACHE.clear()
    _INSTR_CACHE.update(new_map)

    global _INSTR_CACHE_TS
    _INSTR_CACHE_TS = time.time()


async def ensure_instr_cache(adapter=None) -> None:
    if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
        return

    async with _INSTR_LOCK:
        if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
            return
        try:
            await refresh_instr_cache(adapter=adapter)
        except Exception:
            return


async def get_instr(symbol: str, adapter=None) -> Optional[dict]:
    if not _INSTR_CACHE:
        await refresh_instr_cache(adapter=adapter)
    return _INSTR_CACHE.get(symbol)
