import time
from typing import Dict, Optional


class OkxInstIdCodeMixin:
    async def _resolve_inst_id_code(self, symbol: str, inst_type: str) -> Optional[str]:

        sym = str(symbol or "").strip()
        if not sym:
            return None

        inst_type_u = str(inst_type or "SPOT").upper().strip() or "SPOT"
        await self._ensure_inst_id_code_cache()

        key = f"{inst_type_u}:{sym}"
        v = self._inst_id_code_cache.get(key)
        if v:
            return v

        # Инструмент мог появиться после последнего refresh — пробуем один раз обновить кэш.
        await self._refresh_inst_id_code_cache()
        return self._inst_id_code_cache.get(key)

    async def _refresh_inst_id_code_cache(self) -> None:
        new_cache: Dict[str, str] = {}
        for one_type in ("SPOT", "FUTURES", "SWAP"):
            raw = await self._request_public(
                path="/public/instruments",
                params={"instType": one_type},
            )
            for it in raw.get("data") or []:
                inst_id = str((it or {}).get("instId") or "").strip()
                inst_id_code = (it or {}).get("instIdCode")
                if not inst_id:
                    continue
                if inst_id_code is None or str(inst_id_code).strip() == "":
                    continue
                new_cache[f"{one_type}:{inst_id}"] = str(inst_id_code)

        self._inst_id_code_cache = new_cache
        self._inst_id_code_cache_ts = time.time()

    async def _ensure_inst_id_code_cache(self) -> None:
        if self._inst_id_code_cache:
            return

        async with self._inst_id_code_cache_lock:
            if self._inst_id_code_cache:
                return
            await self._refresh_inst_id_code_cache()

