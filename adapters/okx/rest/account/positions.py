from typing import Any, Dict, List, Optional


class OkxRestAccountPositionsMixin:
    async def get_positions_snapshot(self, inst_type: Optional[str] = None) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        try:
            bal = await self._request_private("GET", "/account/balance")
            data = bal.get("data") or []
            if data:
                out.extend(self._parse_okx_account_balance_any(data[0] or {}))
        except Exception:
            pass

        for itype in ("MARGIN", "FUTURES", "SWAP"):
            try:
                pos = await self._request_private(
                    "GET",
                    "/account/positions",
                    params={"instType": itype},
                )
                for it in (pos.get("data") or []):
                    out.append(self._parse_okx_position_any(it or {}))
            except Exception:
                continue

        return out
