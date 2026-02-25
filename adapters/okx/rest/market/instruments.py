from datetime import datetime, timezone
from typing import Any, List, Optional


class OkxRestMarketInstrumentsMixin:
    async def list_instruments(self, inst_type: str = "SPOT") -> List[dict]:
        raw = await self._request_public(
            path="/public/instruments",
            params={"instType": inst_type},
        )

        def to_opt_float(v: Any) -> Optional[float]:
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        out: List[dict] = []
        for item in raw.get("data", []):
            inst_type_item = item.get("instType") or inst_type
            ct_val = to_opt_float(item.get("ctVal"))
            exp_ms = self._to_int(item.get("expTime"))
            cancellation = (
                datetime.fromtimestamp(exp_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if exp_ms > 0 else None
            )
            out.append(
                {
                    "symbol": item.get("instId") or "",
                    "exchange": "OKX",
                    "instType": inst_type_item,
                    "type": inst_type_item,
                    "state": item.get("state"),
                    "baseCcy": item.get("baseCcy") or item.get("ctValCcy"),
                    "settleCcy": item.get("settleCcy"),
                    "quoteCcy": item.get("quoteCcy") or item.get("settleCcy"),
                    "ctVal": ct_val,
                    "ctValCcy": item.get("ctValCcy"),
                    "facevalue": ct_val if str(inst_type_item).upper() in ("FUTURES", "SWAP") else None,
                    "cancellation": cancellation,
                    "lotSz": to_opt_float(item.get("lotSz")),
                    "tickSz": to_opt_float(item.get("tickSz")),
                }
            )
        return out
