from typing import Any, Dict, List


class OkxRestAccountBalancesMixin:
    async def get_account_balances(self) -> List[Dict[str, Any]]:
        raw = await self._request_private("GET", "/account/balance")
        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []) or []:
            for d in item.get("details", []) or []:
                out.append(
                    {
                        "ccy": d.get("ccy"),
                        "cashBal": self._to_float(d.get("cashBal")),
                        "availBal": self._to_float(d.get("availBal")),
                        "eq": self._to_float(d.get("eq")),
                    }
                )
        return out
