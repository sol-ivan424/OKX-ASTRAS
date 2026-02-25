from typing import Any, Dict


class OkxRestAccountSummariesMixin:
    async def get_summaries_snapshot(self) -> Dict[str, Any]:
        raw = await self._request_private("GET", "/account/balance")
        data = raw.get("data") or []
        if not data:
            raise RuntimeError("OKX /account/balance returned empty data")
        return self._parse_okx_account_summary_any(data[0] or {})
