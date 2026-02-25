from adapters.okx.auth import OkxAuthMixin
from adapters.okx.http import OkxHttpMixin
from adapters.okx.inst_id_cache import OkxInstIdCodeMixin
from adapters.okx.rest.account import OkxRestAccountMixin
from adapters.okx.rest.market import OkxRestMarketMixin
from adapters.okx.rest.orders import OkxRestOrdersMixin
from adapters.okx.state import OkxStateMixin
from adapters.okx.ws.manage_orders import OkxWsPrivateOrdersMixin
from adapters.okx.ws.subscriptions_market import OkxWsMarketSubscriptionsMixin
from adapters.okx.ws.subscriptions_portfolio import OkxWsPortfolioSubscriptionsMixin


class OkxAdapter(
    OkxStateMixin,
    OkxAuthMixin,
    OkxHttpMixin,
    OkxInstIdCodeMixin,
    OkxRestMarketMixin,
    OkxRestOrdersMixin,
    OkxRestAccountMixin,
    OkxWsPrivateOrdersMixin,
    OkxWsMarketSubscriptionsMixin,
    OkxWsPortfolioSubscriptionsMixin,
):
    async def warmup(self) -> None:
        await self._ensure_order_ws()
        await self._ensure_inst_id_code_cache()

    def tf_to_okx_ws_channel(self, tf: str) -> str:
        return self._tf_to_okx_ws_channel(tf)

    async def get_fills_history_page(
        self,
        inst_type: str,
        limit: int = 100,
        after: str | None = None,
    ) -> tuple[list[dict], str | None]:
        params = {"instType": inst_type, "limit": str(limit)}
        if after:
            params["after"] = after
        raw = await self._request_private("GET", "/trade/fills-history", params=params)
        raw_page = raw.get("data") or []
        parsed_page = [
            self._parse_okx_trade_any(item, is_history=True, inst_type=inst_type)
            for item in raw_page
        ]
        last = raw_page[-1] if raw_page else {}
        next_after = str(last.get("tradeId") or last.get("billId") or "").strip() or None
        return parsed_page, next_after
