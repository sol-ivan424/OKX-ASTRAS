from adapters.okx.ws.subscriptions_market.bars import OkxWsBarsSubscriptionMixin
from adapters.okx.ws.subscriptions_market.orderbook import OkxWsOrderBookSubscriptionMixin
from adapters.okx.ws.subscriptions_market.quotes import OkxWsQuotesSubscriptionMixin


class OkxWsMarketSubscriptionsMixin(
    OkxWsBarsSubscriptionMixin,
    OkxWsOrderBookSubscriptionMixin,
    OkxWsQuotesSubscriptionMixin,
):
    pass


__all__ = ["OkxWsMarketSubscriptionsMixin"]
