from adapters.okx.ws.subscriptions_portfolio.orders import OkxWsOrdersSubscriptionMixin
from adapters.okx.ws.subscriptions_portfolio.positions import OkxWsPositionsSubscriptionMixin
from adapters.okx.ws.subscriptions_portfolio.summaries import OkxWsSummariesSubscriptionMixin
from adapters.okx.ws.subscriptions_portfolio.trades import OkxWsTradesSubscriptionMixin


class OkxWsPortfolioSubscriptionsMixin(
    OkxWsOrdersSubscriptionMixin,
    OkxWsTradesSubscriptionMixin,
    OkxWsPositionsSubscriptionMixin,
    OkxWsSummariesSubscriptionMixin,
):
    pass


__all__ = ["OkxWsPortfolioSubscriptionsMixin"]
