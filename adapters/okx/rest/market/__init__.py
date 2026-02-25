from adapters.okx.rest.market.bars import OkxRestMarketBarsMixin
from adapters.okx.rest.market.instruments import OkxRestMarketInstrumentsMixin
from adapters.okx.rest.market.orderbook import OkxRestMarketOrderBookMixin
from adapters.okx.rest.market.parsers import OkxRestMarketParsersMixin
from adapters.okx.rest.market.tickers import OkxRestMarketTickersMixin


class OkxRestMarketMixin(
    OkxRestMarketParsersMixin,
    OkxRestMarketInstrumentsMixin,
    OkxRestMarketTickersMixin,
    OkxRestMarketOrderBookMixin,
    OkxRestMarketBarsMixin,
):
    pass


__all__ = ["OkxRestMarketMixin"]
