from adapters.okx.rest.orders.orders import OkxRestOrderMethodsMixin
from adapters.okx.rest.orders.parsers import OkxRestOrderParsersMixin
from adapters.okx.rest.orders.risk import OkxRestOrderRiskMixin
from adapters.okx.rest.orders.trades import OkxRestOrderTradesMixin


class OkxRestOrdersMixin(
    OkxRestOrderParsersMixin,
    OkxRestOrderMethodsMixin,
    OkxRestOrderRiskMixin,
    OkxRestOrderTradesMixin,
):
    pass


__all__ = ["OkxRestOrdersMixin"]
