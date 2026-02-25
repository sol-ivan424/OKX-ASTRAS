from adapters.okx.ws.manage_orders.cancel import OkxWsOrderCancelMixin
from adapters.okx.ws.manage_orders.connection import OkxWsOrderConnectionMixin
from adapters.okx.ws.manage_orders.create import OkxWsOrderCreateMixin
from adapters.okx.ws.manage_orders.transport import OkxWsOrderTransportMixin


class OkxWsPrivateOrdersMixin(
    OkxWsOrderConnectionMixin,
    OkxWsOrderTransportMixin,
    OkxWsOrderCreateMixin,
    OkxWsOrderCancelMixin,
):
    pass


__all__ = ["OkxWsPrivateOrdersMixin"]
