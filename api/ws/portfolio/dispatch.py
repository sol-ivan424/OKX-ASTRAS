from typing import List

from ..common import WSContext
from .orders import handle_orders
from .trades import handle_trades
from .positions import handle_positions
from .summaries import handle_summaries


async def handle_portfolio_opcode(
    ctx: WSContext,
    msg: dict,
    opcode: str | None,
    req_guid: str | None,
    symbols: List[str],
) -> bool:
    if opcode == "OrdersGetAndSubscribeV2":
        await handle_orders(ctx, msg, req_guid, symbols)
        return True

    if opcode == "TradesGetAndSubscribeV2":
        await handle_trades(ctx, msg, req_guid)
        return True

    if opcode == "PositionsGetAndSubscribeV2":
        await handle_positions(ctx, msg, req_guid)
        return True

    if opcode == "SummariesGetAndSubscribeV2":
        await handle_summaries(ctx, msg, req_guid)
        return True

    return False
