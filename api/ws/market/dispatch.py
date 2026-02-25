from typing import List

from ..common import WSContext
from .bars import handle_bars
from .orderbook import handle_order_book
from .quotes import handle_quotes


async def handle_market_opcode(
    ctx: WSContext,
    msg: dict,
    opcode: str | None,
    req_guid: str | None,
    symbols: List[str],
) -> bool:
    if opcode == "BarsGetAndSubscribe":
        await handle_bars(ctx, msg, req_guid)
        return True

    if opcode == "OrderBookGetAndSubscribe":
        await handle_order_book(ctx, msg, req_guid)
        return True

    if opcode == "QuotesSubscribe":
        await handle_quotes(ctx, msg, req_guid, symbols)
        return True

    return False
