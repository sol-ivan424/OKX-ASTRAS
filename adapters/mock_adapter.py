import time
import asyncio
import random
from typing import Callable, List, Dict, Any

from api.schemas import (
    QuoteSlim, BookSlim, TradeSlim, BarSlim,
    Order, AccountInfo, Position
)

def now_ms() -> int:
    return int(time.time() * 1000)


class MockAdapter:
    def __init__(self):
        self._orders: Dict[str, Order] = {}

        # Текущее состояние инструментов (ALOR/Astras Slim-совместимое)
        self._inst_state: Dict[str, Dict[str, Any]] = {
            "BTC-USDT": {
                # основные поля
                "symbol": "BTC-USDT",
                "exchange": "MOCK",
                "board": "SPOT",
                "tradingStatus": 17,
                "tradingStatusInfo": "нормальный период торгов",
                "priceMin": 100.0,
                "priceMax": 200.0,
            },
            "ETH-USDT": {
                "symbol": "ETH-USDT",
                "exchange": "MOCK",
                "board": "SPOT",
                "tradingStatus": 17,
                "tradingStatusInfo": "нормальный период торгов",
                "priceMin": 50.0,
                "priceMax": 100.0,
            },
        }

    # ---------- INSTRUMENTS ----------
    async def list_instruments(self) -> List[dict]:
        """Возвращает текущий полный срез инструментов."""
        return list(self._inst_state.values())

    async def stream_instruments(
        self,
        symbols: List[str],
        on_data: Callable[[dict], asyncio.Future],
        stop_event: asyncio.Event,
    ) -> None:
        """
        1) Отправляет полный объект по каждому инструменту.
        2) Затем периодически отправляет дельты (только при изменениях).
        """
        syms = symbols or list(self._inst_state.keys())

        # --- шаг 1: полный срез (FULL)
        for s in syms:
            full = dict(self._inst_state[s])
            await on_data(full)
            await asyncio.sleep(0.3)  # небольшая пауза, чтобы различить пакеты

        # --- шаг 2: дельты (только если значения реально изменились)
        async def _pump(sym: str):
            while not stop_event.is_set():
                cur = self._inst_state[sym]

                # моделируем случайное изменение цен
                delta = random.choice([-0.2, -0.1, 0.1, 0.2])
                new_min = round(cur["priceMin"] + delta, 2)
                new_max = round(cur["priceMax"] + delta, 2)

                changed: Dict[str, Any] = {}
                if new_min != cur["priceMin"]:
                    changed["priceMin"] = new_min
                if new_max != cur["priceMax"]:
                    changed["priceMax"] = new_max

                # если что-то изменилось — обновляем и шлём
                if changed:
                    cur.update(changed)
                    await on_data({"symbol": sym, **changed})

                await asyncio.sleep(2)  # задержка между обновлениями

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- QUOTES / BOOK / TRADES ----------
    async def subscribe_quotes(self, symbols: List[str], on_data: Callable[[QuoteSlim], None]) -> None:
        ts = now_ms()
        for s in symbols:
            on_data(QuoteSlim(symbol=s, bid=100.0, ask=101.0, ts=ts))

    async def subscribe_order_book(self, symbols: List[str], on_data: Callable[[BookSlim], None]) -> None:
        ts = now_ms()
        for s in symbols:
            on_data(BookSlim(symbol=s, bids=[(100.0, 1.0)], asks=[(101.0, 1.2)], ts=ts))

    async def subscribe_trades(self, symbols: List[str], on_data: Callable[[TradeSlim], None]) -> None:
        ts = now_ms()
        for s in symbols:
            on_data(TradeSlim(symbol=s, price=100.5, size=0.01, side="buy", ts=ts))

    # ---------- ORDERS / ACCOUNT / POSITIONS ----------
    async def place_order(self, o: Order) -> Order:
        oid = f"mock-{len(self._orders) + 1}"
        created = Order(
            id=oid, symbol=o.symbol, side=o.side, type=o.type, price=o.price,
            quantity=o.quantity, status="new", filledQuantity=0.0, ts=now_ms()
        )
        self._orders[oid] = created
        return created

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        o = self._orders.get(order_id)
        if not o:
            o = Order(
                id=order_id, symbol=symbol, side="buy", type="limit",
                price=None, quantity=0.0, status="canceled", filledQuantity=0.0, ts=now_ms()
            )
        else:
            o.status = "canceled"
        return o

    async def get_account_info(self) -> AccountInfo:
        return AccountInfo(balances=[{"asset": "USDT", "free": 1000.0, "locked": 0.0}], ts=now_ms())

    async def get_positions(self) -> List[Position]:
        return [Position(symbol="BTC-USDT", qty=0.0, avgPrice=None, pnl=0.0, ts=now_ms())]

    # ---------- HISTORY ----------
    async def get_history(self, symbol: str, tf: str, limit: int) -> list:
        """Исторические бары в формате ALOR: t, c, o, h, l, v (в секундах)."""
        base_s = int(time.time() // 60) * 60
        bars = []
        for i in range(limit):
            t = base_s - (limit - 1 - i) * 60
            bars.append({
                "t": t,
                "c": 101.0,
                "o": 100.0,
                "h": 102.0,
                "l": 99.0,
                "v": 12.3
            })
        return bars
