import time
import asyncio
import random
import uuid
import datetime
from typing import Callable, List, Dict, Any, Optional

from api.schemas import (
    QuoteSlim, BookSlim, TradeSlim, BarSlim,
    Order, AccountInfo, Position
)

def now_ms() -> int:
    return int(time.time() * 1000)

def iso_now_7() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


class MockAdapter:
    def __init__(self):
        self._orders: Dict[str, Order] = {}
        self._inst_state: Dict[str, Dict[str, Any]] = {
            "BTC-USDT": {
                "symbol": "BTC-USDT", "exchange": "MOCK", "board": "SPOT",
                "tradingStatus": 17, "tradingStatusInfo": "нормальный период торгов",
                "priceMin": 100.0, "priceMax": 200.0,
            },
            "ETH-USDT": {
                "symbol": "ETH-USDT", "exchange": "MOCK", "board": "SPOT",
                "tradingStatus": 17, "tradingStatusInfo": "нормальный период торгов",
                "priceMin": 50.0, "priceMax": 100.0,
            },
        }
        # Портфель/сводка для WS summaries
        self._cash_free = 1_000.0
        self._pnl = 0.0

    # ---------- INSTRUMENTS ----------
    async def list_instruments(self) -> List[dict]:
        return list(self._inst_state.values())

    async def stream_instruments(
        self,
        symbols: List[str],
        on_data: Callable[[dict], asyncio.Future],
        stop_event: asyncio.Event,
    ) -> None:
        syms = symbols or list(self._inst_state.keys())

        # FULL сначала
        for s in syms:
            await on_data(dict(self._inst_state[s]))
            await asyncio.sleep(0.2)

        # DELTA далее
        async def _pump(sym: str):
            while not stop_event.is_set():
                cur = self._inst_state[sym]
                delta = random.choice([-0.2, -0.1, 0.1, 0.2])
                new_min = round(cur["priceMin"] + delta, 2)
                new_max = round(cur["priceMax"] + delta, 2)

                changed: Dict[str, Any] = {}
                if new_min != cur["priceMin"]:
                    changed["priceMin"] = new_min
                if new_max != cur["priceMax"]:
                    changed["priceMax"] = new_max

                if changed:
                    cur.update(changed)
                    await on_data({"symbol": sym, **changed})

                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- QUOTES / BOOK (public) ----------
    async def subscribe_quotes(
        self, symbols: List[str], on_data: Callable[[QuoteSlim], None], stop_event: Optional[asyncio.Event] = None
    ) -> None:
        syms = symbols or list(self._inst_state.keys())

        async def _pump(sym: str):
            while not (stop_event and stop_event.is_set()):
                ts = now_ms()
                bid = round(random.uniform(99, 101), 2)
                ask = round(bid + random.uniform(0.5, 1.5), 2)
                on_data(QuoteSlim(symbol=sym, bid=bid, ask=ask, ts=ts))
                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    async def subscribe_order_book(
        self, symbols: List[str], on_data: Callable[[BookSlim], None], stop_event: Optional[asyncio.Event] = None
    ) -> None:
        syms = symbols or list(self._inst_state.keys())

        async def _pump(sym: str):
            mid = 100.0
            while not (stop_event and stop_event.is_set()):
                ts = now_ms()
                mid += random.uniform(-0.5, 0.5)
                bids = [(round(mid - i*0.1, 2), round(random.uniform(0.5, 2.0), 3)) for i in range(1, 6)]
                asks = [(round(mid + i*0.1, 2), round(random.uniform(0.5, 2.0), 3)) for i in range(1, 6)]
                on_data(BookSlim(symbol=sym, bids=bids, asks=asks, ts=ts))
                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- ПОРТФЕЛЬНЫЕ СДЕЛКИ / EXECUTIONS (fills) ----------
    async def subscribe_fills(
        self, symbols: List[str], on_data: Callable[[dict], Any], stop_event: asyncio.Event
    ) -> None:
        syms = symbols or ["BTC-USDT"]

        def _fill_row(sym: str) -> dict:
            if "-" in sym:
                exchange, board = "MOCK", "SPOT"
            else:
                exchange, board = "MOEX", "TQBR"
            price = round(random.uniform(90, 210), 2)
            qty_units = random.choice([1, 10, 20])
            volume = round(price * qty_units, 2)
            return {
                "id": str(uuid.uuid4().int)[:10],
                "orderno": str(uuid.uuid4().int)[:11],
                "comment": None,
                "symbol": sym,
                "brokerSymbol": f"{exchange}:{sym}",
                "exchange": exchange,
                "date": iso_now_7(),
                "board": board,
                "qtyUnits": qty_units,
                "qtyBatch": 1,
                "qty": 1,
                "price": price,
                "accruedInt": 0.0,
                "side": random.choice(["buy", "sell"]),
                "existing": True,
                "commission": round(volume * 0.0008, 6),
                "repoSpecificFields": None,
                "volume": volume,
            }

        async def _pump(sym: str):
            while not stop_event.is_set():
                await on_data(_fill_row(sym))
                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- ORDERS (WS) ----------
    async def subscribe_orders(
        self, symbols: List[str], on_data: Callable[[Order], Any], stop_event: asyncio.Event
    ) -> None:
        """Эмуляция WS-потока заявок пользователя (Orders). Формат = наша схема Order."""
        syms = symbols or list(self._inst_state.keys())

        async def _pump(sym: str):
            while not stop_event.is_set():
                # случайное событие: новая/частично/отмена/исполнена
                ev = random.choice(["new", "partially_filled", "canceled", "filled"])
                oid = f"mock-{random.randint(1, 9999)}"
                price = round(random.uniform(95, 105), 2)
                qty = round(random.uniform(0.01, 0.2), 4)
                filled = 0.0
                if ev == "partially_filled":
                    filled = round(qty * random.uniform(0.1, 0.9), 4)
                elif ev == "filled":
                    filled = qty

                order = Order(
                    id=oid,
                    symbol=sym,
                    side=random.choice(["buy", "sell"]),
                    type=random.choice(["limit", "market"]),
                    price=price if ev != "market" else None,
                    quantity=qty,
                    status=ev,
                    filledQuantity=filled,
                    ts=now_ms(),
                )
                self._orders[oid] = order
                await on_data(order)
                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- POSITIONS (WS) ----------
    async def subscribe_positions(
        self, symbols: List[str], on_data: Callable[[Position], Any], stop_event: asyncio.Event
    ) -> None:
        """Эмуляция WS-потока позиций. Формат = наша схема Position."""
        syms = symbols or list(self._inst_state.keys())

        async def _pump(sym: str):
            qty = 0.0
            avg = None
            pnl = 0.0
            while not stop_event.is_set():
                # немного колеблем позицию
                delta = round(random.uniform(-0.02, 0.02), 4)
                qty = round(max(0.0, qty + delta), 4)
                if qty > 0 and avg is None:
                    avg = round(random.uniform(95, 105), 2)
                if qty == 0:
                    avg = None
                # PnL как шум около нуля
                pnl = round(pnl + random.uniform(-1, 1), 2)
                await on_data(Position(symbol=sym, qty=qty, avgPrice=avg, pnl=pnl, ts=now_ms()))
                await asyncio.sleep(2)

        for s in syms:
            asyncio.create_task(_pump(s))

    # ---------- SUMMARIES (WS) ----------
    async def subscribe_summaries(
        self, on_data: Callable[[dict], Any], stop_event: asyncio.Event
    ) -> None:
        """Эмуляция WS-потока клиентской сводки (Summaries). Отдаём совместимую структуру:
           balances[] + агрегаты (cash, equity, pnl) в Slim-духе."""
        cash = self._cash_free
        pnl = self._pnl
        while not stop_event.is_set():
            # имитируем небольшие изменения
            pnl = round(pnl + random.uniform(-3, 3), 2)
            cash = round(cash + random.uniform(-5, 5), 2)
            data = {
                "balances": [
                    {"asset": "USDT", "free": max(0.0, cash), "locked": 0.0},
                ],
                "cash": cash,
                "equity": round(cash + pnl, 2),
                "pnl": pnl,
                "ts": now_ms(),
            }
            await on_data(data)
            await asyncio.sleep(3)

    # ---------- ORDERS / ACCOUNT / POSITIONS (REST) ----------
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
        base_s = int(time.time() // 60) * 60
        bars = []
        for i in range(limit):
            t = base_s - (limit - 1 - i) * 60
            bars.append({"t": t, "c": 101.0, "o": 100.0, "h": 102.0, "l": 99.0, "v": 12.3})
        return bars

    # ---------- NEW: рыночная лента через REST-путь (Slim) ----------
    async def get_all_trades(self, exchange: str, symbol: str, limit: int) -> List[TradeSlim]:
        """
        MOCK-реализация. Для реальной интеграции сделать AlorAdapter,
        который вызовет HTTP:
          /md/v2/securities/{exchange}/{symbol}/alltrades?limit=...
        и вернёт Slim-массив [{id, symbol, price, qty, side, ts}, ...]
        """
        out: List[TradeSlim] = []
        for _ in range(limit):
            out.append(
                TradeSlim(
                    id=str(uuid.uuid4().int)[:12],
                    symbol=symbol,
                    price=round(random.uniform(98, 102), 2),
                    qty=round(random.uniform(0.001, 0.05), 4),
                    side=random.choice(["buy", "sell"]),
                    ts=now_ms()
                )
            )
        return out
