from abc import ABC, abstractmethod
from typing import Callable, List, Any, Optional

from api.schemas import (
    QuoteSlim, BookSlim, TradeSlim,
    Order, AccountInfo, Position
)


class ExchangeAdapter(ABC):
    # ---------- Instruments ----------
    @abstractmethod
    async def list_instruments(self) -> List[dict]:
        """Snapshot списка инструментов (Simple-совместимый словарь по каждому инструменту)."""
        ...

    @abstractmethod
    async def stream_instruments(
        self,
        symbols: List[str],
        on_data: Callable[[dict], Any],
        stop_event: Any,
    ) -> None:
        """Поток обновлений по инструментам (FULL затем DELTA); on_data получает dict."""
        ...

    # ---------- Market data: Quotes / OrderBook ----------
    @abstractmethod
    async def subscribe_quotes(
        self,
        symbols: List[str],
        on_data: Callable[[QuoteSlim], None],
        stop_event: Optional[Any] = None,
    ) -> None:
        """WS-подписка на котировки (Slim: symbol,bid,ask,ts)."""
        ...

    @abstractmethod
    async def subscribe_order_book(
        self,
        symbols: List[str],
        on_data: Callable[[BookSlim], None],
        stop_event: Optional[Any] = None,
    ) -> None:
        """WS-подписка на стакан (Slim: symbol,bids[[p,q]],asks[[p,q]],ts)."""
        ...

    # ---------- Portfolio executions (fills) ----------
    @abstractmethod
    async def subscribe_fills(
        self,
        symbols: List[str],
        on_data: Callable[[dict], Any],
        stop_event: Any,
    ) -> None:
        """WS-подписка на сделки пользователя (детальная запись как в ALOR TradesGetAndSubscribeV2)."""
        ...

    # ---------- Orders / Positions / Summaries (WS) ----------
    @abstractmethod
    async def subscribe_orders(  # NEW
        self,
        symbols: List[str],
        on_data: Callable[[Order], Any],
        stop_event: Any,
    ) -> None:
        """WS-подписка на заявки пользователя (совместимо с нашей схемой Order)."""
        ...

    @abstractmethod
    async def subscribe_positions(  # NEW
        self,
        symbols: List[str],
        on_data: Callable[[Position], Any],
        stop_event: Any,
    ) -> None:
        """WS-подписка на позиции пользователя (совместимо с нашей схемой Position)."""
        ...

    @abstractmethod
    async def subscribe_summaries(  # NEW
        self,
        on_data: Callable[[dict], Any],
        stop_event: Any,
    ) -> None:
        """WS-подписка на сводку по счёту (balances/cash/equity/pnl/ts — Simple-совместимый dict)."""
        ...

    # ---------- Trading / Portfolio (REST) ----------
    @abstractmethod
    async def place_order(self, o: Order) -> Order:
        """Размещение заявки."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        """Отмена заявки."""
        ...

    @abstractmethod
    async def get_account_info(self) -> AccountInfo:
        """Снимок балансов/счёта."""
        ...

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Снимок позиций."""
        ...

    # ---------- History ----------
    @abstractmethod
    async def get_history(self, symbol: str, tf: str, limit: int) -> list:
        """Исторические свечи (Slim: {t,o,h,l,c,v})."""
        ...

    # ---------- Market trades (AllTrades via REST) ----------
    @abstractmethod
    async def get_all_trades(self, exchange: str, symbol: str, limit: int) -> List[TradeSlim]:  # NEW
        """Рыночная лента сделок через REST (Slim: {id,symbol,price,qty,side,ts})."""
        ...
