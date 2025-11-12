from pydantic import BaseModel
from typing import Optional, List, Literal, Tuple
import time

Side = Literal["buy", "sell"]
OrderType = Literal["limit", "market", "stop", "stop_limit"]
TimeInForce = Literal["GTC", "IOC", "FOK"]

class Instrument(BaseModel):
    id: str
    symbol: str
    base: str
    quote: str
    lotSize: float
    tickSize: float

class Quote(BaseModel):
    symbol: str
    bid: Optional[float]
    ask: Optional[float]
    ts: int

class OrderBook(BaseModel):
    symbol: str
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]
    ts: int

class Trade(BaseModel):
    symbol: str
    price: float
    size: float
    side: Side
    ts: int

class Order(BaseModel):
    id: str
    symbol: str
    side: Side
    type: OrderType
    price: Optional[float] = None
    quantity: float
    status: Literal["new", "partially_filled", "filled", "canceled", "rejected"]
    filledQuantity: float = 0.0
    ts: int

class AccountInfo(BaseModel):
    balances: List[dict]
    ts: int

class Position(BaseModel):
    symbol: str
    qty: float
    avgPrice: Optional[float] = None
    pnl: Optional[float] = None
    ts: int

def now_ms() -> int:
    return int(time.time() * 1000)


#ДОБАВИЛИ

from pydantic import BaseModel
from typing import Optional, List, Tuple, Literal

class BarSlim(BaseModel):      # свеча для /history
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float

class QuoteSlim(BaseModel):    # котировки (best bid/ask)
    symbol: str
    bid: Optional[float]
    ask: Optional[float]
    ts: int

class BookSlim(BaseModel):     # стакан
    symbol: str
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]
    ts: int

class TradeSlim(BaseModel):    # сделки (лента)
    id: Optional[str] = None   # идентификатор сделки (рекомендовано для Astras)
    symbol: str
    price: float
    qty: float                 # ОБЯЗАТЕЛЬНО: Astras ждёт qty (а не size)
    side: Literal["buy","sell"]
    ts: int


class InstrumentAlor(BaseModel):  # минимально совместимо с Astras/ALOR
    symbol: str            # "BTC-USDT"
    exchange: str = "MOCK" # поменяем на "OKX" в реальном адаптере
    description: str       # "BTC/USDT"
    lotSize: float
    tickSize: float
    type: str = "SPOT"


from pydantic import BaseModel
from typing import Optional

class PortfolioTrade(BaseModel):
    id: str
    orderno: str
    comment: Optional[str] = None
    symbol: str
    brokerSymbol: str
    exchange: str
    date: str                 # ISO8601 '2023-12-29T12:35:06.0000000Z'
    board: str
    qtyUnits: int
    qtyBatch: int
    qty: int
    price: float
    accruedInt: float
    side: str                 # "buy" | "sell"
    existing: bool
    commission: float
    repoSpecificFields: Optional[dict] = None
    volume: float
