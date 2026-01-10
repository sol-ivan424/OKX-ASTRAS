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

class BarSlim(BaseModel):      # свеча для /history
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float

# --- КОТИРОВКИ (SLIM) ---

class QuoteSlim(BaseModel):
    # Обязательные поля
    sym: str                  # тикер, например "SBER" или "BTC-USDT"
    ex: Optional[str] = None  # биржа, "MOEX", "SPBX", "MOCK" и т.п.
    desc: Optional[str] = None  # описание инструмента

    tst: int                  # timestamp последних торгов (Unix seconds)
    tso: Optional[int] = None # время начала сессии (если есть)

    # Цены OHLC
    o: Optional[float] = None # open
    h: Optional[float] = None # high
    l: Optional[float] = None # low
    c: Optional[float] = None # close / last

    # Объёмы и открытый интерес
    v: Optional[float] = None     # объём текущей сессии
    acci: Optional[float] = None  # накопленный доход/купон и т.п.
    oi: Optional[float] = None    # open interest
    y: Optional[float] = None     # вчерашняя цена закрытия (или аналог)

    # Стакан по лучшей цене
    ask: Optional[float] = None   # лучшая цена продавца
    bid: Optional[float] = None   # лучшая цена покупателя
    av: Optional[float] = None    # объём на лучшем ask
    bv: Optional[float] = None    # объём на лучшем bid

    # Суммарные объёмы
    tbv: Optional[float] = None   # суммарный bid volume
    tav: Optional[float] = None   # суммарный ask volume

    # Лотность
    lot: Optional[float] = None   # размер лота
    lotv: Optional[float] = None  # объём в лотах (или денежный эквивалент)

    # Прочее
    fv: Optional[float] = None    # шаг цены / face value / etc. (зависит от рынка)
    t: Optional[str] = None       # тип котировки/торговой сессии, например "C5"


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
