import os
import sys

# добавляем корень проекта (Astras-ALOR) в sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest
from fastapi.testclient import TestClient
from api.server import app

client = TestClient(app)


def ws_first_data(opcode_payload: dict) -> dict:
    """Открываем WS, шлём один opcode-запрос, ждём первое сообщение с полем data."""
    with client.websocket_connect("/stream") as ws:
        ws.send_json(opcode_payload)
        msg = ws.receive_json()
        assert "data" in msg, f"no 'data' in response: {msg}"
        return msg["data"]


def base_payload(opcode: str) -> dict:
    """Базовый каркас запроса в стиле Astras."""
    return {
        "opcode": opcode,
        "exchange": "MOEX",
        "format": "Slim",
        "guid": f"test-{opcode}",
        "token": "test-token-123",
    }


@pytest.mark.parametrize("opcode", ["BarsGetAndSubscribe"])
def test_bars_get_and_subscribe_returns_slim_bar(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "code": "BTC-USDT",
            "instrumentGroup": "SPOT",
            "tf": "60",
            "from": 0,
            "skipHistory": False,
            "splitAdjust": True,
            "frequency": 250,
        }
    )

    data = ws_first_data(payload)

    for key in ("t", "o", "h", "l", "c", "v"):
        assert key in data, f"missing '{key}' in bar: {data}"


@pytest.mark.parametrize("opcode", ["QuotesSubscribe"])
def test_quotes_subscribe_returns_quote_slim(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "code": "BTC-USDT",
            "instrumentGroup": "SPOT",
            "frequency": 250,
        }
    )

    data = ws_first_data(payload)

    required_keys = (
        "sym",
        "ex",
        "tst",
        "o",
        "h",
        "l",
        "c",
        "v",
        "ask",
        "bid",
        "av",
        "bv",
        "tbv",
        "tav",
        "lot",
        "lotv",
        "fv",
        "t",
        "acci",
        "oi",
        "y",
    )

    for key in required_keys:
        assert key in data, f"missing '{key}' in QuoteSlim: {data}"


@pytest.mark.parametrize("opcode", ["OrderBookGetAndSubscribe"])
def test_order_book_get_and_subscribe_returns_slim_book(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "code": "BTC-USDT",
            "instrumentGroup": "SPOT",
            "depth": 10,
            "frequency": 250,
        }
    )

    data = ws_first_data(payload)

    for key in ("b", "a", "t", "h"):
        assert key in data, f"missing '{key}' in book: {data}"

    assert isinstance(data["b"], list), "bids must be list"
    assert isinstance(data["a"], list), "asks must be list"
    if data["b"]:
        first_bid = data["b"][0]
        assert "p" in first_bid and "v" in first_bid, f"bid row must contain p, v: {first_bid}"
    if data["a"]:
        first_ask = data["a"][0]
        assert "p" in first_ask and "v" in first_ask, f"ask row must contain p, v: {first_ask}"


@pytest.mark.parametrize("opcode", ["OrdersGetAndSubscribeV2"])
def test_orders_get_and_subscribe_v2_returns_simple_orders(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "portfolio": "D39004",
            "skipHistory": False,
            "orderStatuses": ["new", "partially_filled", "filled", "canceled"],
        }
    )

    data = ws_first_data(payload)

    required_keys = (
        "id",
        "sym",
        "tic",
        "p",
        "ex",
        "cmt",
        "t",
        "s",
        "st",
        "tt",
        "ut",
        "et",
        "q",
        "qb",
        "fq",
        "fqb",
        "px",
        "h",
        "tf",
        "i",
        "v",
    )

    for key in required_keys:
        assert key in data, f"missing '{key}' in Simple order: {data}"


@pytest.mark.parametrize("opcode", ["TradesGetAndSubscribeV2"])
def test_trades_get_and_subscribe_v2_returns_portfolio_trades(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "portfolio": "D39004",
            "skipHistory": False,
        }
    )

    data = ws_first_data(payload)

    required_keys = (
        "id",
        "orderno",
        "comment",
        "symbol",
        "brokerSymbol",
        "exchange",
        "date",
        "board",
        "qtyUnits",
        "qtyBatch",
        "qty",
        "price",
        "accruedInt",
        "side",
        "existing",
        "commission",
        "repoSpecificFields",
        "volume",
    )

    for key in required_keys:
        assert key in data, f"missing '{key}' in PortfolioTrade: {data}"


@pytest.mark.parametrize("opcode", ["PositionsGetAndSubscribeV2"])
def test_positions_get_and_subscribe_v2_returns_position_slim(opcode):
    payload = base_payload(opcode)
    payload.update(
        {
            "portfolio": "D39004",
            "skipHistory": False,
        }
    )

    data = ws_first_data(payload)

    required_keys = (
        "v",
        "cv",
        "sym",
        "tic",
        "p",
        "ex",
        "pxavg",
        "q",
        "o",
        "lot",
        "n",
        "q0",
        "q1",
        "q2",
        "qf",
        "upd",
        "up",
        "cur",
        "h",
    )

    for key in required_keys:
        assert key in data, f"missing '{key}' in Position Slim: {data}"
