from api.server import _astras_order_simple_from_okx_neutral

def test_orders_mapping_existing_and_norm():
    okx_neutral = {
        "id": "123",
        "symbol": "BTC-USDT",
        "type": "limit",
        "side": "buy",
        "status": "live",
        "tif": "gtc",
        "price": 100.0,
        "qty": 2.0,
        "filled": 0.0,
        "ts_create": 1700000000000,
        "ts_update": 1700000005000,
    }

    snap = _astras_order_simple_from_okx_neutral(okx_neutral, exchange="MOEX", portfolio="P1", existing=True)
    assert snap["existing"] is True
    assert snap["status"] == "working"
    assert snap["type"] == "limit"
    assert snap["timeInForce"] == "GoodTillCancelled"

    upd = _astras_order_simple_from_okx_neutral(okx_neutral, exchange="MOEX", portfolio="P1", existing=False)
    assert upd["existing"] is False
    assert upd["timeInForce"] == "GoodTillCancelled"