import os
import sys

# Добавляем корень проекта в sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fastapi.testclient import TestClient
from api.server import app

client = TestClient(app)


def test_http_get_instruments_simple_format():
    response = client.get(
        "/v2/instruments",
        params={
            "exchange": "MOEX",
            "format": "Simple",
            "token": "test-token-123",
        }
    )

    assert response.status_code == 200, f"Unexpected status: {response.status_code}"

    data = response.json()
    assert isinstance(data, list), "response must be a list"
    assert len(data) > 0, "instruments list should not be empty"

    instrument = data[0]
    assert isinstance(instrument, dict), "instrument must be an object"

    # Поля SIMPLE-формата Astras для инструментов (по примеру из документации)
    required_keys = (
        "symbol",
        "shortname",
        "description",
        "exchange",
        "market",
        "type",
        "lotsize",
        "facevalue",
        "cfiCode",
        "cancellation",
        "minstep",
        "rating",
        "marginbuy",
        "marginsell",
        "marginrate",
        "pricestep",
        "priceMax",
        "priceMin",
        "theorPrice",
        "theorPriceLimit",
        "volatility",
        "currency",
        "ISIN",
        "yield",
        "board",
        "primary_board",
        "tradingStatus",
        "tradingStatusInfo",
        "complexProductCategory",
        "priceMultiplier",
        "priceShownUnits",
    )

    for key in required_keys:
        assert key in instrument, f"missing field '{key}' in instrument: {instrument}"


def test_http_get_instruments_requires_token():
    response = client.get(
        "/v2/instruments",
        params={
            "exchange": "MOEX",
            "format": "Simple",
        }
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "TokenRequired"}
