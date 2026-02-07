import httpx
import pytest
import json

BASE_URL = "http://127.0.0.1:8000"


@pytest.mark.anyio
async def test_astras_positions_rest_dump():
    """
    Проверяем REST positions:
    /md/v2/clients/{client_id}/positions

    Тест:
    - отправляет запрос как Astras
    - выводит ВСЕ полученные данные
    """

    client_id = "DEV"
    params = {
        "exchange": "MOEX",          # Astras всегда шлёт MOEX / SPBX
        "portfolio": "D00013",       # виртуальный портфель
        "format": "Simple",
        "withoutCurrency": False,
        "jsonResponse": False,
    }

    url = f"{BASE_URL}/md/v2/clients/{client_id}/positions"

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params)

    assert resp.status_code == 200

    data = resp.json()

    print("\n=== REST POSITIONS RESPONSE ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    print(f"\n=== RECEIVED {len(data)} POSITION(S) ===")

    # Минимальная sanity-проверка формата
    for p in data:
        assert "symbol" in p
        assert "portfolio" in p
        assert "exchange" in p