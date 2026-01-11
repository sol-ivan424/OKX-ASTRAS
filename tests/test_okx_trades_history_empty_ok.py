import os
import asyncio
import pytest

from adapters.okx_adapter import OkxAdapter


def test_okx_trades_history_returns_list_even_if_empty():
    async def _main():
        api_key = os.getenv("OKX_API_KEY")
        api_secret = os.getenv("OKX_API_SECRET")
        api_passphrase = os.getenv("OKX_API_PASSPHRASE")

        if not (api_key and api_secret and api_passphrase):
            pytest.skip("OKX API ключи не заданы в окружении.")

        a = OkxAdapter(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )

        # сам факт, что запрос проходит и возвращает список — уже проверка работоспособности
        trades = await a.get_trades_history(inst_type="SPOT", limit=1)

        assert isinstance(trades, list)

        # если есть сделки — проверяем минимальные поля нейтрального формата
        if trades:
            t = trades[0]
            assert isinstance(t, dict)
            for k in ["id", "orderno", "symbol", "side", "price", "qtyUnits", "ts", "commission", "volume", "value", "is_history"]:
                assert k in t

        await a.close()

    asyncio.run(_main())