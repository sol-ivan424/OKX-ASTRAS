# tests/test_okx_orders_pending.py

import asyncio
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

from adapters.okx_adapter import OkxAdapter


#загрузка .env из корня проекта
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


async def main():
    #быстрая проверка, что переменные окружения реально подхватились
    print("OKX_API_KEY:", bool(os.getenv("OKX_API_KEY")))
    print("OKX_API_SECRET:", bool(os.getenv("OKX_API_SECRET")))
    print("OKX_API_PASSPHRASE:", bool(os.getenv("OKX_API_PASSPHRASE")))

    a = OkxAdapter(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
    )

    try:
        orders = await a.get_orders_pending(inst_type="SPOT")
        print("orders_pending_count:", len(orders))
        if orders:
            print("first:", orders[0])

    except httpx.HTTPStatusError as e:
        print("HTTP status:", e.response.status_code)
        print("OKX response body:", e.response.text)

    finally:
        await a.close()


asyncio.run(main())
