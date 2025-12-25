import os
import asyncio
from dotenv import load_dotenv

from adapters.okx_adapter import OkxAdapter


async def main() -> None:
    load_dotenv()

    adapter = OkxAdapter(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
    )

    try:
        await adapter.check_api_keys()
        print("OKX API ключи работают: приватный запрос /account/balance успешен")
    finally:
        await adapter.close()


if __name__ == "__main__":
    asyncio.run(main())
