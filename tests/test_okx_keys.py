import asyncio
import os
from dotenv import load_dotenv
from adapters.okx_adapter import OkxAdapter

load_dotenv()

async def main():
    a = OkxAdapter(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
    )

    await a.check_api_keys()
    print("OK")

    await a.close()

asyncio.run(main())
