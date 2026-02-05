# tests/test_okx_env_and_private_ws.py
import os
import sys
import asyncio
import contextlib
from pathlib import Path

import pytest

# гарантируем, что корень проекта в sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from adapters.okx_adapter import OkxAdapter


def _get_env(name: str) -> str:
    v = os.getenv(name)
    assert v and v.strip(), f"Missing env var: {name}"
    return v.strip()


def _print_balance(raw: dict):
    # OKX /account/balance -> {"data":[{"details":[...]}]}
    data = raw.get("data") or []
    if not data:
        print("\n=== OKX BALANCE: EMPTY ===")
        return

    details = (data[0] or {}).get("details") or []
    print("\n=== OKX BALANCE (first 20 currencies) ===")
    for d in details[:20]:
        ccy = d.get("ccy")
        avail = d.get("availBal")
        cash = d.get("cashBal")
        eq = d.get("eq")
        if ccy:
            print(f"{ccy:>6}  availBal={avail}  cashBal={cash}  eq={eq}")


@pytest.mark.anyio
async def test_okx_private_rest_keys_are_valid():
    """
    Проверяет, что ключи из окружения реально работают на приватном REST.
    Успех: /account/balance проходит без исключений.
    Плюс: печатаем полученные данные.
    """
    adapter = OkxAdapter(
        api_key=_get_env("OKX_API_KEY"),
        api_secret=_get_env("OKX_API_SECRET"),
        api_passphrase=_get_env("OKX_API_PASSPHRASE"),
        demo=False,
    )
    try:
        # read-only
        raw = await adapter._request_private("GET", "/account/balance")
        _print_balance(raw)
    finally:
        await adapter.close()


@pytest.mark.anyio
async def test_okx_private_ws_subscribe_works():
    """
    Проверяет, что приватный WS логин и подписка реально проходят.
    Плюс: печатаем первое пришедшее summary (если оно есть).
    """
    adapter = OkxAdapter(
        api_key=_get_env("OKX_API_KEY"),
        api_secret=_get_env("OKX_API_SECRET"),
        api_passphrase=_get_env("OKX_API_PASSPHRASE"),
        demo=os.getenv("OKX_DEMO", "0") in ("1", "true", "True", "yes", "YES"),
    )

    stop_event = asyncio.Event()
    subscribed_evt = asyncio.Event()
    error_evt = asyncio.Event()
    enough_data_evt = asyncio.Event()

    error_payload = {}
    summaries = []

    async def on_subscribed(msg: dict):
        subscribed_evt.set()

    async def on_error(msg: dict):
        nonlocal error_payload
        error_payload = msg or {}
        error_evt.set()

    async def on_data(summary: dict):
        nonlocal summaries
        if len(summaries) < 5:
            summaries.append(summary or {})
            print(f"\n=== OKX WS SUMMARY (message {len(summaries)}) ===")
            print(summary)
            if len(summaries) == 5:
                enough_data_evt.set()

    task = asyncio.create_task(
        adapter.subscribe_summaries(
            on_data=on_data,
            stop_event=stop_event,
            on_subscribed=on_subscribed,
            on_error=on_error,
        )
    )

    try:
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(subscribed_evt.wait()),
                asyncio.create_task(error_evt.wait()),
            ],
            timeout=12.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

        assert done, "Timeout: OKX did not confirm subscribe"
        assert subscribed_evt.is_set(), f"OKX WS subscribe failed: {error_payload}"

        # ждём до 10 секунд, пока не придёт 5 сообщений summary (может не прийти, это нормально)
        try:
            await asyncio.wait_for(enough_data_evt.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            print(f"\n=== OKX WS SUMMARY: received {len(summaries)} message(s) within 10s (это нормально) ===")

    finally:
        # просим подписку завершиться корректно
        stop_event.set()

        # даём задаче шанс остановиться сама
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        except asyncio.CancelledError:
            pass

        await adapter.close()