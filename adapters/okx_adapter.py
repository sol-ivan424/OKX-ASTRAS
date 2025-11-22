import asyncio
import json
import time
from typing import Any, Dict, Optional, Callable, Awaitable, List

import httpx
import websockets


def _now_ms() -> int:
    """Текущее время в миллисекундах."""
    return int(time.time() * 1000)


# Преобразуем строковое состояние инструмента OKX в числовой tradingStatus Astras
OKX_STATE_TO_TRADING_STATUS: Dict[str, int] = {
    "live": 1,
    "preopen": 2,
    "suspend": 3,
    "settled": 4,
    "expired": 5,
}


class OkxAdapter:
    """
    Адаптер для биржи OKX.

    В этой версии:
    - REST-клиент
    - инструменты сразу в Slim-формате Astras
    - базовая WS-инфраструктура (для будущих подписок на котировки/стакан и т. д.)
    """

    def __init__(
        self,
        rest_base: str = "https://www.okx.com",
        ws_public: str = "wss://ws.okx.com/ws/v5/public",
        ws_business: str = "wss://ws.okx.com/ws/v5/business",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        demo: bool = False,
    ) -> None:

        # Базовые URL REST и WebSocket
        self._rest_base = rest_base.rstrip("/")
        self._ws_public_url = ws_public
        self._ws_business_url = ws_business

        # Ключи пока не используются, но оставлены для будущего функционала
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._demo = demo

        # HTTP-клиент создаём лениво при первом запросе
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Ленивая инициализация httpx.AsyncClient."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    async def close(self) -> None:
        """Закрытие HTTP-клиента при завершении работы адаптера."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _request_public(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Выполнение публичного REST-запроса к OKX.

        path может быть вида "market/ticker" или "/market/ticker".
        Здесь автоматически добавляется префикс /api/v5.
        """
        client = await self._get_http_client()

        # Формируем корректный путь
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path

        resp = await client.request(method.upper(), path, params=params)
        resp.raise_for_status()

        data = resp.json()

        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(
                f"OKX error {data.get('code')}: {data.get('msg')}"
            )

        return data

    async def list_instruments(self) -> List[dict]:
        """
        Возвращает список инструментов OKX в Slim-формате Astras.

        Используется публичный метод:
        GET /api/v5/public/instruments?instType=SPOT
        """

        params = {"instType": "SPOT"}

        raw = await self._request_public(
            method="GET",
            path="/public/instruments",
            params=params,
        )

        out: List[dict] = []

        for item in raw.get("data", []):

            inst_id = item.get("instId")
            inst_type = item.get("instType")
            state = item.get("state", "unknown")

            base = item.get("baseCcy")
            quote = item.get("quoteCcy")

            lot_sz = item.get("lotSz")      # минимальный лот
            tick_sz = item.get("tickSz")    # шаг цены

            def to_float(v):
                """Пробуем преобразовать значение в float."""
                try:
                    return float(v)
                except:
                    return None

            slim = {
                # Основные поля Slim
                "sym": inst_id,
                "n": inst_id,
                "desc": inst_id,     # OKX не отдаёт текстовое описание — оставляем символ
                "ex": "OKX",

                # Описание (в Astras "t") — биржа не предоставляет
                "t": None,

                # Лоты и параметры инструмента
                "lot": to_float(lot_sz),
                "fv": None,
                "cfi": None,

                # Шаг цены
                "stp": to_float(tick_sz),
                "cncl": None,
                "rt": None,

                # Границы цен — OKX их не отдаёт для SPOT
                "mgb": None,
                "mgs": None,
                "mgrt": None,
                "stppx": None,

                "pxmx": None,
                "pxmn": None,

                # Разные вспомогательные поля
                "pxt": None,
                "pxtl": None,
                "pxmu": None,
                "pxu": None,
                "vl": None,

                # Валюта — корректно ставить валюту котировки
                "cur": quote,

                "isin": None,
                "yld": None,

                # Тип инструмента (SPOT/SWAP/FUTURES...)
                "bd": inst_type,
                "pbd": inst_type,

                # Торговый статус
                "st": OKX_STATE_TO_TRADING_STATUS.get(state, 0),
                "sti": state,
                "cpct": None,
            }

            out.append(slim)

        return out

    async def _ws_subscribe(
        self,
        url: str,
        args: List[Dict[str, Any]],
        on_message: Callable[[Dict[str, Any]], Awaitable[None]],
        stop_event: asyncio.Event,
        ping_interval: float = 20.0,
    ) -> None:
        """
        Универсальная подписка на WebSocket-каналы OKX.
        """
        payload = {"op": "subscribe", "args": args}

        try:
            async with websockets.connect(url, ping_interval=ping_interval) as ws:
                await ws.send(json.dumps(payload))

                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=ping_interval)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("event") in ("subscribe", "error"):
                        continue

                    await on_message(msg)

        except Exception as exc:
            self._log_error("Ошибка WebSocket-подключения", url=url, error=str(exc))

    async def _ws_subscribe_public(
        self,
        args: List[Dict[str, Any]],
        on_message: Callable[[Dict[str, Any]], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        """Обёртка для подписки на публичные WebSocket-каналы OKX."""
        await self._ws_subscribe(self._ws_public_url, args, on_message, stop_event)

    async def _ws_subscribe_business(
        self,
        args: List[Dict[str, Any]],
        on_message: Callable[[Dict[str, Any]], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        """Обёртка для подписки на бизнес-каналы OKX."""
        await self._ws_subscribe(self._ws_business_url, args, on_message, stop_event)

    def _log_debug(self, msg: str, **extra: Any) -> None:
        print(f"[OKX DEBUG] {msg} | {extra}")

    def _log_error(self, msg: str, **extra: Any) -> None:
        print(f"[OKX ERROR] {msg} | {extra}")
