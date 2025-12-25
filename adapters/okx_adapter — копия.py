# adapters/okx_adapter.py
import time
import json
import hmac
import base64
import hashlib

from typing import Any, Dict, Optional, List

import httpx


# Маппинг строкового состояния инструмента OKX в числовой tradingStatus Astras
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

    На данный момент реализовано только получение инструментов
    и возврат их сразу в Slim-формате Astras.
    """

    def __init__(
        self,
        rest_base: str = "https://www.okx.com",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        demo: bool = False,
    ) -> None:
        """
        rest_base        — базовый URL для REST API OKX
        api_key          — API-ключ (пока не используется, но сохранён для будущих приватных методов)
        api_secret       — секретный ключ (пока не используется)
        api_passphrase   — passphrase (пока не используется)
        demo             — флаг для будущей поддержки demo-окружения
        """
        self._rest_base = rest_base.rstrip("/")

        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._demo = demo

        # HTTP-клиент создаём лениво при первом запросе
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Создаёт и возвращает httpx.AsyncClient при первом обращении."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    async def close(self) -> None:
        """Закрывает HTTP-клиент при остановке адаптера."""
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
        Выполняет публичный REST-запрос к OKX.

        path может быть вида "public/instruments" или "/public/instruments".
        Здесь автоматически добавляется префикс /api/v5.
        """
        client = await self._get_http_client()

        # Нормализуем путь: добавляем ведущий слэш и префикс /api/v5 при необходимости
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path

        resp = await client.request(method.upper(), path, params=params)
        resp.raise_for_status()

        data = resp.json()

        # Стандартный формат ответа OKX: {"code": "0", "msg": "", "data": [...]}
        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"OKX error {data.get('code')}: {data.get('msg')}")

        return data

    async def list_instruments(self) -> List[dict]:
        """
        Возвращает список инструментов OKX в Slim-формате Astras.

        Используется публичный метод:
        GET /api/v5/public/instruments?instType=SPOT

        Все поля, которые нельзя корректно заполнить на основе ответа OKX,
        оставляются равными None.
        """

        params = {"instType": "SPOT"}

        raw = await self._request_public(
            method="GET",
            path="/public/instruments",
            params=params,
        )

        out: List[dict] = []

        for item in raw.get("data", []):
            inst_id = item.get("instId")       # например "BTC-USDT"
            inst_type = item.get("instType")   # "SPOT", "SWAP", "FUTURES", ...
            state = item.get("state", "unknown")

            quote = item.get("quoteCcy")       # валюта котировки, например "USDT"
            lot_sz = item.get("lotSz")         # минимальный лот
            tick_sz = item.get("tickSz")       # шаг цены

            def to_float(value: Any) -> Optional[float]:
                """Аккуратно конвертирует значение в float, если это возможно."""
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            slim = {
                # Основные поля Slim
                "sym": inst_id,
                "n": inst_id,
                "desc": inst_id,
                "ex": "OKX",

                # Текстовое описание инструмента биржа не отдаёт
                "t": None,

                # Лоты и параметры инструмента
                "lot": to_float(lot_sz),
                "fv": None,
                "cfi": None,

                # Шаг цены
                "stp": to_float(tick_sz),
                "cncl": None,
                "rt": None,

                # Границы цен — публичное API OKX их не отдаёт для SPOT
                "mgb": None,
                "mgs": None,
                "mgrt": None,
                "stppx": None,

                "pxmx": None,
                "pxmn": None,

                # Вспомогательные поля, для криптобиржи значения отсутствуют
                "pxt": None,
                "pxtl": None,
                "pxmu": None,
                "pxu": None,
                "vl": None,

                # Валюта котировки инструмента
                "cur": quote,

                # Для криптовалют ISIN и доходность не применимы
                "isin": None,
                "yld": None,

                # Тип инструмента (SPOT/SWAP/FUTURES/OPTION...)
                "bd": inst_type,
                "pbd": inst_type,

                # Торговый статус
                "st": OKX_STATE_TO_TRADING_STATUS.get(state, 0),
                "sti": state,

                # Тип точности цены — у OKX прямого аналога нет
                "cpct": None,
            }

            out.append(slim)

        return out
