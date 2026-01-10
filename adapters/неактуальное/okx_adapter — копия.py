# adapters/okx_adapter.py

import json
import hmac
import base64
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import httpx


class OkxAdapter:
    """
    Адаптер OKX.

    Содержит:
    - публичные REST-запросы (например, список инструментов)
    - приватные REST-запросы с подписью (например, проверка API-ключей)

    Никаких Astras-форматов здесь нет: адаптер только получает данные OKX
    и возвращает их в нашем нейтральном виде для server.py.
    """

    def __init__(
        self,
        rest_base: str = "https://www.okx.com",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        demo: bool = False,
    ) -> None:
        self._rest_base = rest_base.rstrip("/")

        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._demo = demo

        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Ленивая инициализация HTTP-клиента."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    async def close(self) -> None:
        """Корректно закрывает HTTP-клиент."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    def _normalize_path(self, path: str) -> str:
        """Приводит путь к формату /api/v5/..."""
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path
        return path

    def _sign_request(self, timestamp: str, method: str, request_path: str, body: str) -> str:
        """
        Формирует подпись OKX для приватного запроса.

        sign = Base64( HMAC_SHA256( timestamp + method + request_path + body ) )
        """
        if not self._api_secret:
            raise RuntimeError("OKX API secret не задан")

        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    def _base_headers(self) -> Dict[str, str]:
        """
        Базовые заголовки.
        Для demo OKX нужен заголовок x-simulated-trading: 1 (если ты используешь demo ключи).
        """
        headers: Dict[str, str] = {}
        if self._demo:
            headers["x-simulated-trading"] = "1"
        return headers

    async def _request_public(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Публичный REST-запрос к OKX."""
        client = await self._get_http_client()
        path = self._normalize_path(path)

        resp = await client.get(path, params=params, headers=self._base_headers())
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"OKX error {data.get('code')}: {data.get('msg')}")
        return data

    async def _request_private(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Приватный REST-запрос к OKX с подписью."""
        if not self._api_key or not self._api_secret or not self._api_passphrase:
            raise RuntimeError("OKX API ключи не заданы")

        client = await self._get_http_client()
        path = self._normalize_path(path)

        timestamp = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        body_str = json.dumps(body) if body else ""
        sign = self._sign_request(
            timestamp=timestamp,
            method=method,
            request_path=path,
            body=body_str,
        )

        headers = {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._api_passphrase,
            "Content-Type": "application/json",
            **self._base_headers(),
        }

        resp = await client.request(
            method.upper(),
            path,
            params=params,
            content=body_str if body else None,
            headers=headers,
        )
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"OKX error {data.get('code')}: {data.get('msg')}")
        return data

    async def check_api_keys(self) -> None:
        """
        Проверка валидности API-ключей безопасным запросом.
        Ничего не изменяет в аккаунте.
        """
        await self._request_private("GET", "/account/balance")

    async def list_instruments(self, inst_type: str = "SPOT") -> List[dict]:
        """
        Возвращает инструменты OKX в нейтральном формате:

        symbol, exchange, instType, state, baseCcy, quoteCcy, lotSz, tickSz
        """
        raw = await self._request_public(
            path="/public/instruments",
            params={"instType": inst_type},
        )

        def to_float(v: Any) -> Optional[float]:
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        out: List[dict] = []
        for item in raw.get("data", []):
            out.append(
                {
                    "symbol": item.get("instId"),
                    "exchange": "OKX",
                    "instType": item.get("instType"),
                    "state": item.get("state"),
                    "baseCcy": item.get("baseCcy"),
                    "quoteCcy": item.get("quoteCcy"),
                    "lotSz": to_float(item.get("lotSz")),
                    "tickSz": to_float(item.get("tickSz")),
                }
            )
        return out
