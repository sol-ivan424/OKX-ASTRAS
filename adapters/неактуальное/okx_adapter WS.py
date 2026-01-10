# adapters/okx_adapter.py

import time
import json
import hmac
import base64
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx


class OkxAdapter:


    def __init__(
        self,
        rest_base: str = "https://www.okx.com",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
    ) -> None:
        self._rest_base = rest_base.rstrip("/")

        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

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

    def _sign_request(
        self,
        timestamp: str,
        method: str,
        request_path: str,
        body: str,
    ) -> str:
        """
        Формирует подпись OKX для приватного запроса.

        Формула:
        sign = Base64( HMAC_SHA256( timestamp + method + request_path + body ) )
        """
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    async def _request_private(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Выполняет приватный REST-запрос к OKX с корректной подписью.
        """

        if not self._api_key or not self._api_secret or not self._api_passphrase:
            raise RuntimeError("OKX API ключи не заданы")

        client = await self._get_http_client()

        # Нормализуем путь
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path

        # OKX требует timestamp в ISO 8601 UTC формате
        timestamp = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        # Для GET body всегда пустая строка
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
        }

        response = await client.request(
            method.upper(),
            path,
            params=params,
            content=body_str if body else None,
            headers=headers,
        )

        response.raise_for_status()
        data = response.json()

        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(
                f"OKX error {data.get('code')}: {data.get('msg')}"
            )

        return data

    async def check_api_keys(self) -> None:
        """
        Проверяет валидность API-ключей безопасным запросом.
        Ничего не изменяет в аккаунте.
        """
        await self._request_private("GET", "/account/balance")
