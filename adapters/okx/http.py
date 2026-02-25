import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import httpx


class OkxHttpMixin:
    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    def _normalize_path(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path
        return path

    def _base_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self._demo:
            headers["x-simulated-trading"] = "1"
        return headers

    def _build_query_string(self, params: Dict[str, Any]) -> str:
        items = []
        for k in sorted(params.keys()):
            v = params[k]
            if v is None:
                continue
            items.append((k, str(v)))
        return urlencode(items)

    async def _request_public(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
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
        if not self._api_key or not self._api_secret or not self._api_passphrase:
            raise RuntimeError("OKX API ключи не заданы")

        client = await self._get_http_client()
        path = self._normalize_path(path)

        timestamp = self._rest_timestamp()
        body_str = json.dumps(body) if body else ""

        #важно: для OKX подпись должна включать query string, если она есть
        request_path_for_sign = path
        request_url = path

        if params:
            qs = self._build_query_string(params)
            if qs:
                request_path_for_sign = f"{path}?{qs}"
                request_url = f"{path}?{qs}"

        sign = self._sign_request(
            timestamp=timestamp,
            method=method,
            request_path=request_path_for_sign,
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

        #params не передаем отдельно, чтобы кодирование query совпало с тем, что подписали
        resp = await client.request(
            method.upper(),
            request_url,
            params=None,
            content=body_str if body else None,
            headers=headers,
        )
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"OKX error {data.get('code')}: {data.get('msg')}")
        return data

