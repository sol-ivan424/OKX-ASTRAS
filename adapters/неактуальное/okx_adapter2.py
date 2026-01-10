import httpx
from typing import Any, Dict, Optional, List


class OkxAdapter:
    """
    Адаптер OKX.

    Отвечает только за получение данных с биржи и возврат в нейтральном формате
    для server.py. Никаких Astras-форматов здесь нет.
    """

    def __init__(
        self,
        rest_base: str = "https://www.okx.com",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        demo: bool = False,
    ) -> None:
        # Параметры ключей оставлены в сигнатуре для совместимости с server.py,
        # но в публичных запросах instruments они не используются.
        self._rest_base = rest_base.rstrip("/")
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    async def close(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _request_public(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client = await self._get_http_client()

        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path

        resp = await client.get(path, params=params)
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"OKX error {data.get('code')}: {data.get('msg')}")

        return data

    async def list_instruments(self) -> List[dict]:
        """
        Возвращает инструменты OKX (SPOT) в нейтральном формате:

        symbol, exchange, instType, state, baseCcy, quoteCcy, lotSz, tickSz
        """
        raw = await self._request_public(
            path="/public/instruments",
            params={"instType": "SPOT"},
        )

        def to_float(value: Any) -> Optional[float]:
            try:
                return float(value)
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
