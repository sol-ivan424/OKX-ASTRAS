import asyncio
from typing import Any, Dict, List, Optional

import httpx


class OkxStateMixin:
    def _apply_okx_book_delta(
        self,
        side_state: Dict[float, float],
        levels: List[Any],
    ) -> None:
        for lvl in levels or []:
            if not lvl:
                continue
            price = self._to_float(lvl[0]) if len(lvl) > 0 else 0.0
            sz = self._to_float(lvl[1]) if len(lvl) > 1 else 0.0
            if price <= 0:
                continue
            if sz <= 0:
                # удаление уровня
                side_state.pop(price, None)
            else:
                # установка/обновление уровня
                side_state[price] = sz

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
        self._inst_id_code_cache: Dict[str, str] = {}
        self._inst_id_code_cache_ts: float = 0.0
        self._inst_id_code_cache_ttl_sec: float = 60.0
        self._inst_id_code_cache_lock = asyncio.Lock()
        self._order_ws = None
        self._order_ws_lock = asyncio.Lock()
        self._order_ws_req_lock = asyncio.Lock()
        self._order_ws_keepalive_task = None

        if self._demo:
            self._ws_candles_url = "wss://wspap.okx.com:8443/ws/v5/business"
            self._ws_public_url = "wss://wspap.okx.com:8443/ws/v5/public"
            self._ws_private_url = "wss://wspap.okx.com:8443/ws/v5/private"
        else:
            self._ws_candles_url = "wss://ws.okx.com:8443/ws/v5/business"
            self._ws_public_url = "wss://ws.okx.com:8443/ws/v5/public"
            self._ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"

        self._ws_private_channels = {"orders", "fills", "account", "positions"}

    def _assert_private_ws(self, channel: str, ws_url: str) -> None:
        if channel in self._ws_private_channels and ws_url != self._ws_private_url:
            raise RuntimeError(
                f"Private OKX channel '{channel}' must use private WS URL {self._ws_private_url}"
            )

    def _to_float(self, v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def _to_int(self, v: Any) -> int:
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0

