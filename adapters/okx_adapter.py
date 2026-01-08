# adapters/okx_adapter.py

import json
import hmac
import base64
import hashlib
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Callable

import httpx
import websockets


class OkxAdapter:
    """
    Адаптер OKX.

    Содержит:
    - публичные REST-запросы (например, список инструментов)
    - приватные REST-запросы с подписью (например, проверка API-ключей)
    - публичные WS-подписки (например, свечи)
    - приватные WS-подписки (например, заявки)

    Никаких Astras-форматов здесь нет: адаптер только получает данные OKX
    и возвращает их в нейтральном виде для server.py.
    """

    #инициализация адаптера
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

        #публичный WS (business) для свечей
        self._ws_business_url = "wss://ws.okx.com:8443/ws/v5/business"
        #приватный WS для заявок
        self._ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"

    #универсальное преобразование в float (если нет значения -> 0.0)
    def _to_float(self, v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    #универсальное преобразование в int (если нет значения -> 0)
    def _to_int(self, v: Any) -> int:
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0

    #подпись HMAC-SHA256 + Base64 (общая крипто-часть для REST и WS)
    def _hmac_sha256_base64(self, message: str) -> str:
        if not self._api_secret:
            raise RuntimeError("OKX API secret не задан")

        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    #timestamp для REST (ISO8601 с миллисекундами)
    def _rest_timestamp(self) -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    #timestamp для WS login (секунды)
    def _ws_timestamp(self) -> str:
        return str(int(datetime.now(timezone.utc).timestamp()))

    #ленивая инициализация HTTP-клиента
    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self._rest_base,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._http_client

    #корректное закрытие HTTP-клиента
    async def close(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    #нормализация пути к /api/v5/...
    def _normalize_path(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/api/"):
            path = "/api/v5" + path
        return path

    #формирование подписи OKX для приватного REST запроса
    def _sign_request(self, timestamp: str, method: str, request_path: str, body: str) -> str:
        """
        Формирует подпись OKX для приватного запроса.

        sign = Base64( HMAC_SHA256( timestamp + method + request_path + body ) )
        """
        message = timestamp + method.upper() + request_path + body
        return self._hmac_sha256_base64(message)

    #базовые заголовки (demo mode)
    def _base_headers(self) -> Dict[str, str]:
        """
        Базовые заголовки.
        Для demo OKX нужен заголовок x-simulated-trading: 1 (если используешь demo).
        """
        headers: Dict[str, str] = {}
        if self._demo:
            headers["x-simulated-trading"] = "1"
        return headers

    #публичный REST-запрос к OKX
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

    #приватный REST-запрос к OKX с подписью
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

    #проверка валидности API-ключей
    async def check_api_keys(self) -> None:
        await self._request_private("GET", "/account/balance")

    #получение списка инструментов OKX
    async def list_instruments(self, inst_type: str = "SPOT") -> List[dict]:
        """
        Возвращает инструменты OKX в нейтральном формате:
        symbol, exchange, instType, state, baseCcy, quoteCcy, lotSz, tickSz
        """
        raw = await self._request_public(
            path="/public/instruments",
            params={"instType": inst_type},
        )

        def to_opt_float(v: Any) -> Optional[float]:
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
                    "lotSz": to_opt_float(item.get("lotSz")),
                    "tickSz": to_opt_float(item.get("tickSz")),
                }
            )
        return out

    #маппинг таймфрейма Astras (секунды) -> OKX bar
    def _tf_to_okx_bar(self, tf: str) -> str:
        tf = str(tf).strip()
        mapping = {
            "1": "1s",
            "60": "1m",
            "180": "3m",
            "300": "5m",
            "900": "15m",
            "1800": "30m",
            "3600": "1H",
            "7200": "2H",
            "14400": "4H",
            "21600": "6H",
            "43200": "12H",
            "86400": "1D",
            "604800": "1W",
        }
        return mapping.get(tf, "1m")

    #маппинг таймфрейма Astras (секунды) -> OKX WS candle channel
    def _tf_to_okx_ws_channel(self, tf: str) -> str:
        bar = self._tf_to_okx_bar(tf)
        return "candle" + bar

    #универсальный парсер свечи OKX (WS и REST)
    def _parse_okx_candle_any(self, symbol: str, arr: list) -> dict:
        """
        WS: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
        REST history-candles: [ts,o,h,l,c,vol,volCcy,confirm]
        ts в миллисекундах
        """
        ts_ms = self._to_int(arr[0]) if len(arr) > 0 else 0
        o = self._to_float(arr[1]) if len(arr) > 1 else 0.0
        h = self._to_float(arr[2]) if len(arr) > 2 else 0.0
        l = self._to_float(arr[3]) if len(arr) > 3 else 0.0
        c = self._to_float(arr[4]) if len(arr) > 4 else 0.0
        vol = self._to_float(arr[5]) if len(arr) > 5 else 0.0

        confirm = 0
        if len(arr) >= 9:
            confirm = self._to_int(arr[8])
        elif len(arr) >= 8:
            confirm = self._to_int(arr[7])

        return {
            "symbol": symbol,
            "ts": ts_ms,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": vol,
            "confirm": confirm,
        }

    #получение истории свечей через REST /market/history-candles
    async def get_bars_history(
        self,
        symbol: str,
        tf: str,
        from_ts: int,
        limit_per_request: int = 100,
        max_requests: int = 20,
    ) -> List[dict]:
        bar = self._tf_to_okx_bar(tf)
        from_ms = int(from_ts) * 1000

        collected: List[dict] = []
        seen_ts: set[int] = set()

        after: Optional[int] = None

        for _ in range(max_requests):
            params: Dict[str, Any] = {
                "instId": symbol,
                "bar": bar,
                "limit": str(int(limit_per_request)),
            }
            if after is not None:
                params["after"] = str(after)

            raw = await self._request_public("/market/history-candles", params=params)
            rows = raw.get("data", [])
            if not rows:
                break

            oldest_ts_ms: Optional[int] = None

            for arr in rows:
                b = self._parse_okx_candle_any(symbol, arr)
                ts_ms = int(b.get("ts", 0))
                if ts_ms <= 0:
                    continue
                if ts_ms in seen_ts:
                    continue
                seen_ts.add(ts_ms)

                if ts_ms >= from_ms:
                    collected.append(b)

                if oldest_ts_ms is None or ts_ms < oldest_ts_ms:
                    oldest_ts_ms = ts_ms

            if oldest_ts_ms is None:
                break

            if oldest_ts_ms < from_ms:
                break

            after = oldest_ts_ms

        collected.sort(key=lambda x: int(x.get("ts", 0)))
        return collected

    #подписка на свечи через WS
    async def subscribe_bars(
        self,
        symbol: str,
        tf: str,
        from_ts: int,
        skip_history: bool,
        split_adjust: bool,
        on_data: Callable[[dict], Any],
        stop_event: asyncio.Event,
    ) -> None:
        channel = self._tf_to_okx_ws_channel(tf)

        sub_msg = {
            "op": "subscribe",
            "args": [{"channel": channel, "instId": symbol}],
        }

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_business_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)

                        if msg.get("event") == "error":
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for candle_arr in data:
                            bar = self._parse_okx_candle_any(symbol, candle_arr)
                            res = on_data(bar)
                            if asyncio.iscoroutine(res):
                                await res

            except Exception:
                await asyncio.sleep(1.0)
                continue

    #формирование payload login для приватного WS OKX
    def _ws_login_payload(self) -> Dict[str, Any]:
        """
        OKX WS login:
        sign = Base64( HMAC_SHA256( timestamp + "GET" + "/users/self/verify" ) )
        timestamp в секундах строкой
        """
        if not self._api_key or not self._api_secret or not self._api_passphrase:
            raise RuntimeError("OKX API ключи не заданы")

        ts = self._ws_timestamp()
        prehash = ts + "GET" + "/users/self/verify"
        sign = self._hmac_sha256_base64(prehash)

        return {
            "op": "login",
            "args": [
                {
                    "apiKey": self._api_key,
                    "passphrase": self._api_passphrase,
                    "timestamp": ts,
                    "sign": sign,
                }
            ],
        }

    #парсер ордера OKX (REST и WS) в нейтральный формат
    def _parse_okx_order_any(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нейтральный формат ордера:
        id, symbol, type, side, status, price, qty, filled, ts_create(ms), ts_update(ms)
        """
        ord_id = d.get("ordId") or "0"
        inst_id = d.get("instId") or "0"

        ord_type = d.get("ordType") or "0"
        side = d.get("side") or "0"
        state = d.get("state") or "0"

        px = self._to_float(d.get("px"))
        sz = self._to_float(d.get("sz"))
        fill_sz = self._to_float(d.get("fillSz"))

        c_time = self._to_int(d.get("cTime"))
        u_time = self._to_int(d.get("uTime"))

        return {
            "id": ord_id,
            "symbol": inst_id,
            "type": ord_type,
            "side": side,
            "status": state,
            "price": px,
            "qty": sz,
            "filled": fill_sz,
            "ts_create": c_time,
            "ts_update": u_time,
        }

    #REST: активные заявки (pending)
    async def get_orders_pending(
        self,
        inst_type: str = "SPOT",
        inst_id: Optional[str] = None,
        ord_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"instType": inst_type, "limit": str(int(limit))}
        if inst_id:
            params["instId"] = inst_id
        if ord_type:
            params["ordType"] = ord_type
        if state:
            params["state"] = state

        raw = await self._request_private("GET", "/trade/orders-pending", params=params)
        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []):
            out.append(self._parse_okx_order_any(item))
        return out

    #WS: подписка на изменения заявок (private channel orders)
    async def subscribe_orders(
        self,
        symbols: List[str],
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: str = "SPOT",
    ) -> None:
        login_msg = self._ws_login_payload()

        sub_args: Dict[str, Any] = {"channel": "orders", "instType": inst_type}
        sub_msg = {"op": "subscribe", "args": [sub_args]}

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_msg))

                    #ждём подтверждение login
                    authed = False
                    login_deadline = asyncio.get_event_loop().time() + 5.0
                    while not authed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > login_deadline:
                            raise RuntimeError("OKX WS login timeout")

                        raw = await ws.recv()
                        msg = json.loads(raw)
                        if msg.get("event") == "login":
                            if msg.get("code") == "0":
                                authed = True
                                break
                            raise RuntimeError(f"OKX WS login error: {msg}")

                    await ws.send(json.dumps(sub_msg))

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)
                        if msg.get("event") == "error":
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            order = self._parse_okx_order_any(item)
                            res = on_data(order)
                            if asyncio.iscoroutine(res):
                                await res

            except Exception:
                await asyncio.sleep(1.0)
                continue
