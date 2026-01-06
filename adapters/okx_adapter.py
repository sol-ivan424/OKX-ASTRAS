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

    Никаких Astras-форматов здесь нет: адаптер только получает данные OKX
    и возвращает их в нейтральном виде для server.py.
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

        self._ws_business_url = "wss://ws.okx.com:8443/ws/v5/business"

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
        Для demo OKX нужен заголовок x-simulated-trading: 1 (если используешь demo).
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
        """Проверка валидности API-ключей безопасным запросом."""
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

    def _tf_to_okx_bar(self, tf: str) -> str:
        """
        Преобразует таймфрейм из запроса Astras в значение OKX bar.

        В Astras у тебя чаще всего tf в секундах строкой: "60", "300", "3600".
        В OKX REST bar: "1m", "5m", "1H", "1D", ...
        """
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

    def _tf_to_okx_ws_channel(self, tf: str) -> str:
        """
        Преобразует таймфрейм Astras в канал OKX WS candle.
        Пример: "60" -> "candle1m", "3600" -> "candle1H".
        """
        bar = self._tf_to_okx_bar(tf)
        return "candle" + bar

    def _parse_okx_candle_any(self, symbol: str, arr: list) -> dict:
        """
        Универсальный парсер свечи OKX для WS и REST.

        WS обычно: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
        REST history-candles обычно: [ts,o,h,l,c,vol,volCcy,confirm]
        ts в миллисекундах.
        """
        def to_float(v: Any) -> float:
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        def to_int(v: Any) -> int:
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return 0

        ts_ms = to_int(arr[0]) if len(arr) > 0 else 0
        o = to_float(arr[1]) if len(arr) > 1 else 0.0
        h = to_float(arr[2]) if len(arr) > 2 else 0.0
        l = to_float(arr[3]) if len(arr) > 3 else 0.0
        c = to_float(arr[4]) if len(arr) > 4 else 0.0
        vol = to_float(arr[5]) if len(arr) > 5 else 0.0

        confirm = 0
        if len(arr) >= 9:
            confirm = to_int(arr[8])
        elif len(arr) >= 8:
            confirm = to_int(arr[7])

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

    async def get_bars_history(
        self,
        symbol: str,
        tf: str,
        from_ts: int,
        limit_per_request: int = 100,
        max_requests: int = 20,
    ) -> List[dict]:
        """
        История свечей через REST OKX /market/history-candles.

        Возвращает нейтральный формат:
        symbol, ts(ms), open, high, low, close, volume, confirm

        from_ts ожидается в секундах Unix (как приходит от Astras).
        """
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
        """
        Подписка на свечи через WS.

        Адаптер отдаёт нейтральный формат:
        symbol, ts(ms), open, high, low, close, volume, confirm

        from_ts/skip_history/split_adjust здесь не используются для OKX WS.
        Историю отдаём отдельным REST методом get_bars_history.
        """
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
