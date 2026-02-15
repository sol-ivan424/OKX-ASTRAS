# adapters/okx_adapter.py

import json
import hmac
import base64
import hashlib
import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Callable
from urllib.parse import urlencode

import httpx
import websockets


class OkxAdapter:
    #применение snapshot/update OKX books к локальному состоянию (price -> size)
    def _apply_okx_book_delta(
        self,
        side_state: Dict[float, float],
        levels: List[Any],
    ) -> None:
        """
        OKX levels: [[price, sz, ...], ...]
        - если sz == 0 -> уровень удаляется
        - иначе уровень устанавливается/обновляется

        Важно: OKX в update присылает только изменившиеся уровни.
        Чтобы всегда отдавать полный стакан нужной глубины, мы храним локальный state
        и применяем update как дельту.
        """
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
        self._inst_id_code_cache: Dict[str, str] = {}
        self._inst_id_code_cache_ts: float = 0.0
        self._inst_id_code_cache_ttl_sec: float = 60.0
        self._inst_id_code_cache_lock = asyncio.Lock()

        # WS для свечей у OKX идёт через /ws/v5/business (а не /public)
        # Иначе OKX отвечает 60018: Wrong URL or channel:candle..., instId ... doesn't exist
        # Для demo/simulated trading у OKX отдельный хост wspap.okx.com
        if self._demo:
            self._ws_candles_url = "wss://wspap.okx.com:8443/ws/v5/business"
            self._ws_public_url = "wss://wspap.okx.com:8443/ws/v5/public"
            self._ws_private_url = "wss://wspap.okx.com:8443/ws/v5/private"
        else:
            self._ws_candles_url = "wss://ws.okx.com:8443/ws/v5/business"
            self._ws_public_url = "wss://ws.okx.com:8443/ws/v5/public"
            self._ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"

        # ВАЖНО:
        # Приватные каналы OKX (orders / fills / account) работают ТОЛЬКО через ws/v5/private
        # Если подписаться на них через public или business — OKX вернёт 60018 (Wrong URL or channel).
        self._ws_private_channels = {"orders", "fills", "account", "positions"}

    def _assert_private_ws(self, channel: str, ws_url: str) -> None:
        if channel in self._ws_private_channels and ws_url != self._ws_private_url:
            raise RuntimeError(
                f"Private OKX channel '{channel}' must use private WS URL {self._ws_private_url}"
            )

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

    #сборка query string для подписи (должна совпасть с тем, что реально уйдет в запрос)
    def _build_query_string(self, params: Dict[str, Any]) -> str:
        items = []
        for k in sorted(params.keys()):
            v = params[k]
            if v is None:
                continue
            items.append((k, str(v)))
        return urlencode(items)

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
                    "symbol": item.get("instId") or "",
                    "exchange": "OKX",
                    "instType": item.get("instType") or inst_type,
                    "state": item.get("state"),
                    # SPOT: baseCcy/quoteCcy
                    # FUTURES/SWAP: у OKX quoteCcy часто пустой, валюты берём из settleCcy/ctValCcy
                    "baseCcy": item.get("baseCcy") or item.get("ctValCcy"),
                    "quoteCcy": item.get("quoteCcy") or item.get("settleCcy"),
                    "lotSz": to_opt_float(item.get("lotSz")),
                    "tickSz": to_opt_float(item.get("tickSz")),
                }
            )
        return out

    async def _resolve_inst_id_code(self, symbol: str, inst_type: str) -> Optional[str]:
        """
        Возвращает instIdCode из отдельного кэша.
        Кэш обновляется пакетно 3 запросами:
        - instType=SPOT
        - instType=FUTURES
        - instType=SWAP
        """
        sym = str(symbol or "").strip()
        if not sym:
            return None

        inst_type_u = str(inst_type or "SPOT").upper().strip() or "SPOT"
        await self._ensure_inst_id_code_cache()

        key = f"{inst_type_u}:{sym}"
        v = self._inst_id_code_cache.get(key)
        if v:
            return v

        # Инструмент мог появиться после последнего refresh — пробуем один раз обновить кэш.
        await self._refresh_inst_id_code_cache()
        return self._inst_id_code_cache.get(key)

    async def _refresh_inst_id_code_cache(self) -> None:
        new_cache: Dict[str, str] = {}
        for one_type in ("SPOT", "FUTURES", "SWAP"):
            raw = await self._request_public(
                path="/public/instruments",
                params={"instType": one_type},
            )
            for it in raw.get("data") or []:
                inst_id = str((it or {}).get("instId") or "").strip()
                inst_id_code = (it or {}).get("instIdCode")
                if not inst_id:
                    continue
                if inst_id_code is None or str(inst_id_code).strip() == "":
                    continue
                new_cache[f"{one_type}:{inst_id}"] = str(inst_id_code)

        self._inst_id_code_cache = new_cache
        self._inst_id_code_cache_ts = time.time()

    async def _ensure_inst_id_code_cache(self) -> None:
        now = time.time()
        if self._inst_id_code_cache and (now - self._inst_id_code_cache_ts) < self._inst_id_code_cache_ttl_sec:
            return

        async with self._inst_id_code_cache_lock:
            now = time.time()
            if self._inst_id_code_cache and (now - self._inst_id_code_cache_ts) < self._inst_id_code_cache_ttl_sec:
                return
            await self._refresh_inst_id_code_cache()

    #маппинг таймфрейма Astras (секунды) -> OKX bar
    def _tf_to_okx_bar(self, tf: str) -> str:
        tf = str(tf).strip()
        mapping = {
            # Astras может присылать таймфрейм как секунды (строкой)
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

            "S": "1s",
            "M": "1m",
            "H": "1H",
            "D": "1D",
            "W": "1W",

            "1s": "1s",
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1H": "1H",
            "2H": "2H",
            "4H": "4H",
            "6H": "6H",
            "12H": "12H",
            "1D": "1D",
            "1W": "1W",
        }
        return mapping.get(tf, "1m")

    #маппинг таймфрейма Astras (секунды) -> OKX WS candle channel
    def _tf_to_okx_ws_channel(self, tf: str) -> str:
        bar = self._tf_to_okx_bar(tf)
        return "candle" + bar

    #универсальный парсер свечи OKX (WS и REST)
    def _parse_okx_candle_any(self, symbol: str, arr: list) -> dict:

        def _get(a, i, default=None):
            return a[i] if (a is not None and i < len(a)) else default

        ts_ms = self._to_int(_get(arr, 0, 0))
        o_raw = _get(arr, 1, None)
        h_raw = _get(arr, 2, None)
        l_raw = _get(arr, 3, None)
        c_raw = _get(arr, 4, None)
        o = self._to_float(o_raw) if o_raw is not None else None
        h = self._to_float(h_raw) if h_raw is not None else None
        l = self._to_float(l_raw) if l_raw is not None else None
        c = self._to_float(c_raw) if c_raw is not None else None
        vol_raw = _get(arr, 5, 0.0)
        vol = self._to_float(vol_raw) if vol_raw is not None else 0.0
        # confirm может быть на 8 (WS) или на 7 (REST history-candles)
        confirm_raw = _get(arr, 8, _get(arr, 7, 0))
        confirm = self._to_int(confirm_raw)

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

    #универсальный парсер стакана OKX (WS snapshot/update) в нейтральный формат
    def _parse_okx_order_book_any(self, symbol: str, item: Dict[str, Any], existing: bool) -> dict:
        """
        OKX books data item:
        {
          "bids": [["price","sz",...], ...],
          "asks": [["price","sz",...], ...],
          "ts": "1703862267800"
        }
        """
        ts_ms = self._to_int(item.get("ts"))

        bids_in = item.get("bids") or []
        asks_in = item.get("asks") or []

        bids = []
        for lvl in bids_in:
            if not lvl:
                continue
            price = self._to_float(lvl[0]) if len(lvl) > 0 else 0.0
            vol = self._to_float(lvl[1]) if len(lvl) > 1 else 0.0
            bids.append((price, vol))

        asks = []
        for lvl in asks_in:
            if not lvl:
                continue
            price = self._to_float(lvl[0]) if len(lvl) > 0 else 0.0
            vol = self._to_float(lvl[1]) if len(lvl) > 1 else 0.0
            asks.append((price, vol))

        return {
            "symbol": symbol,
            "ts": ts_ms,
            "bids": bids,
            "asks": asks,
            "existing": bool(existing),
        }

    #универсальный парсер котировки OKX (WS tickers) в нейтральный формат
    def _parse_okx_ticker_any(self, symbol: str, item: Dict[str, Any]) -> dict:
        
        ts_ms = self._to_int(item.get("ts"))

        last = self._to_float(item.get("last"))
        bid_px = self._to_float(item.get("bidPx"))
        ask_px = self._to_float(item.get("askPx"))

        bid_sz = self._to_float(item.get("bidSz"))
        ask_sz = self._to_float(item.get("askSz"))

        high_24h = self._to_float(item.get("high24h"))
        low_24h = self._to_float(item.get("low24h"))

        vol_24h_base = self._to_float(item.get("vol24h"))
        open_24h = self._to_float(item.get("open24h"))

        return {
            "symbol": symbol,
            "ts": ts_ms,
            "last": last,
            "bid": bid_px,
            "ask": ask_px,
            "bid_sz": bid_sz,
            "ask_sz": ask_sz,
            "high24h": high_24h,
            "low24h": low_24h,
            "open24h": open_24h, 
            "vol24h": vol_24h_base,
        }

    #REST: котировка (tickers) — те же поля, что и WS tickers
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Возвращает котировку OKX через REST в нейтральном формате (как subscribe_quotes / WS tickers):
        {
          "symbol": "...",
          "ts": <ms>,
          "last": <float>,
          "bid": <float>,
          "ask": <float>,
          "bid_sz": <float>,
          "ask_sz": <float>,
          "high24h": <float>,
          "low24h": <float>,
          "open24h": <float>,
          "vol24h": <float>
        }

        OKX endpoint: /market/ticker?instId=...
        """
        raw = await self._request_public(
            path="/market/ticker",
            params={"instId": symbol},
        )

        items = raw.get("data") or []
        if not items:
            # если данных нет — возвращаем нейтральный формат с нулями
            return {
                "symbol": symbol,
                "ts": 0,
                "last": 0.0,
                "bid": 0.0,
                "ask": 0.0,
                "bid_sz": 0.0,
                "ask_sz": 0.0,
                "high24h": 0.0,
                "low24h": 0.0,
                "open24h": 0.0,
                "vol24h": 0.0,
            }

        return self._parse_okx_ticker_any(symbol, items[0])

    #REST: список котировок (tickers) — те же поля, что и WS tickers
    async def list_tickers(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        """
        Возвращает список котировок OKX через REST в нейтральном формате (как WS tickers).

        OKX endpoint: /market/tickers?instType=...
        """
        raw = await self._request_public(
            path="/market/tickers",
            params={"instType": inst_type},
        )

        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []) or []:
            sym = item.get("instId") or "0"
            out.append(self._parse_okx_ticker_any(sym, item))
        return out

    #REST: стакан (books) — снепшот (existing=True) в нейтральном формате как WS books
    async def get_order_book_rest(self, symbol: str, depth: int = 20) -> Dict[str, Any]:
        """
        Возвращает снепшот стакана OKX через REST в нейтральном формате (как subscribe_order_book):
        {
          "symbol": "...",
          "ts": <ms>,
          "bids": [(price, volume), ...],
          "asks": [(price, volume), ...],
          "existing": True
        }

        OKX endpoint: /market/books?instId=...&sz=...
        sz = глубина (максимум зависит от OKX; обычно 1..400). Мы используем то, что просит Astras (например 10/20/50).
        """
        # OKX REST books: sz обязателен и должен быть строкой/числом
        depth_i = int(depth) if depth is not None else 20
        if depth_i <= 0:
            depth_i = 20

        raw = await self._request_public(
            path="/market/books",
            params={"instId": symbol, "sz": str(depth_i)},
        )

        items = raw.get("data") or []
        if not items:
            return {
                "symbol": symbol,
                "ts": 0,
                "bids": [],
                "asks": [],
                "existing": True,
            }

        # В REST books data обычно приходит объект со строковыми bids/asks и ts
        return self._parse_okx_order_book_any(symbol, items[0], existing=True)

    #REST: снепшот котировки для формирования ответа на Astras REST /quotes
    async def get_quote_snapshot_rest(self, symbol: str, book_depth: int = 20) -> Dict[str, Any]:
        """
        Возвращает снепшот котировки OKX через REST в расширенном нейтральном формате для server.py.

        Основа берётся из REST ticker (как get_ticker / WS tickers), плюс доп. поля из REST books:
        - ob_ts: временная метка стакана (ms)
        - total_bid_vol / total_ask_vol: сумма объёмов по уровням, полученным из REST books

        Формат:
        {
          "symbol": "...",
          "ts": <ms>,                 # timestamp тикера
          "last": <float>,
          "bid": <float>,
          "ask": <float>,
          "bid_sz": <float>,
          "ask_sz": <float>,
          "high24h": <float>,
          "low24h": <float>,
          "open24h": <float>,
          "vol24h": <float>,          # объём в базовой валюте за 24ч
          "ob_ts": <ms>,              # timestamp стакана (ms) или 0
          "total_bid_vol": <float>,   # сумма size по bids из books (в базовой валюте)
          "total_ask_vol": <float>    # сумма size по asks из books (в базовой валюте)
        }
        """
        # 1) Тикер (REST /market/ticker) — те же поля, что и WS tickers
        t = await self.get_ticker(symbol)

        # 2) Стакан (REST /market/books) — для ob_ts и суммарных объёмов
        try:
            ob = await self.get_order_book_rest(symbol, depth=book_depth)
        except Exception:
            ob = {"ts": 0, "bids": [], "asks": []}

        ob_ts = int(ob.get("ts", 0) or 0)

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []

        # Суммарные объёмы считаем строго по тем уровням, которые вернул OKX REST books.
        total_bid_vol = 0.0
        for _p, _v in bids:
            try:
                total_bid_vol += float(_v)
            except Exception:
                continue

        total_ask_vol = 0.0
        for _p, _v in asks:
            try:
                total_ask_vol += float(_v)
            except Exception:
                continue

        # Возвращаем расширенный нейтральный формат для server.py
        return {
            **t,
            "ob_ts": ob_ts,
            "total_bid_vol": total_bid_vol,
            "total_ask_vol": total_ask_vol,
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
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        #публичный WS (public) для свечей
        channel = self._tf_to_okx_ws_channel(tf)

        sub_msg = {
            "op": "subscribe",
            "args": [{"channel": channel, "instId": symbol}],
        }
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_candles_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)

                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue

                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for candle_arr in data:
                            bar = self._parse_okx_candle_any(symbol, candle_arr)
                            res = on_data(bar)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

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

    async def _ws_unsubscribe(self, ws, args: Optional[List[Dict[str, Any]]]) -> None:
        if not args:
            return
        try:
            await ws.send(json.dumps({"op": "unsubscribe", "args": args}))
        except Exception:
            return

    #парсер ордера OKX (REST и WS) в нейтральный формат
    def _parse_okx_order_any(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нейтральный формат ордера:
        id, symbol, type, side, status, price, qty, filled,
        ts_create(ms), ts_update(ms), tif
        """

        ord_id = d.get("ordId") or "0"
        inst_id = d.get("instId") or "0"

        ord_type = d.get("ordType") or "0"
        side = d.get("side") or "0"
        state = d.get("state") or "0"

        px = self._to_float(d.get("px"))
        if px == 0:
            px = self._to_float(d.get("avgPx"))
        sz = self._to_float(d.get("sz"))
        fill_sz = self._to_float(d.get("fillSz"))

        c_time = self._to_int(d.get("cTime"))
        u_time = self._to_int(d.get("uTime"))

        # tif (Time In Force) от OKX: gtc / ioc / fok / day и др.
        tif = d.get("tif") or "0"

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
            "tif": tif,
        }

    #парсер сделки OKX (fills) из REST и WS orders в нейтральный формат
    def _parse_okx_trade_any(
        self,
        d: Dict[str, Any],
        is_history: bool,
        inst_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Нейтральный формат сделки (исполнения):
        id, orderno, comment, symbol, side, price, qty_units, qty, ts(ms), commission, fee_ccy, is_history
        """

        trade_id = d.get("tradeId") or d.get("billId") or "0"
        ord_id = d.get("ordId") or "0"
        inst_id = d.get("instId") or "0"

        side = d.get("side") or "0"

        # цена и количество исполнения
        fill_px = self._to_float(d.get("fillPx") if d.get("fillPx") is not None else d.get("px"))
        fill_sz = self._to_float(d.get("fillSz") if d.get("fillSz") is not None else d.get("sz"))

        # время исполнения (мс)
        ts_ms = self._to_int(
            d.get("fillTime") if d.get("fillTime") is not None else d.get("ts")
        )
        if ts_ms <= 0:
            # fallback: uTime/cTime, если fillTime/ts нет
            ts_ms = self._to_int(d.get("uTime") if d.get("uTime") is not None else d.get("cTime"))

        # комиссия
        fee = d.get("fee")
        commission = abs(self._to_float(fee)) if fee is not None else 0.0
        fee_ccy = d.get("feeCcy") or "0"

        # стоимость сделки в валюте расчёта (для OKX SPOT это quote)
        value = fill_px * fill_sz
        volume = value

        # exchange/instType полезны для маппинга в Astras (board/instrumentGroup)
        inst_type_s = str(inst_type or d.get("instType") or "").upper() or None

        # date в ISO 8601 UTC (как ждёт Astras): 2023-12-29T12:35:06.0000000Z
        date_iso = None
        if ts_ms and ts_ms > 0:
            try:
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                date_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "0Z"
            except Exception:
                date_iso = None

        return {
            "id": str(trade_id),
            "orderno": str(ord_id),
            "comment": None,
            "symbol": inst_id,
            "exchange": "OKX",
            "instType": inst_type_s,
            "date": date_iso,
            "side": side,
            "price": fill_px,
            "qtyUnits": fill_sz,     # float
            "qty": fill_sz,          # float, "Количество" по Astras
            "ts": ts_ms,
            "commission": commission,
            "feeCcy": fee_ccy,
            "volume": volume,
            "value": value,
            "is_history": bool(is_history),
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
        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)

        # Если inst_type не задан или не распознан — берём по всем основным типам
        if it == "SPOT":
            inst_types = ["SPOT", "MARGIN"]
        elif it in ("SWAP", "FUTURES", "MARGIN"):
            inst_types = [it]
        else:
            inst_types = ["SPOT", "SWAP", "FUTURES", "MARGIN"]

        out: List[Dict[str, Any]] = []

        for one_type in inst_types:
            params: Dict[str, Any] = {"instType": one_type, "limit": str(int(limit))}
            if inst_id:
                params["instId"] = inst_id
            if ord_type:
                params["ordType"] = ord_type
            if state:
                params["state"] = state

            raw = await self._request_private("GET", "/trade/orders-pending", params=params)
            for item in raw.get("data", []) or []:
                out.append(self._parse_okx_order_any(item))

        # сортируем по времени обновления
        out.sort(key=lambda x: int(x.get("ts_update", 0) or 0))
        return out

    #REST: история заявок (orders-history)
    async def get_orders_history(
        self,
        inst_type: Optional[str] = None,
        inst_id: Optional[str] = None,
        ord_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает историю заявок в нейтральном формате (как _parse_okx_order_any).

        OKX endpoint: /trade/orders-history

        Важно:
        - В запросе Astras на историю заявок instType может отсутствовать.
          Если inst_type=None — считаем, что нужно взять историю по всему аккаунту,
          и делаем запросы для SPOT + SWAP + FUTURES, объединяя результат.

        Параметры:
        - inst_id / ord_type / state — опциональные фильтры OKX.
        - limit — ограничение на ответ OKX (обычно до 100).
        """

        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)

        # Если inst_type не задан — берём по всем основным типам
        inst_types: List[str]
        if it is None:
            inst_types = ["SPOT", "SWAP", "FUTURES", "MARGIN"]
        elif it == "SPOT":
            inst_types = ["SPOT", "MARGIN"]
        else:
            inst_types = [it]

        out: List[Dict[str, Any]] = []

        for one_type in inst_types:
            params: Dict[str, Any] = {
                "instType": one_type,
                "limit": str(int(limit)),
            }
            if inst_id:
                params["instId"] = str(inst_id)
            if ord_type:
                params["ordType"] = str(ord_type)
            if state:
                params["state"] = str(state)

            raw = await self._request_private("GET", "/trade/orders-history", params=params)
            for item in raw.get("data", []) or []:
                out.append(self._parse_okx_order_any(item))

        # сортируем по времени обновления
        out.sort(key=lambda x: int(x.get("ts_update", 0) or 0))
        return out

    #REST: выставление рыночной заявки (market order)
    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        inst_type: str = "SPOT",
        td_mode: Optional[str] = None,
        pos_side: Optional[str] = None,
        tgt_ccy: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tif: Optional[str] = None 
    ) -> Dict[str, Any]:
        """ 
        Выставляет рыночную заявку через OKX REST: POST /api/v5/trade/order

        Минимально необходимые параметры по документации OKX:
        - instId, tdMode, side, ordType, sz
        Дополнительно:
        - posSide: требуется в long/short режиме (деривативы)
        - tgtCcy: для SPOT market buy позволяет указать, в какой валюте задан sz (base_ccy/quote_ccy)
        - clOrdId: клиентский id

        Возвращает нейтральный результат:
        {
          "ordId": "...",
          "clOrdId": "...",
          "sCode": "0",
          "sMsg": ""
        }
        """

        inst_id = symbol
        side_s = str(side or "").lower().strip()
        if side_s not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")

        def _fmt_sz(x: float) -> str:
            s = f"{x:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        # tdMode: SPOT без маржи -> cash, деривативы/маржа -> cross/isolated
        if td_mode is None:
            td_mode = "cash" if str(inst_type).upper() == "SPOT" else "cross"

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_s,
            "ordType": "market",
            "sz": _fmt_sz(quantity),
        }
        inst_id_code = await self._resolve_inst_id_code(inst_id, inst_type)
        if not inst_id_code:
            raise RuntimeError(
                f"Cannot resolve instIdCode for instType={str(inst_type or '').upper()} instId={inst_id}"
            )
        body["instIdCode"] = inst_id_code
        if cl_ord_id:
            body["clOrdId"] = str(cl_ord_id)
        if pos_side:
            body["posSide"] = str(pos_side)
        if tgt_ccy:
            body["tgtCcy"] = str(tgt_ccy)
        if ccy:
            body["ccy"] = str(ccy)
        if tif:
            body["tif"] = str(tif)

        raw = await self._request_private("POST", "/trade/order", body=body)

        items = raw.get("data") or []
        if not items:
            raise RuntimeError("OKX order: empty response data")

        it0 = items[0] or {}
        s_code = str(it0.get("sCode") or "")
        s_msg = str(it0.get("sMsg") or "")

        # OKX: code==0 может быть, но конкретно по ордеру sCode!=0
        if s_code and s_code != "0":
            raise RuntimeError(f"OKX order error {s_code}: {s_msg}")

        return {
            "ordId": str(it0.get("ordId") or "0"),
            "clOrdId": str(it0.get("clOrdId") or ""),
            "sCode": s_code or "0",
            "sMsg": s_msg,
        }

    #WS: выставление рыночной заявки (market order) через private WS op=order
    async def place_market_order_ws(
        self,
        symbol: str,
        side: str,
        quantity: Any,
        inst_type: str = "SPOT",
        td_mode: Optional[str] = None,
        pos_side: Optional[str] = None,
        tgt_ccy: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tif: Optional[str] = None,
        ws_request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Выставляет рыночную заявку через OKX WS private (op=order).
        Возвращает нейтральный результат: ordId/clOrdId/sCode/sMsg.
        """

        inst_id = symbol
        side_s = str(side or "").lower().strip()

        def _fmt_sz(x: Any) -> str:
            try:
                f = float(x)
            except Exception:
                return "" if x is None else str(x)
            s = f"{f:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        # tdMode: SPOT без маржи -> cash, деривативы/маржа -> cross
        if td_mode is None:
            td_mode = "cash" if str(inst_type).upper() == "SPOT" else "cross"

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_s,
            "ordType": "market",
            "sz": _fmt_sz(quantity),
        }
        inst_id_code = await self._resolve_inst_id_code(inst_id, inst_type)
        if not inst_id_code:
            raise RuntimeError(
                f"Cannot resolve instIdCode for instType={str(inst_type or '').upper()} instId={inst_id}"
            )
        body["instIdCode"] = inst_id_code
        if cl_ord_id:
            body["clOrdId"] = str(cl_ord_id)
        if pos_side:
            body["posSide"] = str(pos_side)
        if tgt_ccy:
            body["tgtCcy"] = str(tgt_ccy)
        if ccy:
            body["ccy"] = str(ccy)
        if tif:
            body["tif"] = str(tif)

        req_id = str(ws_request_id or cl_ord_id or f"order-{uuid.uuid4().hex}")
        login_msg = self._ws_login_payload()
        order_msg = {"id": req_id, "op": "order", "args": [body]}

        async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps(login_msg))

            authed = False
            login_deadline = asyncio.get_event_loop().time() + 5.0
            while not authed:
                if asyncio.get_event_loop().time() > login_deadline:
                    raise RuntimeError("OKX WS login timeout")

                raw = await ws.recv()
                msg = json.loads(raw)

                if msg.get("event") == "error":
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")

                if msg.get("event") == "login":
                    if msg.get("code") == "0":
                        authed = True
                        break
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")

            await ws.send(json.dumps(order_msg))

            resp_deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                if asyncio.get_event_loop().time() > resp_deadline:
                    raise RuntimeError("OKX WS order timeout")

                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                msg = json.loads(raw)

                if msg.get("id") != req_id:
                    continue

                code = str(msg.get("code") or "")
                if code and code != "0":
                    err_msg = str(msg.get("msg") or "").strip()
                    err_items = msg.get("data") or []
                    err_it0 = err_items[0] if err_items else {}
                    err_s_code = str((err_it0 or {}).get("sCode") or "")
                    err_s_msg = str((err_it0 or {}).get("sMsg") or "")
                    if err_s_code or err_s_msg:
                        raise RuntimeError(
                            f"OKX WS order error {code}: {err_msg} (sCode={err_s_code or 'n/a'}, sMsg={err_s_msg})"
                        )
                    raise RuntimeError(f"OKX WS order error {code}: {err_msg}")

                items = msg.get("data") or []
                if not items:
                    raise RuntimeError("OKX WS order: empty response data")

                it0 = items[0] or {}
                s_code = str(it0.get("sCode") or "")
                s_msg = str(it0.get("sMsg") or "")
                if s_code and s_code != "0":
                    raise RuntimeError(f"OKX order error {s_code}: {s_msg}")

                return {
                    "ordId": str(it0.get("ordId") or "0"),
                    "clOrdId": str(it0.get("clOrdId") or ""),
                    "sCode": s_code or "0",
                    "sMsg": s_msg,
                }

    #WS: выставление лимитной заявки (limit/ioc/fok/post_only) через private WS op=order
    async def place_limit_order_ws(
        self,
        symbol: str,
        side: str,
        quantity: Any,
        price: Any,
        inst_type: str = "SPOT",
        td_mode: Optional[str] = None,
        pos_side: Optional[str] = None,
        ord_type: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tif: Optional[str] = None,
        ws_request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Выставляет лимитную заявку через OKX WS private (op=order).
        Возвращает нейтральный результат: ordId/clOrdId/sCode/sMsg.
        """

        inst_id = symbol
        side_s = str(side or "").lower().strip()

        def _fmt_num(x: Any) -> str:
            try:
                f = float(x)
            except Exception:
                return "" if x is None else str(x)
            s = f"{f:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        if td_mode is None:
            td_mode = "cash" if str(inst_type).upper() == "SPOT" else "cross"

        ord_type_s = (ord_type or "limit").lower().strip()
        if ord_type_s not in ("limit", "post_only", "ioc", "fok"):
            raise ValueError("ord_type must be one of: limit, post_only, ioc, fok")

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_s,
            "ordType": ord_type_s,
            "sz": _fmt_num(quantity),
            "px": _fmt_num(price),
        }
        inst_id_code = await self._resolve_inst_id_code(inst_id, inst_type)
        if not inst_id_code:
            raise RuntimeError(
                f"Cannot resolve instIdCode for instType={str(inst_type or '').upper()} instId={inst_id}"
            )
        body["instIdCode"] = inst_id_code
        if cl_ord_id:
            body["clOrdId"] = str(cl_ord_id)
        if pos_side:
            body["posSide"] = str(pos_side)
        if ccy:
            body["ccy"] = str(ccy)
        if tif:
            body["tif"] = str(tif)

        req_id = str(ws_request_id or cl_ord_id or f"order-{uuid.uuid4().hex}")
        login_msg = self._ws_login_payload()
        order_msg = {"id": req_id, "op": "order", "args": [body]}

        async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps(login_msg))

            authed = False
            login_deadline = asyncio.get_event_loop().time() + 5.0
            while not authed:
                if asyncio.get_event_loop().time() > login_deadline:
                    raise RuntimeError("OKX WS login timeout")

                raw = await ws.recv()
                msg = json.loads(raw)

                if msg.get("event") == "error":
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")

                if msg.get("event") == "login":
                    if msg.get("code") == "0":
                        authed = True
                        break
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")

            await ws.send(json.dumps(order_msg))

            resp_deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                if asyncio.get_event_loop().time() > resp_deadline:
                    raise RuntimeError("OKX WS order timeout")

                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                msg = json.loads(raw)

                if msg.get("id") != req_id:
                    continue

                code = str(msg.get("code") or "")
                if code and code != "0":
                    err_msg = str(msg.get("msg") or "").strip()
                    err_items = msg.get("data") or []
                    err_it0 = err_items[0] if err_items else {}
                    err_s_code = str((err_it0 or {}).get("sCode") or "")
                    err_s_msg = str((err_it0 or {}).get("sMsg") or "")
                    if err_s_code or err_s_msg:
                        raise RuntimeError(
                            f"OKX WS order error {code}: {err_msg} (sCode={err_s_code or 'n/a'}, sMsg={err_s_msg})"
                        )
                    raise RuntimeError(f"OKX WS order error {code}: {err_msg}")

                items = msg.get("data") or []
                if not items:
                    raise RuntimeError("OKX WS order: empty response data")

                it0 = items[0] or {}
                s_code = str(it0.get("sCode") or "")
                s_msg = str(it0.get("sMsg") or "")
                if s_code and s_code != "0":
                    raise RuntimeError(f"OKX order error {s_code}: {s_msg}")

                return {
                    "ordId": str(it0.get("ordId") or "0"),
                    "clOrdId": str(it0.get("clOrdId") or ""),
                    "sCode": s_code or "0",
                    "sMsg": s_msg,
                }

    #REST: выставление лимитной заявки (limit order)
    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        inst_type: str = "SPOT",
        td_mode: Optional[str] = None,
        pos_side: Optional[str] = None,
        ord_type: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tif: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Выставляет лимитную заявку через OKX REST: POST /api/v5/trade/order

        По документации OKX для лимитной заявки нужны:
        - instId, tdMode, side, ordType, sz, px

        ordType (OKX):
        - limit (обычная лимитная)
        - post_only (пассивная, Maker-only)
        - ioc (Immediate-or-cancel)
        - fok (Fill-or-kill)

        Возвращает нейтральный результат:
        {
          "ordId": "...",
          "clOrdId": "...",
          "sCode": "0",
          "sMsg": ""
        }
        """

        inst_id = symbol
        side_s = str(side or "").lower().strip()
        if side_s not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")

        def _fmt_sz(x: float) -> str:
            s = f"{x:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        def _fmt_px(x: float) -> str:
            s = f"{x:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        # tdMode: SPOT без маржи -> cash, деривативы/маржа -> cross/isolated
        if td_mode is None:
            td_mode = "cash" if str(inst_type).upper() == "SPOT" else "cross"

        # ordType по умолчанию для лимитной заявки
        ord_type_s = (ord_type or "limit").lower().strip()
        if ord_type_s not in ("limit", "post_only", "ioc", "fok"):
            raise ValueError("ord_type must be one of: limit, post_only, ioc, fok")

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_s,
            "ordType": ord_type_s,
            "sz": _fmt_sz(quantity),
            "px": _fmt_px(price),
        }
        if cl_ord_id:
            body["clOrdId"] = str(cl_ord_id)
        if pos_side:
            body["posSide"] = str(pos_side)
        if ccy:
            body["ccy"] = str(ccy)
        if tif:
            body["tif"] = str(tif)

        raw = await self._request_private("POST", "/trade/order", body=body)

        items = raw.get("data") or []
        if not items:
            raise RuntimeError("OKX order: empty response data")

        it0 = items[0] or {}
        s_code = str(it0.get("sCode") or "")
        s_msg = str(it0.get("sMsg") or "")

        # OKX: code==0 может быть, но конкретно по ордеру sCode!=0
        if s_code and s_code != "0":
            raise RuntimeError(f"OKX order error {s_code}: {s_msg}")

        return {
            "ordId": str(it0.get("ordId") or "0"),
            "clOrdId": str(it0.get("clOrdId") or ""),
            "sCode": s_code or "0",
            "sMsg": s_msg,
        }

    #REST: выставление стоп-заявки (conditional / algo order)
    async def place_stop_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        trigger_price: float,
        inst_type: str = "SPOT",
        td_mode: Optional[str] = None,
        pos_side: Optional[str] = None,
        trigger_px_type: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tgt_ccy: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Выставляет стоп-заявку через OKX REST conditional/algo ордер:
        POST /api/v5/trade/order-algo

        Мы делаем STOP-MARKET (после триггера исполняется по рынку):
        - ordType = "conditional"
        - triggerPx = условная цена
        - triggerPxType = тип цены для сравнения (по умолчанию "last")
        - orderPx = "-1"  (для market исполнения после триггера)

        Минимально необходимые параметры по документации OKX:
        - instId, tdMode, side, ordType, sz, triggerPx, triggerPxType, orderPx

        Дополнительно:
        - posSide: требуется для деривативов в hedge (long/short) режиме
        - algoClOrdId: клиентский id для algo-ордера (можно использовать X-REQID)
        - ccy: валюта списания (опционально)
        - tgtCcy: для SPOT может потребоваться в некоторых режимах (опционально; если OKX не поддержит — вернёт ошибку)

        Возвращает нейтральный результат:
        {
          "algoId": "...",
          "algoClOrdId": "...",
          "sCode": "0",
          "sMsg": ""
        }
        """

        inst_id = symbol
        side_s = str(side or "").lower().strip()
        if side_s not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")

        def _fmt_num(x: float) -> str:
            s = f"{x:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

        # tdMode: SPOT без маржи -> cash, деривативы/маржа -> cross/isolated
        if td_mode is None:
            td_mode = "cash" if str(inst_type).upper() == "SPOT" else "cross"

        # triggerPxType: last / index / mark (по документации OKX)
        tpx_type = (trigger_px_type or "last").lower().strip()
        if tpx_type not in ("last", "index", "mark"):
            raise ValueError("trigger_px_type must be one of: last, index, mark")

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_s,
            "ordType": "conditional",
            "sz": _fmt_num(quantity),
            "triggerPx": _fmt_num(trigger_price),
            "triggerPxType": tpx_type,
            # ordPx = -1 => market order after trigger
            "orderPx": "-1",
        }
        if pos_side:
            body["posSide"] = str(pos_side)
        if cl_ord_id:
            body["algoClOrdId"] = str(cl_ord_id)
        if ccy:
            body["ccy"] = str(ccy)
        if tgt_ccy:
            body["tgtCcy"] = str(tgt_ccy)

        raw = await self._request_private("POST", "/trade/order-algo", body=body)

        items = raw.get("data") or []
        if not items:
            raise RuntimeError("OKX stop/algo order: empty response data")

        it0 = items[0] or {}
        s_code = str(it0.get("sCode") or "")
        s_msg = str(it0.get("sMsg") or "")

        if s_code and s_code != "0":
            raise RuntimeError(f"OKX stop/algo order error {s_code}: {s_msg}")

        return {
            "algoId": str(it0.get("algoId") or "0"),
            "algoClOrdId": str(it0.get("algoClOrdId") or ""),
            "sCode": s_code or "0",
            "sMsg": s_msg,
        }

    #REST: оценка максимально доступного размера заявки
    async def get_max_order_size(
        self,
        inst_id: str,
        td_mode: str,
        ccy: Optional[str] = None,
        px: Optional[Any] = None,
    ) -> Dict[str, float]:
        """
        OKX REST: GET /api/v5/account/max-size
        Возвращает maxBuy / maxSell в единицах размера заявки (sz).
        """
        params: Dict[str, Any] = {
            "instId": str(inst_id),
            "tdMode": str(td_mode),
        }
        if ccy is not None and str(ccy).strip():
            params["ccy"] = str(ccy).strip()
        if px is not None and str(px).strip():
            params["px"] = str(px).strip()

        raw = await self._request_private("GET", "/account/max-size", params=params)
        items = raw.get("data") or []
        it0 = items[0] if items else {}

        return {
            "maxBuy": self._to_float((it0 or {}).get("maxBuy")),
            "maxSell": self._to_float((it0 or {}).get("maxSell")),
        }

    #REST: история сделок (fills-history) — исполнения за последние 3 месяца
    async def get_trades_history(
        self,
        inst_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список исполнений в нейтральном формате.
        OKX endpoint: /trade/fills-history
        Возвращает историю за последние 3 месяца.
        Важно:
        - В запросе Astras TradesGetAndSubscribeV2 нет instType.
          Если inst_type=None — считаем, что нужно взять историю по всему аккаунту,
          и делаем запросы для SPOT + SWAP + FUTURES, объединяя результат.
        """

        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)

        # Если inst_type не задан — берём по всем основным типам
        inst_types: List[str]
        if it is None:
            inst_types = ["SPOT", "SWAP", "FUTURES"]
        else:
            inst_types = [it]

        # Собираем историю (без удаления дублей)
        out: List[Dict[str, Any]] = []

        for one_type in inst_types:
            params: Dict[str, Any] = {"instType": one_type, "limit": str(int(limit))}
            raw = await self._request_private("GET", "/trade/fills-history", params=params)

            for item in raw.get("data", []) or []:
                t = self._parse_okx_trade_any(item, is_history=True, inst_type=one_type)
                out.append(t)

        # сортируем по времени
        out.sort(key=lambda x: int(x.get("ts", 0) or 0))
        return out

    #WS: подписка на изменения заявок (private channel orders)
    async def subscribe_orders(
        self,
        symbols: List[str],
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: str = "SPOT",
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        login_msg = self._ws_login_payload()

        inst_type_u = str(inst_type).strip().upper() if inst_type else ""
        if inst_type_u == "SPOT":
            inst_types = ["SPOT", "MARGIN"]
        elif inst_type_u in ("SWAP", "FUTURES", "MARGIN"):
            inst_types = [inst_type_u]
        else:
            inst_types = ["SPOT", "SWAP", "FUTURES", "MARGIN"]

        sub_args_list: List[Dict[str, Any]] = []
        for it in inst_types:
            sub_args_list.append({"channel": "orders", "instType": it})

        sub_msg = {"op": "subscribe", "args": sub_args_list}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_msg))

                    #ждём подтверждение login
                    authed = False
                    login_deadline = asyncio.get_event_loop().time() + 5.0
                    while not authed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > login_deadline:
                            # пробрасываем ошибку наружу, чтобы server.py отправил Astras error envelope
                            if on_error is not None:
                                res = on_error({"event": "error", "code": None, "msg": "OKX WS login timeout"})
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        raw = await ws.recv()
                        msg = json.loads(raw)

                        # OKX может прислать event:error вместо event:login
                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        if msg.get("event") == "login":
                            if msg.get("code") == "0":
                                authed = True
                                break
                            # ошибка логина — отдаём реальный code/msg
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)
                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue
                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            order = self._parse_okx_order_any(item)
                            res = on_data(order)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                # если произошла нештатная ошибка до подтверждения подписки — пробрасываем наружу
                if on_error is not None:
                    try:
                        res = on_error({"event": "error", "code": None, "msg": f"OKX adapter error: {e}"})
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
                return

    #WS: подписка на сделки (исполнения) через private channel orders
    async def subscribe_trades(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: str = "SPOT",
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Подписка на сделки (исполнения) OKX через приватный WS.

        Используется private channel "orders": в сообщениях приходят fill-поля:
        tradeId, fillPx, fillSz, fillTime, fee, feeCcy, ordId, instId, side.

        Возвращает нейтральный формат для server.py (см. _parse_okx_trade_any).
        Поле existing формируется в server.py, здесь есть флаг is_history=False.
        """
        login_msg = self._ws_login_payload()
        # instType может не прийти от Astras. Для OKX private channel `orders` он нужен.
        # Если inst_type не задан — подписываемся сразу на все типы, чтобы получить все сделки по аккаунту.
        inst_type_u = str(inst_type).strip().upper() if inst_type else ""
        if inst_type_u in ("SPOT", "SWAP", "FUTURES"):
            inst_types = [inst_type_u]
        else:
            inst_types = ["SPOT", "SWAP", "FUTURES"]

        sub_args_list: List[Dict[str, Any]] = []
        for it in inst_types:
            sub_args_list.append({"channel": "orders", "instType": it})

        sub_msg = {"op": "subscribe", "args": sub_args_list}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_msg))

                    #ждём подтверждение login
                    authed = False
                    login_deadline = asyncio.get_event_loop().time() + 5.0
                    while not authed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > login_deadline:
                            # пробрасываем ошибку наружу, чтобы server.py отправил Astras error envelope
                            if on_error is not None:
                                res = on_error({"event": "error", "code": None, "msg": "OKX WS login timeout"})
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        raw = await ws.recv()
                        msg = json.loads(raw)

                        # OKX может прислать event:error вместо event:login
                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        if msg.get("event") == "login":
                            if msg.get("code") == "0":
                                authed = True
                                break
                            # ошибка логина — отдаём реальный code/msg
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)
                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue
                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            # сделка (исполнение) определяется наличием tradeId
                            trade_id = item.get("tradeId")
                            if not trade_id:
                                continue

                            # иногда приходит tradeId, но нет fillSz — отфильтруем пустые
                            fill_sz = self._to_float(item.get("fillSz"))
                            if fill_sz <= 0:
                                continue

                            trade = self._parse_okx_trade_any(item, is_history=False, inst_type=inst_type)
                            res = on_data(trade)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                # если произошла нештатная ошибка до подтверждения подписки — пробрасываем наружу
                if on_error is not None:
                    try:
                        res = on_error({"event": "error", "code": None, "msg": f"OKX adapter error: {e}"})
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
                return

    #WS: подписка на стакан (public channel books)
    async def subscribe_order_book(
        self,
        symbol: str,
        depth: int,
        on_data: Callable[[dict], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Подписка на стакан OKX.

        Возвращает нейтральный формат для server.py:
        {
          "symbol": "...",
          "ts": <ms>,
          "bids": [(price, volume), ...],
          "asks": [(price, volume), ...],
          "existing": True|False   # True только для snapshot
        }

        """

        # OKX присылает action: snapshot / update
        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": symbol,
                }
            ],
        }
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_public_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    # локальное состояние стакана: price -> size
                    bids_state: Dict[float, float] = {}
                    asks_state: Dict[float, float] = {}

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)

                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue

                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        action = (msg.get("action") or "").lower()
                        is_snapshot = action == "snapshot"

                        data = msg.get("data")
                        if not data:
                            continue

                        # OKX обычно присылает список с одним элементом
                        for item in data:
                            # timestamp стакана
                            ts_ms = self._to_int(item.get("ts"))

                            # на snapshot мы полностью переинициализируем состояние
                            if is_snapshot:
                                bids_state.clear()
                                asks_state.clear()

                            # применяем дельту update к локальному состоянию
                            self._apply_okx_book_delta(bids_state, item.get("bids") or [])
                            self._apply_okx_book_delta(asks_state, item.get("asks") or [])

                            # формируем полный стакан из состояния
                            bids_full = sorted(bids_state.items(), key=lambda x: x[0], reverse=True)
                            asks_full = sorted(asks_state.items(), key=lambda x: x[0])

                            # existing=True только для snapshot, existing=False для update,
                            # но bids/asks всегда полные (после применения дельты)
                            book = {
                                "symbol": symbol,
                                "ts": ts_ms,
                                "bids": [(float(p), float(v)) for (p, v) in bids_full],
                                "asks": [(float(p), float(v)) for (p, v) in asks_full],
                                "existing": bool(is_snapshot),
                            }

                            res = on_data(book)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception:
                await asyncio.sleep(1.0)
                continue

    #WS: подписка на котировки (public channel tickers)
    async def subscribe_quotes(
        self,
        symbol: str,
        on_data: Callable[[dict], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Подписка на котировки OKX через WS tickers.
        Возвращает нейтральный формат для server.py:
        {
          "symbol": "...",
          "ts": <ms>,
          "last": <float>,
          "bid": <float>,
          "ask": <float>,
          "bid_sz": <float>,
          "ask_sz": <float>,
          "high24h": <float>,
          "low24h": <float>,
          "vol24h": <float>   # объём в базовой валюте за 24ч
        }
        """

        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "tickers",
                    "instId": symbol,
                }
            ],
        }
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_public_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)

                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue

                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            t = self._parse_okx_ticker_any(symbol, item)
                            res = on_data(t)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception:
                await asyncio.sleep(1.0)
                continue

    # REST: денежные остатки и SPOT-позиции
    async def get_account_balances(self) -> List[Dict[str, Any]]:
        """
        OKX REST /account/balance
        Используется для SPOT:
        - валютные остатки
        - позиции по базовой валюте инструмента

        Возвращает нейтральный формат:
        {
          ccy, cashBal, availBal, eq
        }
        """
        raw = await self._request_private("GET", "/account/balance")

        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []) or []:
            for d in item.get("details", []) or []:
                out.append({
                    "ccy": d.get("ccy"),
                    "cashBal": self._to_float(d.get("cashBal")),
                    "availBal": self._to_float(d.get("availBal")),
                    "eq": self._to_float(d.get("eq")),
                })
        return out

    # REST: позиции по деривативам (SWAP / FUTURES)
    async def get_positions(self, inst_type: str = "SWAP") -> List[Dict[str, Any]]:
        """
        OKX REST /account/positions
        Используется для SWAP / FUTURES.

        Возвращает нейтральный формат позиции:
        {
          instId, instType, pos, avgPx, upl, uplRatio
        }
        """
        raw = await self._request_private(
            "GET",
            "/account/positions",
            params={"instType": inst_type},
        )

        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []) or []:
            out.append({
                "instId": item.get("instId"),
                "instType": item.get("instType"),
                "pos": self._to_float(item.get("pos")),
                "avgPx": self._to_float(item.get("avgPx")),
                "upl": self._to_float(item.get("upl")),
                "uplRatio": self._to_float(item.get("uplRatio")),
            })
        return out

    # WS: подписка на позиции и деньги (private)
    async def subscribe_positions_and_balances(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: str = "SPOT",
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        OKX private WS:
        - channel=account   -> деньги / балансы
        - channel=positions -> позиции (SWAP/FUTURES)

        SPOT:
        - используем account
        SWAP/FUTURES:
        - используем positions + account
        """

        login_msg = self._ws_login_payload()

        args: List[Dict[str, Any]] = [{"channel": "account"}]

        inst_type_u = str(inst_type).upper()
        if inst_type_u in ("SWAP", "FUTURES"):
            args.append({"channel": "positions", "instType": inst_type_u})

        sub_msg = {"op": "subscribe", "args": args}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    self._ws_private_url, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(login_msg))

                    # ждём login
                    authed = False
                    deadline = asyncio.get_event_loop().time() + 5.0
                    while not authed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > deadline:
                            if on_error:
                                res = on_error({"event": "error", "msg": "OKX WS login timeout"})
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        msg = json.loads(await ws.recv())
                        if msg.get("event") == "login" and msg.get("code") == "0":
                            authed = True
                            break
                        if msg.get("event") == "error":
                            if on_error:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                    await ws.send(json.dumps(sub_msg))

                    # Ждём реальный event: subscribe от OKX, а не шлём "заглушку"
                    subscribed = False
                    sub_deadline = asyncio.get_event_loop().time() + 5.0
                    while not subscribed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > sub_deadline:
                            if on_error:
                                res = on_error({"event": "error", "msg": "OKX WS subscribe timeout"})
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        raw = await ws.recv()
                        msg = json.loads(raw)

                        if msg.get("event") == "error":
                            if on_error:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        if msg.get("event") == "subscribe":
                            if on_subscribed:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed = True
                            break

                        # Если пришли данные раньше subscribe — не теряем: прокидываем в on_data
                        data = msg.get("data")
                        if data:
                            for item in data:
                                res = on_data(item)
                                if asyncio.iscoroutine(res):
                                    await res

                    # основной цикл чтения данных
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)
                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            res = on_data(item)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                if on_error:
                    res = on_error({"event": "error", "msg": str(e)})
                    if asyncio.iscoroutine(res):
                        await res
                await asyncio.sleep(1.0)
                return

    def _parse_okx_account_balance_any(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """OKX private WS/REST `account` channel/balance endpoint -> currency positions."""
        out: List[Dict[str, Any]] = []
        details = item.get("details") or []
        for d in details:
            ccy = (d.get("ccy") or "").strip()
            if not ccy:
                continue

            cash_bal = d.get("cashBal")
            eq = d.get("eq")

            # cashBal is usually the total balance for SPOT margin-less accounts.
            # If cashBal is absent, fall back to eq.
            qty = self._to_float(cash_bal if cash_bal is not None else eq)

            out.append(
                {
                    "symbol": ccy,
                    "qtyUnits": qty,
                    "avgPrice": None,
                    "currentPrice": None,
                    "volume": None,
                    "currentVolume": None,
                    "lotSize": 0.0,
                    "shortName": ccy,
                    "isCurrency": True,
                }
            )
        return out

    def _parse_okx_position_any(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """OKX private WS/REST `positions` -> instrument positions."""
        inst_id = (d.get("instId") or "").strip()
        pos = self._to_float(d.get("pos"))
        avg_px = self._to_float(d.get("avgPx"))

        # OKX often provides markPx; last may be absent on private feed.
        cur_px_raw = d.get("markPx")
        if cur_px_raw is None:
            cur_px_raw = d.get("last")
        cur_px = self._to_float(cur_px_raw)

        vol = (avg_px * pos) if avg_px > 0 else None
        cur_vol = (cur_px * pos) if cur_px > 0 else None

        return {
            "symbol": inst_id,
            "qtyUnits": pos,
            "avgPrice": avg_px,
            "currentPrice": cur_px,
            "volume": vol,
            "currentVolume": cur_vol,
            "lotSize": 0.0,
            "shortName": inst_id,
            "isCurrency": False,
        }

    async def get_positions_snapshot(self, inst_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Snapshot positions for Astras PositionsGetAndSubscribeV2.

        Всегда возвращает:
        - SPOT: валютные остатки (account/balance)
        - FUTURES + SWAP: позиции по инструментам (account/positions)

        inst_type из Astras игнорируется — по нашей схеме всегда отдаём все 3 типа.
        """

        out: List[Dict[str, Any]] = []

        # 1) SPOT — валютные остатки
        try:
            bal = await self._request_private("GET", "/account/balance")
            data = bal.get("data") or []
            if data:
                out.extend(self._parse_okx_account_balance_any(data[0] or {}))
        except Exception:
            pass

        # 2) FUTURES и SWAP — позиции по деривативам
        for itype in ("FUTURES", "SWAP"):
            try:
                pos = await self._request_private(
                    "GET",
                    "/account/positions",
                    params={"instType": itype},
                )
                for it in (pos.get("data") or []):
                    out.append(self._parse_okx_position_any(it or {}))
            except Exception:
                continue

        return out

    async def subscribe_positions(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: Optional[str] = None,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Private WS subscribe for positions.

        We keep the WS connection alive and forward:
        - `account` channel -> currency balances (isCurrency=True)
        - `positions` channel (for FUTURES/SWAP) -> instrument positions (isCurrency=False)

        `server.py` decides how to mark these as existing/live.
        """
        inst_type_s = (str(inst_type).strip().upper() if inst_type else "SPOT")

        # OKX private channels must use private WS URL
        self._assert_private_ws("account", self._ws_private_url)
        if inst_type_s in ("FUTURES", "SWAP"):
            self._assert_private_ws("positions", self._ws_private_url)

        async def _call(cb, arg):
            if cb is None:
                return
            try:
                r = cb(arg)
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                return

        login_msg = self._ws_login_payload()

        sub_args: List[Dict[str, Any]] = [{"channel": "account"}]
        if inst_type_s in ("FUTURES", "SWAP"):
            sub_args.append({"channel": "positions", "instType": inst_type_s})

        sub_msg = {"op": "subscribe", "args": sub_args}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    # login
                    await ws.send(json.dumps(login_msg))

                    # wait login event
                    authed = False
                    while not stop_event.is_set() and not authed:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "login":
                            if m.get("code") == "0":
                                authed = True
                                break
                            await _call(on_error, m)
                            return

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                    if not authed:
                        continue

                    # subscribe
                    await ws.send(json.dumps(sub_msg))

                    # Ждём реальный event: subscribe от OKX, а не "заглушку"
                    subscribed = False
                    sub_deadline = asyncio.get_event_loop().time() + 5.0
                    while not subscribed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > sub_deadline:
                            await _call(on_error, {"event": "error", "msg": "OKX WS subscribe timeout"})
                            return

                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "subscribe":
                            await _call(on_subscribed, m)
                            subscribed = True
                            break

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        # Если прилетели данные до subscribe — обработаем их, чтобы не потерять
                        arg = m.get("arg") or {}
                        ch = arg.get("channel")
                        data = m.get("data")
                        if ch and data:
                            if ch == "account":
                                for it in (data or []):
                                    for pos in self._parse_okx_account_balance_any(it or {}):
                                        r = on_data(pos)
                                        if asyncio.iscoroutine(r):
                                            await r
                            elif ch == "positions":
                                for it in (data or []):
                                    pos = self._parse_okx_position_any(it or {})
                                    r = on_data(pos)
                                    if asyncio.iscoroutine(r):
                                        await r

                    # основной цикл чтения данных
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")

                        m = json.loads(raw)

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        arg = m.get("arg") or {}
                        ch = arg.get("channel")
                        data = m.get("data")
                        if not ch or not data:
                            continue

                        if ch == "account":
                            # data is a list, each item contains `details`
                            for it in (data or []):
                                for pos in self._parse_okx_account_balance_any(it or {}):
                                    r = on_data(pos)
                                    if asyncio.iscoroutine(r):
                                        await r
                            continue

                        if ch == "positions":
                            for it in (data or []):
                                pos = self._parse_okx_position_any(it or {})
                                r = on_data(pos)
                                if asyncio.iscoroutine(r):
                                    await r
                            continue

                    if stop_event.is_set() and subscribed:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                await _call(on_error, {"event": "error", "msg": str(e)})
                await asyncio.sleep(1.0)
                continue
    
    # Summaries (сводная информация по "портфелю" = аккаунту OKX)
    def _parse_okx_account_summary_any(self, acc: Dict[str, Any]) -> Dict[str, Any]:
        total_eq = self._to_float(acc.get("totalEq"))
        avail_eq = self._to_float(acc.get("availEq"))
        imr = self._to_float(acc.get("imr"))
        mmr = self._to_float(acc.get("mmr"))
        by_ccy: List[Dict[str, Any]] = []
        for d in (acc.get("details") or []):
            ccy = (d.get("ccy") or "").strip()
            if not ccy:
                continue
            by_ccy.append(
                {
                    "ccy": ccy,
                    "availEq": self._to_float(d.get("availEq")),
                    "imr": self._to_float(d.get("imr")),
                }
            )
        return {
            "totalEq": total_eq,
            "availEq": avail_eq,
            "imr": imr,
            "mmr": mmr,
            "byCcy": by_ccy,
        }

    async def get_summaries_snapshot(self) -> Dict[str, Any]:
        raw = await self._request_private("GET", "/account/balance")
        data = raw.get("data") or []
        if not data:
            raise RuntimeError("OKX /account/balance returned empty data")
        return self._parse_okx_account_summary_any(data[0] or {})

    async def subscribe_summaries(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Live-подписка для Summaries (нейтральный формат) через OKX private WS channel=account.

        OKX не присылает отдельного "portfolio summary" канала — берём account updates.
        Каждый update маппим в нейтральную сводку OKX и отдаём в server.py.
        """

        self._assert_private_ws("account", self._ws_private_url)

        async def _call(cb, arg):
            if cb is None:
                return
            try:
                r = cb(arg)
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                return

        login_msg = self._ws_login_payload()
        sub_msg = {"op": "subscribe", "args": [{"channel": "account"}]}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    # login
                    await ws.send(json.dumps(login_msg))

                    authed = False
                    while not stop_event.is_set() and not authed:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "login":
                            if m.get("code") == "0":
                                authed = True
                                break
                            await _call(on_error, m)
                            return

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                    if not authed:
                        continue

                    await ws.send(json.dumps(sub_msg))

                    # Ждём реальный event: subscribe от OKX, а не "заглушку"
                    subscribed = False
                    sub_deadline = asyncio.get_event_loop().time() + 5.0
                    while not subscribed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > sub_deadline:
                            await _call(on_error, {"event": "error", "msg": "OKX WS subscribe timeout"})
                            return

                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "subscribe":
                            # summaries подтверждаем через on_subscribed, если оно передано
                            # (server.py решает, нужно ли это Astras)
                            await _call(on_subscribed, m)
                            subscribed = True
                            break

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        # Если обновления account пришли раньше subscribe — обработаем
                        arg = m.get("arg") or {}
                        if arg.get("channel") == "account":
                            data = m.get("data") or []
                            if data:
                                summary = self._parse_okx_account_summary_any(data[0] or {})
                                r = on_data(summary)
                                if asyncio.iscoroutine(r):
                                    await r

                    # основной цикл чтения данных
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")

                        m = json.loads(raw)

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        arg = m.get("arg") or {}
                        if arg.get("channel") != "account":
                            continue

                        data = m.get("data") or []
                        # OKX возвращает data как список; берём первый элемент
                        if not data:
                            continue

                        summary = self._parse_okx_account_summary_any(data[0] or {})
                        r = on_data(summary)
                        if asyncio.iscoroutine(r):
                            await r

                    if stop_event.is_set() and subscribed:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                await _call(on_error, {"event": "error", "msg": str(e)})
                await asyncio.sleep(1.0)
                continue
