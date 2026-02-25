from typing import Any, Dict, Optional


class OkxRestMarketParsersMixin:
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

    def _tf_to_okx_ws_channel(self, tf: str) -> str:
        return "candle" + self._tf_to_okx_bar(tf)

    def _parse_okx_candle_any(self, symbol: str, arr: list, inst_type: Optional[str] = None) -> dict:
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
        inst_type_u = str(inst_type).upper().strip()
        vol_raw = _get(arr, 5, 0.0)
        vol_ccy_raw = _get(arr, 6, None)
        if inst_type_u in ("FUTURES", "SWAP") and vol_ccy_raw is not None:
            vol = self._to_float(vol_ccy_raw)
        else:
            vol = self._to_float(vol_raw) if vol_raw is not None else 0.0
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

    def _parse_okx_order_book_any(self, symbol: str, item: Dict[str, Any], existing: bool) -> dict:
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
