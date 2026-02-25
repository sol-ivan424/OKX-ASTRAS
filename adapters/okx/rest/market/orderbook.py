from typing import Any, Dict


class OkxRestMarketOrderBookMixin:
    async def get_order_book_rest(self, symbol: str, depth: int = 20) -> Dict[str, Any]:
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
        return self._parse_okx_order_book_any(symbol, items[0], existing=True)

    async def get_quote_snapshot_rest(self, symbol: str, book_depth: int = 20) -> Dict[str, Any]:
        t = await self.get_ticker(symbol)
        try:
            ob = await self.get_order_book_rest(symbol, depth=book_depth)
        except Exception:
            ob = {"ts": 0, "bids": [], "asks": []}

        ob_ts = int(ob.get("ts", 0) or 0)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
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
        return {
            **t,
            "ob_ts": ob_ts,
            "total_bid_vol": total_bid_vol,
            "total_ask_vol": total_ask_vol,
        }
