from typing import Any, Dict, List


class OkxRestMarketTickersMixin:
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        raw = await self._request_public(
            path="/market/ticker",
            params={"instId": symbol},
        )
        items = raw.get("data") or []
        if not items:
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

    async def list_tickers(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        raw = await self._request_public(
            path="/market/tickers",
            params={"instType": inst_type},
        )
        out: List[Dict[str, Any]] = []
        for item in raw.get("data", []) or []:
            sym = item.get("instId") or "0"
            out.append(self._parse_okx_ticker_any(sym, item))
        return out
