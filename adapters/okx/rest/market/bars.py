from typing import Any, Dict, List, Optional


class OkxRestMarketBarsMixin:
    async def get_bars_history(
        self,
        symbol: str,
        tf: str,
        from_ts: int,
        inst_type: Optional[str] = None,
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
                b = self._parse_okx_candle_any(symbol, arr, inst_type=inst_type)
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
