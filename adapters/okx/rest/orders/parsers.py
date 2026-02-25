from datetime import datetime, timezone
from typing import Any, Dict, Optional


class OkxRestOrderParsersMixin:
    def _parse_okx_order_any(self, d: Dict[str, Any]) -> Dict[str, Any]:
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

    def _parse_okx_trade_any(
        self,
        d: Dict[str, Any],
        is_history: bool,
        inst_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        trade_id = d.get("tradeId") or d.get("billId") or "0"
        ord_id = d.get("ordId") or "0"
        inst_id = d.get("instId") or "0"
        side = d.get("side") or "0"
        fill_px = self._to_float(d.get("fillPx") if d.get("fillPx") is not None else d.get("px"))
        fill_sz = self._to_float(d.get("fillSz") if d.get("fillSz") is not None else d.get("sz"))
        ts_ms = self._to_int(
            d.get("fillTime") if d.get("fillTime") is not None else d.get("ts")
        )
        if ts_ms <= 0:
            ts_ms = self._to_int(d.get("uTime") if d.get("uTime") is not None else d.get("cTime"))
        fee = d.get("fee")
        commission = abs(self._to_float(fee)) if fee is not None else 0.0
        fee_ccy = d.get("feeCcy") or "0"
        value = fill_px * fill_sz
        volume = value
        inst_type_s = str(inst_type or d.get("instType") or "").upper() or None
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
            "qtyUnits": fill_sz,
            "qty": fill_sz,
            "ts": ts_ms,
            "commission": commission,
            "feeCcy": fee_ccy,
            "volume": volume,
            "value": value,
            "is_history": bool(is_history),
        }
