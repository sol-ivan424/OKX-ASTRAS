import uuid
from typing import Any, Dict, Optional


class OkxWsOrderCreateMixin:
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
        inst_id = symbol
        side_s = str(side or "").lower().strip()

        def _fmt_sz(x: Any) -> str:
            try:
                f = float(x)
            except Exception:
                return "" if x is None else str(x)
            s = f"{f:.16f}".rstrip("0").rstrip(".")
            return s if s else "0"

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
        msg = await self._place_order_via_private_ws(body, req_id)

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
        msg = await self._place_order_via_private_ws(body, req_id)

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
