import uuid
from typing import Any, Dict, Optional


class OkxWsOrderCancelMixin:
    async def cancel_order_ws(
        self,
        symbol: str,
        order_id: str,
        inst_type: Optional[str] = None,
        ws_request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"ordId": str(order_id)}
        if isinstance(symbol, str) and symbol.strip():
            body["instId"] = str(symbol).strip()
            if inst_type is not None and str(inst_type).strip():
                inst_id_code = await self._resolve_inst_id_code(str(symbol).strip(), str(inst_type).strip().upper())
                if inst_id_code:
                    body["instIdCode"] = str(inst_id_code)

        req_id = str(ws_request_id or f"cancel-{uuid.uuid4().hex}")
        msg = await self._cancel_order_via_private_ws(body, req_id)

        code = str(msg.get("code") or "")
        if code and code != "0":
            err_msg = str(msg.get("msg") or "").strip()
            err_items = msg.get("data") or []
            err_it0 = err_items[0] if err_items else {}
            err_s_code = str((err_it0 or {}).get("sCode") or "")
            err_s_msg = str((err_it0 or {}).get("sMsg") or "")
            if err_s_code or err_s_msg:
                raise RuntimeError(
                    f"OKX WS cancel-order error {code}: {err_msg} (sCode={err_s_code or 'n/a'}, sMsg={err_s_msg})"
                )
            raise RuntimeError(f"OKX WS cancel-order error {code}: {err_msg}")

        items = msg.get("data") or []
        if not items:
            raise RuntimeError("OKX WS cancel-order: empty response data")

        it0 = items[0] or {}
        s_code = str(it0.get("sCode") or "")
        s_msg = str(it0.get("sMsg") or "")
        if s_code and s_code != "0":
            raise RuntimeError(f"OKX cancel-order error {s_code}: {s_msg}")

        return {
            "ordId": str(it0.get("ordId") or order_id or "0"),
            "clOrdId": str(it0.get("clOrdId") or ""),
            "sCode": s_code or "0",
            "sMsg": s_msg,
        }
