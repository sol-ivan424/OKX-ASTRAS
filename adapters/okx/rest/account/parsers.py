from typing import Any, Dict, List


class OkxRestAccountParsersMixin:
    def _parse_okx_account_balance_any(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        details = item.get("details") or []
        for d in details:
            ccy = (d.get("ccy") or "").strip()
            if not ccy:
                continue

            cash_bal = d.get("cashBal")
            eq = d.get("eq")
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
        inst_id = (d.get("instId") or "").strip()
        pos = self._to_float(d.get("pos"))
        avg_px = self._to_float(d.get("avgPx"))

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

    def _parse_okx_account_summary_any(self, acc: Dict[str, Any]) -> Dict[str, Any]:
        total_eq = self._to_float(acc.get("totalEq"))
        adj_eq = self._to_float(acc.get("adjEq"))
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
            "adjEq": adj_eq,
            "availEq": avail_eq,
            "imr": imr,
            "mmr": mmr,
            "byCcy": by_ccy,
        }
