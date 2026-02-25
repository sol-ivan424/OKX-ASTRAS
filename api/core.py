import hashlib
import os
import uuid
from typing import Optional

from adapters.okx import OkxAdapter
from dotenv import load_dotenv

load_dotenv()


def _make_adapter():
    name = os.getenv("ADAPTER", "okx").lower()
    if name == "okx":
        return OkxAdapter(
            api_key=os.getenv("OKX_API_KEY"),
            api_secret=os.getenv("OKX_API_SECRET"),
            api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
            demo=os.getenv("OKX_DEMO", "0") in ("1", "true", "True", "yes", "YES"),
        )
    raise RuntimeError("Поддерживается только ADAPTER=okx")


adapter = _make_adapter()
USER_SETTINGS: dict[str, str] = {}
SUPPORTED_BOARDS = ["SPOT", "FUTURES", "SWAP"]


def okx_client_id(guid: str | None) -> str:
    raw = str(guid or "").strip()
    if not raw:
        return uuid.uuid4().hex[:32]
    try:
        return uuid.UUID(raw).hex[:32]
    except Exception:
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


async def warmup_okx():
    await adapter.warmup()


async def resolve_order_ccy(
    symbol: Optional[str],
    inst_type_s: str,
    side: str,
    allow_margin: bool,
) -> Optional[str]:
    if not isinstance(symbol, str) or not symbol.strip():
        return None

    side_s = str(side or "").lower()
    if inst_type_s == "SPOT":
        if not allow_margin:
            return None
        parts = [p.strip().upper() for p in symbol.split("-")]
        if len(parts) < 2:
            return None
        return parts[1] if side_s == "buy" else (parts[0] if side_s == "sell" else None)

    if inst_type_s in ("FUTURES", "SWAP"):
        from api import instruments_cache

        instr = await instruments_cache.get_instr(symbol, adapter=adapter)
        ccy = (instr or {}).get("quoteCcy")
        if ccy is not None and str(ccy).strip():
            return str(ccy).strip().upper()
    return None
