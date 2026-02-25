import time
from typing import Any


_TTL_SEC = 60 * 60
_CLEANUP_INTERVAL_SEC = 10 * 60
_LAST_CLEANUP_TS = 0.0

_CWS_MARKET_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_CWS_LIMIT_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_CWS_DELETE_MARKET_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_CWS_DELETE_LIMIT_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_CWS_UPDATE_LIMIT_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_CWS_ORDER_SYMBOL_BY_ID: dict[str, tuple[float, str]] = {}


def maybe_cleanup() -> None:
    global _LAST_CLEANUP_TS
    now = time.time()
    if now - _LAST_CLEANUP_TS < _CLEANUP_INTERVAL_SEC:
        return

    cutoff = now - _TTL_SEC
    for cache in (
        _CWS_MARKET_IDEMPOTENCY,
        _CWS_LIMIT_IDEMPOTENCY,
        _CWS_DELETE_MARKET_IDEMPOTENCY,
        _CWS_DELETE_LIMIT_IDEMPOTENCY,
        _CWS_UPDATE_LIMIT_IDEMPOTENCY,
        _CWS_ORDER_SYMBOL_BY_ID,
    ):
        for key, (ts, _) in list(cache.items()):
            if ts < cutoff:
                cache.pop(key, None)

    _LAST_CLEANUP_TS = now


def get_cached_payload(
    cache: dict[str, tuple[float, dict[str, Any]]],
    key: str,
) -> dict[str, Any] | None:
    item = cache.get(key)
    if not item:
        return None
    ts, payload = item
    if time.time() - ts > _TTL_SEC:
        cache.pop(key, None)
        return None
    return payload


def put_cached_payload(
    cache: dict[str, tuple[float, dict[str, Any]]],
    key: str,
    payload: dict[str, Any],
) -> None:
    cache[key] = (time.time(), payload)


def get_order_symbol(
    cache: dict[str, tuple[float, str]],
    order_id: str,
) -> str:
    item = cache.get(order_id)
    if not item:
        return ""
    ts, symbol = item
    if time.time() - ts > _TTL_SEC:
        cache.pop(order_id, None)
        return ""
    return symbol


def set_order_symbol(
    cache: dict[str, tuple[float, str]],
    order_id: str,
    symbol: str,
) -> None:
    cache[order_id] = (time.time(), symbol)


def pop_order_symbol(
    cache: dict[str, tuple[float, str]],
    order_id: str,
) -> None:
    cache.pop(order_id, None)


def cws_market_idempotency() -> dict[str, tuple[float, dict[str, Any]]]:
    return _CWS_MARKET_IDEMPOTENCY


def cws_limit_idempotency() -> dict[str, tuple[float, dict[str, Any]]]:
    return _CWS_LIMIT_IDEMPOTENCY


def cws_delete_market_idempotency() -> dict[str, tuple[float, dict[str, Any]]]:
    return _CWS_DELETE_MARKET_IDEMPOTENCY


def cws_delete_limit_idempotency() -> dict[str, tuple[float, dict[str, Any]]]:
    return _CWS_DELETE_LIMIT_IDEMPOTENCY


def cws_update_limit_idempotency() -> dict[str, tuple[float, dict[str, Any]]]:
    return _CWS_UPDATE_LIMIT_IDEMPOTENCY


def cws_order_symbol_by_id() -> dict[str, tuple[float, str]]:
    return _CWS_ORDER_SYMBOL_BY_ID
