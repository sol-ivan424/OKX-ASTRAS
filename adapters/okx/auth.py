import base64
import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any, Dict


class OkxAuthMixin:
    def _hmac_sha256_base64(self, message: str) -> str:
        if not self._api_secret:
            raise RuntimeError("OKX API secret не задан")

        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    def _rest_timestamp(self) -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    def _ws_timestamp(self) -> str:
        return str(int(datetime.now(timezone.utc).timestamp()))

    def _sign_request(self, timestamp: str, method: str, request_path: str, body: str) -> str:
        message = timestamp + method.upper() + request_path + body
        return self._hmac_sha256_base64(message)

    def _ws_login_payload(self) -> Dict[str, Any]:

        if not self._api_key or not self._api_secret or not self._api_passphrase:
            raise RuntimeError("OKX API ключи не заданы")

        ts = self._ws_timestamp()
        prehash = ts + "GET" + "/users/self/verify"
        sign = self._hmac_sha256_base64(prehash)

        return {
            "op": "login",
            "args": [
                {
                    "apiKey": self._api_key,
                    "passphrase": self._api_passphrase,
                    "timestamp": ts,
                    "sign": sign,
                }
            ],
        }
