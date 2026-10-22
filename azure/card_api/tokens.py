"""OAuth 2.0 client credentials, versión mínima: tokens firmados con HMAC que vencen a los 15 minutos.

Un token es `base64(payload).firma`. No hace falta una base de datos: la Function verifica la firma con su clave.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

TOKEN_TTL_SECONDS = 900


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def issue(client_id: str, signing_key: str, now: float | None = None, ttl: int = TOKEN_TTL_SECONDS) -> dict:
    now = time.time() if now is None else now
    payload = _b64(json.dumps({"cid": client_id, "exp": int(now) + ttl}).encode())
    sig = _b64(hmac.new(signing_key.encode(), payload.encode(), hashlib.sha256).digest())
    return {"access_token": f"{payload}.{sig}", "token_type": "Bearer", "expires_in": ttl}


def verify(token: str, signing_key: str, now: float | None = None) -> tuple[bool, str]:
    """(válido, motivo). Motivos: ok, malformed, bad_signature, token_expired."""
    now = time.time() if now is None else now
    try:
        payload, sig = token.split(".")
    except ValueError:
        return False, "malformed"
    expected = _b64(hmac.new(signing_key.encode(), payload.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        return False, "bad_signature"
    if json.loads(_unb64(payload))["exp"] < now:
        return False, "token_expired"
    return True, "ok"
