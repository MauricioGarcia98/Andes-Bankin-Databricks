"""Cliente HTTP robusto para la API del procesador (y cualquier API con OAuth client_credentials).

Lo que resuelve, problema por problema (docs/03, sección A):

  API-01  429            espera lo que dice `Retry-After` (+ un poco de azar para no volver todos juntos)
  API-02  5xx            backoff exponencial con *jitter*: 1, 2, 4, 8... segundos, con tope
  API-03  timeout        el pedido es idempotente (GET): se reintenta igual
  API-04  401 vencido    renueva el token y repite EL MISMO pedido (mismo cursor), sin contar como intento fallido
  API-10  JSON cortado   se trata como falla transitoria y se reintenta
  API-12  200 con error  se valida el cuerpo, no sólo el código HTTP
  API-05  página repetida → dedupe por id dentro del día
  API-06  página salteada → se concilia contra los totales del día; si falta algo, se recorre el día otra vez

Cada pedido (incluidos los fallidos) se registra en `calls`, que después va a `ops.api_calls`.
"""

from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone


class ApiError(Exception):
    """Falla no recuperable (o reintentos agotados)."""


@dataclass
class DayResult:
    resource: str
    day: str
    records: dict[str, dict]
    expected: int
    passes: int
    pages_read: int
    duplicates_seen: int

    @property
    def complete(self) -> bool:
        return len(self.records) == self.expected


@dataclass
class ApiClient:
    base_url: str
    client_id: str
    client_secret: str
    run_id: str = ""
    timeout: float = 30.0
    max_retries: int = 8
    max_backoff: float = 30.0
    session: object = None
    sleep: object = time.sleep
    clock: object = time.time
    rng: random.Random = field(default_factory=random.Random)
    calls: list[dict] = field(default_factory=list)

    def __post_init__(self):
        if self.session is None:
            import requests

            self.session = requests.Session()
        self._token: str | None = None
        self._token_exp = 0.0
        self._lock = threading.RLock()   # reentrante: _bearer() renueva el token y _log() vuelve a tomarlo

    # ------------------------------------------------------------------------------------------ token
    def _refresh_token(self) -> None:
        r = self.session.post(f"{self.base_url}/oauth/token", timeout=self.timeout, data={
            "grant_type": "client_credentials", "client_id": self.client_id, "client_secret": self.client_secret})
        self._log("POST", "/oauth/token", {}, r.status_code, 1, 0.0, None if r.status_code == 200 else r.text[:200], 0)
        if r.status_code != 200:
            raise ApiError(f"No se pudo obtener el token: {r.status_code} {r.text[:200]}")
        body = r.json()
        self._token = body["access_token"]
        self._token_exp = self.clock() + int(body.get("expires_in", 900))

    def _bearer(self, force: bool = False) -> str:
        with self._lock:
            # renovación anticipada: si le quedan menos de 60 s, se pide uno nuevo antes de usarlo
            if force or self._token is None or self._token_exp - self.clock() < 60:
                self._refresh_token()
            return self._token

    # ---------------------------------------------------------------------------------------- logging
    def _log(self, method, path, params, status, attempt, latency, error, rows) -> None:
        with self._lock:
            self.calls.append({
                "run_id": self.run_id, "called_at": datetime.now(timezone.utc), "method": method, "endpoint": path,
                "params": json.dumps({k: v for k, v in params.items() if k != "cursor"} | ({"cursor": "…"} if "cursor" in params else {})),
                "status_code": status, "attempt": attempt, "latency_ms": int(latency * 1000), "error": error, "rows": rows,
            })

    def _backoff(self, attempt: int) -> float:
        return min(self.max_backoff, 2 ** (attempt - 1)) * (0.5 + self.rng.random())   # "full jitter" acotado

    # ---------------------------------------------------------------------------------------- pedido
    def get(self, path: str, params: dict | None = None) -> dict:
        params = dict(params or {})
        attempt, token_refreshes = 0, 0
        while True:
            attempt += 1
            if attempt > self.max_retries:
                raise ApiError(f"Reintentos agotados en {path} {params}")
            t0 = self.clock()
            try:
                r = self.session.get(f"{self.base_url}/{path.lstrip('/')}", params=params, timeout=self.timeout,
                                     headers={"Authorization": f"Bearer {self._bearer()}"})
            except Exception as exc:                                       # API-03 (timeout) y cortes de red
                self._log("GET", path, params, None, attempt, self.clock() - t0, f"{type(exc).__name__}", 0)
                self.sleep(self._backoff(attempt))
                continue
            latency = self.clock() - t0
            if r.status_code == 401 and token_refreshes < 3:              # API-04
                self._log("GET", path, params, 401, attempt, latency, r.text[:200], 0)
                self._bearer(force=True)
                token_refreshes += 1
                attempt -= 1                                                # renovar no gasta un intento
                continue
            if r.status_code == 429:                                       # API-01
                wait = float(r.headers.get("Retry-After", 1)) + self.rng.random()
                self._log("GET", path, params, 429, attempt, latency, f"retry_after={wait:.1f}", 0)
                self.sleep(wait)
                continue
            if r.status_code >= 500:                                       # API-02
                self._log("GET", path, params, r.status_code, attempt, latency, r.text[:200], 0)
                self.sleep(self._backoff(attempt))
                continue
            if r.status_code != 200:
                self._log("GET", path, params, r.status_code, attempt, latency, r.text[:200], 0)
                raise ApiError(f"{r.status_code} en {path}: {r.text[:300]}")
            try:
                body = r.json()
            except ValueError:                                             # API-10
                self._log("GET", path, params, 200, attempt, latency, "invalid_json", 0)
                self.sleep(self._backoff(attempt))
                continue
            if isinstance(body, dict) and body.get("status") == "error":   # API-12
                self._log("GET", path, params, 200, attempt, latency, f"error_in_body: {body.get('message')}", 0)
                self.sleep(self._backoff(attempt))
                continue
            rows = len(body.get("data", body.get("items", body.get("results", [])))) if isinstance(body, dict) else 0
            self._log("GET", path, params, 200, attempt, latency, None, rows)
            return body

    # ------------------------------------------------------------------------------ un día, completo
    def fetch_day(self, resource: str, day: str, id_field: str, date_param: str, max_passes: int = 4) -> DayResult:
        """Recorre el día siguiendo el cursor hasta tener TODOS los registros que dicen los totales."""
        expected = self.get(f"v2/{resource}/daily-totals", {date_param: day})["records"]
        records: dict[str, dict] = {}
        passes = pages = dups = 0
        while passes < max_passes and len(records) < expected:
            passes += 1
            params: dict = {date_param: day}
            while True:
                body = self.get(f"v2/{resource}", params)
                pages += 1
                for rec in body["data"]:
                    if rec[id_field] in records:
                        dups += 1                                          # API-05
                    records[rec[id_field]] = rec
                if not body.get("has_more"):
                    break
                params = {"cursor": body["next_cursor"]}
        return DayResult(resource, day, records, expected, passes, pages, dups)
