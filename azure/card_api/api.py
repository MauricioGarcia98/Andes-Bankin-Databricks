"""La API de "Andes Card Processing" (el procesador de tarjetas), el espejo del BCRA y la API del CRM comercial,
sin depender de Azure.

`handle(request)` recibe un pedido HTTP ya parseado y devuelve (status, headers, body). `function_app.py` sólo
traduce entre esto y Azure Functions; así toda la lógica (y el caos) se prueba con pytest, sin nube.

Endpoints
---------
POST /oauth/token                                  client_credentials → token Bearer de 15 minutos
GET  /v2/authorizations?processing_date=YYYY-MM-DD[&cursor=...]      paginado por cursor (1.000 por página)
GET  /v2/settlements?settlement_date=YYYY-MM-DD[&cursor=...]
GET  /v2/authorizations/daily-totals?processing_date=...             para conciliar (cantidad y suma)
GET  /v2/settlements/daily-totals?settlement_date=...
GET  /v1/authorizations?processing_date=...&offset=0&limit=200       legacy: offset/limit y montos en centavos
GET  /fx/estadisticascambiarias/v1.0/Cotizaciones/USD?fechadesde=&fechahasta=&offset=&limit=   espejo del BCRA
GET  /crm/v1/<Objeto>?modified_since=<ISO>[&cursor=...]    CRM: registros (borrados incluidos) modificados desde
GET  /health
"""

from __future__ import annotations

import base64
import calendar
import json
import time
from dataclasses import dataclass, field

import tokens
from chaos import Chaos
from store import PAGE_SIZE

V2_LAUNCH = "2025-04-01"
DATE_PARAM = {"authorizations": "processing_date", "settlements": "settlement_date"}
AMOUNT_FIELD = {"authorizations": "billing_amount_ars", "settlements": "settled_billing_ars"}
CRM_OBJECTS = ("Account", "Contact", "Lead", "Opportunity", "Case")
CRM_PAGE_SIZE = 500


@dataclass
class Request:
    method: str
    path: str
    params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    form: dict = field(default_factory=dict)


@dataclass
class Settings:
    client_id: str
    client_secret: str
    signing_key: str


def _json(status: int, body, headers: dict | None = None) -> tuple[int, dict, str]:
    h = {"Content-Type": "application/json"}
    h.update(headers or {})
    return status, h, json.dumps(body, ensure_ascii=False)


def _encode_cursor(c: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(c, separators=(",", ":")).encode()).decode().rstrip("=")


def _decode_cursor(s: str) -> dict | None:
    try:
        return json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))
    except Exception:
        return None


def _valid_day(day: str | None) -> bool:
    try:
        time.strptime(day or "", "%Y-%m-%d")
        return True
    except ValueError:
        return False


class CardApi:
    def __init__(self, store, fx_store, settings: Settings, chaos: Chaos, sleep=time.sleep, crm_store=None):
        self.store = store
        self.fx_store = fx_store
        self.crm_store = crm_store
        self.settings = settings
        self.chaos = chaos
        self.sleep = sleep

    # ------------------------------------------------------------------------------------------ entrada
    def handle(self, req: Request) -> tuple[int, dict, str]:
        path = "/" + req.path.strip("/")
        if path == "/health":
            return _json(200, {"status": "ok", "chaos": self.chaos.profile.__dict__})
        if path == "/oauth/token" and req.method == "POST":
            return self._token(req)
        if path.startswith("/fx/"):
            return self._guarded(req, lambda: self._fx(req, path))
        parts = path.strip("/").split("/")
        if len(parts) == 3 and parts[:2] == ["crm", "v1"]:
            return self._guarded(req, lambda: self._crm(req, parts[2]), pageable=True)
        if len(parts) >= 2 and parts[0] == "v2" and parts[1] in DATE_PARAM:
            if len(parts) == 3 and parts[2] == "daily-totals":
                return self._guarded(req, lambda: self._totals(req, parts[1]))
            if len(parts) == 2:
                return self._guarded(req, lambda: self._page_v2(req, parts[1]), pageable=True)
        if parts == ["v1", "authorizations"]:
            return self._guarded(req, lambda: self._page_v1(req))
        return _json(404, {"error": "not_found", "message": f"No existe {req.method} {path}"})

    # ------------------------------------------------------------------------------ autenticación
    def _token(self, req: Request):
        cid, secret = req.form.get("client_id"), req.form.get("client_secret")
        if req.form.get("grant_type", "client_credentials") != "client_credentials":
            return _json(400, {"error": "unsupported_grant_type"})
        if cid != self.settings.client_id or secret != self.settings.client_secret:
            return _json(401, {"error": "invalid_client"})
        return _json(200, tokens.issue(cid, self.settings.signing_key))

    def _auth_error(self, req: Request):
        header = req.headers.get("authorization") or req.headers.get("Authorization") or ""
        if not header.startswith("Bearer "):
            return _json(401, {"error": "missing_token"}, {"WWW-Authenticate": "Bearer"})
        ok, reason = tokens.verify(header[7:], self.settings.signing_key)
        if not ok:
            return _json(401, {"error": reason}, {"WWW-Authenticate": f'Bearer error="{reason}"'})
        if self.chaos.roll(self.chaos.profile.early_expiry):                         # API-04
            return _json(401, {"error": "token_expired"}, {"WWW-Authenticate": 'Bearer error="token_expired"'})
        return None

    # ---------------------------------------------------------------------------- caos transitorio
    def _guarded(self, req: Request, fn, pageable: bool = False):
        err = self._auth_error(req)
        if err:
            return err
        p = self.chaos.profile
        if self.chaos.roll(p.rate_limit):                                              # API-01
            wait = self.chaos.rng.randint(2, 8)
            return _json(429, {"error": "rate_limited", "message": "Demasiados pedidos"}, {"Retry-After": str(wait)})
        if self.chaos.roll(p.server_error):                                            # API-02
            code = self.chaos.rng.choice([500, 502, 503])
            return _json(code, {"error": "upstream_error", "message": "Error interno del procesador"})
        if self.chaos.roll(p.slow):                                                    # API-03
            self.sleep(p.slow_seconds)
        status, headers, body = fn()
        if status == 200 and pageable:
            if self.chaos.roll(p.error_in_200):                                        # API-12
                return _json(200, {"status": "error", "message": "backend timeout, reintente"})
            if self.chaos.roll(p.truncated):                                           # API-10
                return status, headers, body[: max(1, len(body) // 2)]
        return status, headers, body

    # ---------------------------------------------------------------------------------- v2 cursor
    def _day_from(self, req: Request, resource: str):
        name = DATE_PARAM[resource]
        cursor = req.params.get("cursor")
        if cursor:
            c = _decode_cursor(cursor)
            if not c or "d" not in c or "p" not in c:
                return None, None, _json(400, {"error": "invalid_cursor"})
            return c["d"], c, None
        day = req.params.get(name)
        if not _valid_day(day):
            return None, None, _json(400, {"error": "invalid_request", "message": f"Falta {name}=YYYY-MM-DD"})
        if day < V2_LAUNCH:
            return None, None, _json(422, {"error": "not_available",
                                           "message": f"La API v2 tiene datos desde {V2_LAUNCH}. Para antes, "
                                                      "usar los exportes de la integración v1."})
        return day, {"d": day, "p": 1}, None

    def _page_v2(self, req: Request, resource: str):
        day, c, err = self._day_from(req, resource)
        if err:
            return err
        manifest = self.store.manifest(resource, day) or {"pages": 0, "records": 0}
        page = int(c["p"])
        rows = self.store.page(resource, day, page) if manifest["pages"] else None
        rows = rows or []
        nxt = None
        if page < manifest["pages"]:
            if not c.get("r") and self.chaos.structural(self.chaos.profile.duplicate_page, resource, day, page, "dup"):
                nxt = {"d": day, "p": page, "r": 1}                                    # API-05: la misma otra vez
            elif self.chaos.structural(self.chaos.profile.skipped_page, resource, day, page, "skip") \
                    and page + 2 <= manifest["pages"]:
                nxt = {"d": day, "p": page + 2}                                       # API-06: se saltea una
            else:
                nxt = {"d": day, "p": page + 1}
        return _json(200, {
            "data": rows,
            "page": page,
            "page_size": PAGE_SIZE,
            "next_cursor": _encode_cursor(nxt) if nxt else None,
            "has_more": nxt is not None,
            "total_estimate": manifest["records"],
        })

    def _totals(self, req: Request, resource: str):
        name = DATE_PARAM[resource]
        day = req.params.get(name)
        if not _valid_day(day):
            return _json(400, {"error": "invalid_request", "message": f"Falta {name}=YYYY-MM-DD"})
        m = self.store.manifest(resource, day) or {"pages": 0, "records": 0, "amount_ars": 0}
        return _json(200, {name: day, "records": m["records"], "amount_ars": m.get("amount_ars", 0), "pages": m["pages"]})

    # ----------------------------------------------------------------------------------- v1 offset
    def _page_v1(self, req: Request):
        day = req.params.get("processing_date")
        if not _valid_day(day):
            return _json(400, {"error": "invalid_request", "message": "Falta processing_date=YYYY-MM-DD"})
        try:
            offset = max(0, int(req.params.get("offset", 0)))
            limit = min(500, max(1, int(req.params.get("limit", 200))))
        except ValueError:
            return _json(400, {"error": "invalid_request", "message": "offset y limit son enteros"})
        manifest = self.store.manifest("authorizations", day) or {"pages": 0, "records": 0}
        # API-07: entre pedido y pedido "entran" filas nuevas y corren el offset (duplica o pierde filas en el borde)
        if offset > 0 and self.chaos.roll(self.chaos.profile.unstable_offset):
            offset = max(0, offset + self.chaos.rng.choice([-3, -2, -1, 1, 2, 3]))
        rows, pos = [], offset
        while len(rows) < limit and pos < manifest["records"]:
            page_rows = self.store.page("authorizations", day, pos // PAGE_SIZE + 1) or []
            take = page_rows[pos % PAGE_SIZE: pos % PAGE_SIZE + (limit - len(rows))]
            if not take:
                break
            rows.extend(take)
            pos += len(take)
        return _json(200, {"total": manifest["records"], "offset": offset, "limit": limit,
                           "items": [_to_v1(r) for r in rows]})

    # ------------------------------------------------------------------------------------------- CRM
    def _crm(self, req: Request, obj: str):
        """Registros modificados desde `modified_since` (>=), ordenados por (SystemModstamp, Id), de a 500.

        Trae también los borrados (`IsDeleted: true`): sin eso, un registro borrado en el CRM no desaparece nunca del
        lakehouse. El cursor guarda el filtro, así que las páginas siguientes no lo repiten.
        """
        if obj not in CRM_OBJECTS:
            return _json(404, {"error": "not_found", "message": f"Objeto desconocido: {obj}. Válidos: {CRM_OBJECTS}"})
        cursor = req.params.get("cursor")
        if cursor:
            c = _decode_cursor(cursor)
            if not c or c.get("o") != obj or "p" not in c:
                return _json(400, {"error": "invalid_cursor"})
        else:
            c = {"o": obj, "s": req.params.get("modified_since") or "", "p": 1}
        since = c["s"][:19]
        rows = [r for r in (self.crm_store.records(obj) if self.crm_store else None) or []
                if r["SystemModstamp"][:19] >= since]
        rows.sort(key=lambda r: (r["SystemModstamp"], r["Id"]))
        page = int(c["p"])
        chunk = rows[(page - 1) * CRM_PAGE_SIZE: page * CRM_PAGE_SIZE]
        more = page * CRM_PAGE_SIZE < len(rows)
        return _json(200, {"data": chunk, "page": page, "page_size": CRM_PAGE_SIZE, "total": len(rows),
                           "next_cursor": _encode_cursor(dict(c, p=page + 1)) if more else None, "has_more": more})

    # ---------------------------------------------------------------------------------- espejo BCRA
    def _fx(self, req: Request, path: str):
        parts = path.strip("/").split("/")
        if parts[:4] != ["fx", "estadisticascambiarias", "v1.0", "Cotizaciones"] or len(parts) != 5:
            return _json(404, {"status": 404, "errorMessages": ["Recurso inexistente"]})
        currency = parts[4].upper()
        desde, hasta = req.params.get("fechadesde"), req.params.get("fechahasta")
        if not (_valid_day(desde) and _valid_day(hasta)) or desde > hasta:
            return _json(400, {"status": 400, "errorMessages": ["Parámetros fechadesde/fechahasta inválidos"]})
        offset, limit = int(req.params.get("offset", 0)), min(1000, int(req.params.get("limit", 1000)))
        series = [r for r in (self.fx_store.series(currency) or []) if desde <= r["fecha"] <= hasta]
        series.sort(key=lambda r: r["fecha"], reverse=True)               # el BCRA devuelve de la más nueva a la más vieja
        window = [_dirty_fx(r, currency) for r in series[offset: offset + limit]]
        return _json(200, {"status": 200, "metadata": {"resultset": {"count": len(series), "offset": offset, "limit": limit}},
                           "results": window})


def _to_v1(r: dict) -> dict:
    """Registro canónico → formato v1 (centavos, epoch ms, estado A/R, código de 2 dígitos)."""
    ts = time.strptime(r["authorized_at"], "%Y-%m-%dT%H:%M:%SZ")
    return {
        "id_autorizacion": r["auth_id"], "token_tarjeta": r["card_token"], "ultimos4": r["pan_last4"],
        "marca": r["brand"], "tipo_tarjeta": r["card_type"], "comercio_id": r["merchant_id"],
        "comercio_nombre": r["merchant_name"], "rubro_mcc": r["mcc"], "pais": r["merchant_country"],
        "ciudad": r.get("merchant_city"), "monto_centavos": round(r["amount"] * 100), "moneda": r["currency"],
        "monto_pesos_centavos": round(r["billing_amount_ars"] * 100), "fecha_hora_ms": calendar.timegm(ts) * 1000,
        "estado": "A" if r["status"] == "APPROVED" else "R", "codigo_respuesta": r["response_code"][:2],
        "modo_ingreso": r["entry_mode"],
    }


def _dirty_fx(r: dict, currency: str) -> dict:
    """Suciedad del espejo: en sep-2024 el valor viene como texto con coma; en feb-2025 el código cambia a 'U$S'."""
    value = r["tipoCotizacion"]
    if r["fecha"].startswith("2024-09"):
        value = f"{value:.4f}".replace(".", ",")
    code = "U$S" if (currency == "USD" and r["fecha"].startswith("2025-02")) else currency
    return {"fecha": r["fecha"], "detalle": [{"codigoMoneda": code, "descripcion": r.get("descripcion", ""),
                                               "tipoPase": r.get("tipoPase", 0.0), "tipoCotizacion": value}]}
