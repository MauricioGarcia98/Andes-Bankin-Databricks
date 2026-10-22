"""Azure Function (modelo Python v2): un solo HTTP trigger que le pasa todo a `api.CardApi` (tarjetas, espejo FX y CRM).

Configuración (app settings de la Function App, nunca en el código):
  DATA_STORAGE_CONNECTION   conexión al Storage con las páginas (api-data), el espejo FX (fx-mirror) y el CRM (crm-data)
  API_CLIENT_ID / API_CLIENT_SECRET   credenciales que le damos al "cliente" (el lakehouse)
  TOKEN_SIGNING_KEY         clave para firmar los tokens
  CHAOS_PROFILE             off | normal | dificil        CHAOS_SEED   semilla de las fallas estructurales
"""

import json
import os
from urllib.parse import parse_qsl

import azure.functions as func

from api import CardApi, Request, Settings
from chaos import Chaos
from store import BlobCrmStore, BlobFxStore, BlobStore

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

_conn = os.environ["DATA_STORAGE_CONNECTION"]
_api = CardApi(
    store=BlobStore(_conn),
    fx_store=BlobFxStore(_conn),
    settings=Settings(os.environ["API_CLIENT_ID"], os.environ["API_CLIENT_SECRET"], os.environ["TOKEN_SIGNING_KEY"]),
    chaos=Chaos(os.environ.get("CHAOS_PROFILE", "normal"), int(os.environ.get("CHAOS_SEED", "1998"))),
    crm_store=BlobCrmStore(_conn),
)


def _form(req: func.HttpRequest) -> dict:
    body = req.get_body() or b""
    ctype = (req.headers.get("content-type") or "").lower()
    if "json" in ctype:
        try:
            return json.loads(body or b"{}")
        except ValueError:
            return {}
    return dict(parse_qsl(body.decode(errors="replace")))


@app.route(route="{*path}", methods=["GET", "POST"])
def router(req: func.HttpRequest) -> func.HttpResponse:
    status, headers, body = _api.handle(Request(
        method=req.method,
        path=req.route_params.get("path", ""),
        params=dict(req.params),
        headers={k.lower(): v for k, v in req.headers.items()},
        form=_form(req) if req.method == "POST" else {},
    ))
    return func.HttpResponse(body, status_code=status, headers=headers)
