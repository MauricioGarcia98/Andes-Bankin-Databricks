"""La API del procesador sin Azure: tokens, paginación, cada falla del catálogo y el espejo del BCRA."""

import json
import random
import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "azure" / "card_api"))

import tokens  # noqa: E402
from api import CardApi, Request, Settings  # noqa: E402
from chaos import PROFILES, Chaos  # noqa: E402
from store import MemoryFxStore, MemoryStore  # noqa: E402

DAY = "2025-07-15"
SETTINGS = Settings("lakehouse", "s3cr3t", "clave-de-firma")


def _rows(n):
    return [{"auth_id": f"A{i:06d}", "card_token": "tok", "pan_last4": "1234", "brand": "VISA", "card_type": "DEBITO",
             "merchant_id": "M1", "merchant_name": "X", "mcc": "5411", "merchant_country": "AR", "merchant_city": "Salta",
             "amount": 100.5, "currency": "ARS", "billing_amount_ars": 100.5, "authorized_at": "2025-07-15T13:00:00Z",
             "processing_date": DAY, "status": "APPROVED", "response_code": "00_APROBADA", "entry_mode": "CHIP"}
            for i in range(n)]


FX = {"USD": [{"fecha": "2024-09-10", "descripcion": "DOLAR E.E.U.U.", "tipoPase": 0.0, "tipoCotizacion": 960.5},
              {"fecha": "2025-02-10", "descripcion": "DOLAR E.E.U.U.", "tipoPase": 0.0, "tipoCotizacion": 1060.0},
              {"fecha": "2025-07-01", "descripcion": "DOLAR E.E.U.U.", "tipoPase": 0.0, "tipoCotizacion": 1205.0}]}


def make_api(profile="off", **overrides):
    chaos = Chaos(profile, seed=1, rng=random.Random(7), clock=lambda: 0)
    if overrides:
        chaos.profile = replace(PROFILES[profile], **overrides)
    store = MemoryStore({("authorizations", DAY): _rows(2_500)})
    return CardApi(store, MemoryFxStore(FX), SETTINGS, chaos, sleep=lambda s: None)


def token(api):
    status, _, body = api.handle(Request("POST", "oauth/token", form={"client_id": "lakehouse", "client_secret": "s3cr3t"}))
    assert status == 200
    return {"authorization": "Bearer " + json.loads(body)["access_token"]}


def get(api, path, headers, **params):
    status, h, body = api.handle(Request("GET", path, params=params, headers=headers))
    return status, h, body


def walk(api, headers):
    """Recorre un día entero siguiendo next_cursor; devuelve los ids recibidos."""
    ids, params = [], {"processing_date": DAY}
    while True:
        status, _, body = get(api, "v2/authorizations", headers, **params)
        assert status == 200
        page = json.loads(body)
        ids += [r["auth_id"] for r in page["data"]]
        if not page["has_more"]:
            return ids, page["total_estimate"]
        params = {"cursor": page["next_cursor"]}


def test_tokens():
    t = tokens.issue("c", "k", now=1000)["access_token"]
    assert tokens.verify(t, "k", now=1500) == (True, "ok")
    assert tokens.verify(t, "k", now=1000 + 901) == (False, "token_expired")
    assert tokens.verify(t, "otra", now=1500) == (False, "bad_signature")


def test_sin_token_o_credenciales_malas():
    api = make_api()
    assert get(api, "v2/authorizations", {}, processing_date=DAY)[0] == 401
    status, _, _ = api.handle(Request("POST", "oauth/token", form={"client_id": "lakehouse", "client_secret": "mal"}))
    assert status == 401


def test_paginacion_completa_sin_caos():
    api = make_api()
    ids, total = walk(api, token(api))
    assert len(ids) == total == 2_500 and len(set(ids)) == 2_500


def test_antes_del_lanzamiento_de_v2():
    api = make_api()
    assert get(api, "v2/authorizations", token(api), processing_date="2024-01-01")[0] == 422


def test_pagina_repetida_y_salteada():
    api = make_api(duplicate_page=1.0)
    ids, total = walk(api, token(api))
    assert len(ids) > total and len(set(ids)) == total            # duplicados, sin pérdida
    api = make_api(skipped_page=1.0)
    ids, total = walk(api, token(api))
    assert len(set(ids)) < total                                  # se perdió una página: hay que conciliar


def test_fallas_transitorias():
    api = make_api(rate_limit=1.0)
    status, headers, _ = get(api, "v2/authorizations", token(api), processing_date=DAY)
    assert status == 429 and 2 <= int(headers["Retry-After"]) <= 8
    api = make_api(server_error=1.0)
    assert get(api, "v2/authorizations", token(api), processing_date=DAY)[0] in (500, 502, 503)
    api = make_api(early_expiry=1.0)
    assert get(api, "v2/authorizations", token(api), processing_date=DAY)[0] == 401


def test_200_con_error_y_cuerpo_truncado():
    api = make_api(error_in_200=1.0)
    status, _, body = get(api, "v2/authorizations", token(api), processing_date=DAY)
    assert status == 200 and json.loads(body)["status"] == "error"
    api = make_api(truncated=1.0)
    status, _, body = get(api, "v2/authorizations", token(api), processing_date=DAY)
    with pytest.raises(json.JSONDecodeError):
        json.loads(body)


def test_v1_en_centavos_y_offset_inestable():
    api = make_api()
    _, _, body = get(api, "v1/authorizations", token(api), processing_date=DAY, offset="999", limit="3")
    items = json.loads(body)["items"]
    assert [i["id_autorizacion"] for i in items] == ["A000999", "A001000", "A001001"]   # cruza de página
    assert items[0]["monto_centavos"] == 10050 and items[0]["estado"] == "A"
    api = make_api(unstable_offset=1.0)
    _, _, body = get(api, "v1/authorizations", token(api), processing_date=DAY, offset="500", limit="10")
    assert json.loads(body)["offset"] != 500


def test_espejo_bcra_con_suciedad():
    api = make_api()
    _, _, body = get(api, "fx/estadisticascambiarias/v1.0/Cotizaciones/USD", token(api),
                     fechadesde="2024-01-01", fechahasta="2025-12-31")
    res = json.loads(body)
    assert res["metadata"]["resultset"]["count"] == 3
    by_date = {r["fecha"]: r["detalle"][0] for r in res["results"]}
    assert [r["fecha"] for r in res["results"]] == sorted(by_date, reverse=True)
    assert by_date["2024-09-10"]["tipoCotizacion"] == "960,5000"
    assert by_date["2025-02-10"]["codigoMoneda"] == "U$S"
    assert by_date["2025-07-01"]["tipoCotizacion"] == 1205.0
