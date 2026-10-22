"""El CRM comercial sin Spark: el estado que se publica, el endpoint `/crm/v1` y el cliente que lo recorre."""

import json
import random
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from fake_http import FakeSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "azure" / "card_api"))

from andes_bank.generator.crm import crm_records, crm_system  # noqa: E402
from andes_bank.ingestion.api_client import ApiClient  # noqa: E402
from andes_bank.ingestion.bronze_crm import crm_api_fetcher  # noqa: E402
from api import CRM_PAGE_SIZE, CardApi, Request, Settings  # noqa: E402
from chaos import Chaos  # noqa: E402
from store import MemoryCrmStore, MemoryFxStore, MemoryStore  # noqa: E402

END = date(2026, 10, 20)


def _customers(n=120):
    out = []
    for i in range(1, n + 1):
        juridica = i % 3 == 0
        out.append({"customer_key": i, "customer_id": f"CLI{i:07d}", "person_type": "JURIDICA" if juridica else "HUMANA",
                    "segment": "PYME" if juridica else "PREMIUM", "first_name": "Ana", "last_name": f"Pérez{i}",
                    "full_name": f"Empresa {i} S.A." if juridica else f"Ana Pérez{i}", "cuit": f"30{i:08d}1",
                    "province": "Salta", "city": "Salta", "phone": "+54 387 4000000", "email": f"ana{i}@mail.com"})
    return out


def _state():
    return crm_system(crm_records(_customers(), seed=1998), seed=1998, end=END)


def test_crm_system_ids_relaciones_y_fechas():
    state = _state()
    assert state == _state()                                                    # misma semilla, mismo CRM
    accounts = {a["Id"]: a for a in state["Account"]}
    assert all(i.startswith("001") for i in accounts)
    for obj in ("Contact", "Opportunity", "Case"):
        for r in state[obj]:
            assert r["AccountId"] in accounts                                   # la clave de carga se resolvió
            assert r["CreatedDate"] >= accounts[r["AccountId"]]["CreatedDate"]  # el hijo nace después que la cuenta
    for obj, rows in state.items():
        for r in rows:
            assert "2024-06-03" <= r["CreatedDate"][:10] <= r["SystemModstamp"][:10] <= END.isoformat()
            assert not any(k.startswith("_") or "." in k for k in r), obj     # sin verdad ni sintaxis de carga
    cases = state["Case"]
    assert all((c["ClosedDate"] is not None) == c["IsClosed"] for c in cases)
    assert all(c["ClosedDate"] >= c["CreatedDate"] for c in cases if c["IsClosed"])
    won = [o for o in state["Opportunity"] if o["IsWon"]]
    assert won and all(o["IsClosed"] and o["StageName"] == "Closed Won" for o in won)


def _api(state, profile="off"):
    chaos = Chaos(profile, seed=3, rng=random.Random(5), clock=lambda: 0)
    return CardApi(MemoryStore({}), MemoryFxStore({}), Settings("c", "s", "k"), chaos, sleep=lambda s: None,
                   crm_store=MemoryCrmStore(state))


def _token(api):
    _, _, body = api.handle(Request("POST", "oauth/token", form={"client_id": "c", "client_secret": "s"}))
    return {"authorization": "Bearer " + json.loads(body)["access_token"]}


def test_endpoint_pagina_filtra_e_incluye_borrados():
    state = _state()
    state["Lead"][0]["IsDeleted"] = True
    api = _api(state)
    h = _token(api)
    assert api.handle(Request("GET", "crm/v1/Lead"))[0] == 401                  # sin token, nada
    assert api.handle(Request("GET", "crm/v1/Usuario", headers=h))[0] == 404
    ids, params = [], {}
    while True:
        status, _, body = api.handle(Request("GET", "crm/v1/Lead", params=params, headers=h))
        page = json.loads(body)
        assert status == 200 and len(page["data"]) <= CRM_PAGE_SIZE
        ids += [r["Id"] for r in page["data"]]
        if not page["has_more"]:
            break
        params = {"cursor": page["next_cursor"]}
    assert sorted(ids) == sorted(r["Id"] for r in state["Lead"])                # el borrado también viaja
    cut = sorted(r["SystemModstamp"] for r in state["Lead"])[200]
    _, _, body = api.handle(Request("GET", "crm/v1/Lead", params={"modified_since": cut[:19]}, headers=h))
    page = json.loads(body)
    assert page["total"] == sum(r["SystemModstamp"] >= cut for r in state["Lead"])
    assert all(r["SystemModstamp"] >= cut for r in page["data"])


def test_fetcher_sobrevive_al_caos():
    state = _state()
    api = _api(state, "dificil")
    client = ApiClient("http://fake/api", "c", "s", session=FakeSession(api), sleep=lambda s: None, rng=random.Random(1))
    fetch = crm_api_fetcher(client)
    for obj, rows in state.items():
        assert sorted(r["Id"] for r in fetch(obj, None)) == sorted(r["Id"] for r in rows)
    since = datetime(2099, 1, 1, tzinfo=timezone.utc)
    assert fetch("Account", since) == []
