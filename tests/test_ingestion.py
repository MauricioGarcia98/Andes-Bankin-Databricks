"""Ingesta de punta a punta en local: cliente robusto vs. la API con caos, sync al Volume y bronze."""

import json
import random
from datetime import date

import pytest
from fake_http import FakeSession
from pyspark.sql import functions as F

from andes_bank.generator.exports import CORRUPT_FILE, DUPLICATE_FILE, export_all
from andes_bank.generator.upload import api_uploads, cards_legacy_uploads, core_uploads
from andes_bank.ingestion import bronze_api, bronze_files
from andes_bank.ingestion.api_client import ApiClient
from andes_bank.ingestion.landing_sync import RemoteFile, run_sync
from api import CardApi, Settings  # noqa: E402  (agregado al path por fake_http)
from chaos import Chaos  # noqa: E402
from store import MemoryFxStore, MemoryStore  # noqa: E402

SETTINGS = Settings("lakehouse", "s3cr3t", "k")


def _client(api, **kw):
    return ApiClient("http://fake/api", "lakehouse", "s3cr3t", run_id="test", session=FakeSession(api),
                     sleep=lambda s: None, rng=random.Random(3), **kw)


def _api(data, profile, clock=lambda: 0.0):
    # reloj fijo: las fallas estructurales dependen de la ventana de 10 minutos (con el reloj real, el test es azaroso)
    return CardApi(MemoryStore(data), MemoryFxStore({}), SETTINGS,
                   Chaos(profile, seed=5, rng=random.Random(11), clock=clock), sleep=lambda s: None)


def _rows(day, n):
    return [{"auth_id": f"{day}-{i}", "billing_amount_ars": 1.0} for i in range(n)]


@pytest.mark.parametrize("profile", ["off", "normal", "dificil"])
def test_cliente_recupera_dias_completos_con_caos(profile):
    days = [f"2025-07-{d:02d}" for d in range(1, 11)]
    now = [0.0]
    api = _api({("authorizations", d): _rows(d, 2_345) for d in days}, profile, clock=lambda: now[0])
    client = _client(api)
    for d in days:
        r = client.fetch_day("authorizations", d, "auth_id", "processing_date")
        if not r.complete:          # página salteada estable en esta ventana (R14): un rato después se recupera
            now[0] += 600
            r = client.fetch_day("authorizations", d, "auth_id", "processing_date")
        assert r.complete, (profile, d, len(r.records), r.expected)
    statuses = {c["status_code"] for c in client.calls}
    if profile != "off":
        assert statuses - {200}, "con caos tiene que haber fallas registradas"


def test_reintentos_agotados_terminan_en_error():
    api = _api({("authorizations", "2025-07-01"): _rows("x", 10)}, "off")
    api.chaos.profile = api.chaos.profile.__class__(server_error=1.0)
    from andes_bank.ingestion.api_client import ApiError

    with pytest.raises(ApiError):
        _client(api, max_retries=3).get("v2/authorizations", {"processing_date": "2025-07-01"})


@pytest.fixture(scope="module")
def landed(spark, truth):
    """Exporta, "sube" a un Azure falso (los Upload en memoria) y sincroniza al Volume."""
    cfg, _ = truth
    export_all(spark, cfg)
    blobs = {}
    for u in list(core_uploads(cfg)) + list(cards_legacy_uploads(cfg)):
        blobs[(u.container, u.name)] = u.data          # el último gana (ING-03: el truncado pisa al completo)
    files = [RemoteFile(c, n, etag=str(hash(d)), size=len(d), fetch=lambda d=d: d) for (c, n), d in blobs.items()]
    counts = run_sync(spark, cfg, files)
    return cfg, counts, blobs


def test_sync_valida_en_el_borde(spark, landed):
    cfg, counts, _ = landed
    assert counts["CORRUPT"] == 1 and counts["DUPLICATE_CONTENT"] >= 1 and counts["EMPTY"] >= 1
    lf = spark.table(cfg.table("ops", "landing_files"))
    corrupt = lf.where("status = 'CORRUPT'").first()
    assert corrupt.blob_name.endswith(f"MOVIMIENTOS_{CORRUPT_FILE:%Y%m%d}.csv.gz") and "_errors" in corrupt.landed_path
    dup = lf.where("status = 'DUPLICATE_CONTENT'").select("blob_name").collect()
    assert any(r.blob_name.endswith(f"MOVIMIENTOS_{DUPLICATE_FILE:%Y%m%d}_copia.csv.gz") for r in dup)
    # segunda corrida: nada nuevo
    files = [RemoteFile(r.container, r.blob_name, r.etag, 0, lambda: b"") for r in lf.collect()]
    assert run_sync(spark, cfg, files) == {}


def test_bronze_de_archivos(spark, landed):
    cfg, _, _ = landed
    out = bronze_files.ingest_all(spark, cfg)
    assert all(v > 0 for v in out.values()), out
    mov = spark.table(cfg.table("bronze", "core_movimientos"))
    assert "canal_digital" in mov.columns
    legacy = spark.table(cfg.table("bronze", "core_movimientos_legacy"))
    assert legacy.where(F.col("IMPORTE").contains(",")).count() > 0          # formato argentino tal cual
    # movimientos en bronze = exportados - archivo corrupto (que llegó como reenvío) ... nada se pierde ni se inventa
    truth = spark.table(cfg.table("sim", "transactions")).count()
    total = mov.count() + legacy.count()
    assert truth * 0.99 < total < truth * 1.01
    assert bronze_files.ingest_all(spark, cfg) == {k: 0 for k in out}      # incremental: segunda corrida, 0 filas


def test_bronze_de_la_api_con_watermark(spark, truth):
    cfg, _ = truth
    data = {}
    for u in api_uploads(cfg, start=date(2025, 7, 1), end=date(2025, 7, 7)):
        if u.name.endswith("_manifest.json"):
            continue
        resource, y, m, d, _ = u.name.split("/")
        data.setdefault((resource, f"{y}-{m}-{d}"), []).extend(json.loads(u.data))
    client = _client(_api(data, "normal"))
    s = bronze_api.ingest_cards_api(spark, cfg, client, "authorizations", until=date(2025, 7, 7))
    assert s["incomplete_days"] == [] and s["days"] > 0
    assert bronze_api.watermark(spark, cfg, "authorizations") == date(2025, 7, 7)
    b = spark.table(cfg.table("bronze", "cards_api_authorizations"))
    assert b.count() == b.select("record_id").distinct().count() == sum(
        len(v) for (r, d), v in data.items() if r == "authorizations" and d >= "2025-04-01")
    assert spark.table(cfg.table("ops", "api_calls")).count() > 0
    # re-correr no trae nada (watermark) y re-ingestar un día no duplica (replaceWhere)
    assert bronze_api.ingest_cards_api(spark, cfg, client, "authorizations", until=date(2025, 7, 7))["days"] == 0


def test_dia_incompleto_se_reintenta_al_final_de_la_corrida(spark, truth):
    """R14: la página salteada es estable un rato; los reintentos inmediatos no alcanzan, los diferidos sí."""
    cfg, _ = truth
    days = ["2025-07-08", "2025-07-09", "2025-07-10"]
    api = _api({("authorizations", d): _rows(d, 2_500) for d in days}, "off")
    api.chaos.profile = api.chaos.profile.__class__(skipped_page=1.0)   # durante "esta ventana", siempre saltea
    waits = []

    def sleep(s):                                                       # "un rato después" la API ya no saltea
        waits.append(s)
        api.chaos.profile = api.chaos.profile.__class__()

    s = bronze_api.ingest_cards_api(spark, cfg, _client(api), "authorizations", until=date(2025, 7, 10), sleep=sleep)
    assert s["incomplete_days"] == [] and s["retry_rounds"] == 1 and len(waits) == 1
    assert bronze_api.watermark(spark, cfg, "authorizations") == date(2025, 7, 10)
    b = spark.table(cfg.table("bronze", "cards_api_authorizations")).where(F.col("_window_date").isin(days))
    assert b.count() == 7_500
