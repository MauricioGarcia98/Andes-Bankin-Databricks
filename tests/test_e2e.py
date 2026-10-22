"""De punta a punta en local: el banco simulado → Azure (falso) → landing → bronze → silver → gold → quality gate.

Cada assert contrasta el lakehouse contra la VERDAD (`sim.*`): si el pipeline resuelve bien los problemas del
catálogo, recupera lo que pasó de verdad (salvo lo que el core nunca mandó, que tiene que quedar detectado).
"""

import json
import random
from dataclasses import replace
from datetime import date

import pytest
from fake_http import FakeSession
from pyspark.sql import functions as F

from andes_bank.config import local_config
from andes_bank.generator import reference as ref
from andes_bank.generator.crm import crm_state, generate_crm
from andes_bank.generator.exports import export_all
from andes_bank.generator.truth import generate_truth
from andes_bank.generator.upload import api_uploads, cards_legacy_uploads, core_uploads
from andes_bank.gold.crm import build_crm_gold
from andes_bank.gold.marts import build_marts
from andes_bank.gold.model import build_gold
from andes_bank.ingestion import bronze_api, bronze_crm, bronze_files
from andes_bank.ingestion.api_client import ApiClient
from andes_bank.ingestion.landing_sync import RemoteFile, run_sync
from andes_bank.quality.gate import gate
from andes_bank.setup import create_namespace
from andes_bank.silver.load import load_all
from api import CardApi, Settings  # noqa: E402
from chaos import Chaos  # noqa: E402
from store import MemoryCrmStore, MemoryFxStore, MemoryStore  # noqa: E402

pytestmark = pytest.mark.spark


def _fx_series():
    """Serie diaria (días hábiles) con la forma del BCRA, a partir del dólar de fin de mes de referencia."""
    rows = []
    for m, _, usd in ref.monthly_index_rows(date(2023, 1, 1), date(2026, 10, 20)):
        y, mo = map(int, m.split("-"))
        for d in range(1, 29):
            day = date(y, mo, d)
            if day.weekday() < 5 and day.isoformat() not in ref.HOLIDAYS:
                rows.append({"fecha": day.isoformat(), "descripcion": "DOLAR E.E.U.U.", "tipoPase": 0.0, "tipoCotizacion": usd})
    return rows


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    cfg = replace(local_config(str(tmp_path_factory.mktemp("e2e"))), schema_prefix="e2e_")
    create_namespace(spark, cfg)
    generate_truth(spark, cfg)
    export_all(spark, cfg)
    # "Azure": los blobs en memoria
    blobs = {}
    for u in list(core_uploads(cfg)) + list(cards_legacy_uploads(cfg)):
        blobs[(u.container, u.name)] = u.data
    run_sync(spark, cfg, [RemoteFile(c, n, str(hash(d)), len(d), lambda d=d: d) for (c, n), d in blobs.items()])
    bronze_files.ingest_all(spark, cfg)
    # la API con caos "normal"
    pages = {}
    for u in api_uploads(cfg):
        if not u.name.endswith("_manifest.json"):
            resource, y, m, d, _ = u.name.split("/")
            pages.setdefault((resource, f"{y}-{m}-{d}"), []).extend(json.loads(u.data))
    fx = _fx_series()
    now = [0.0]  # reloj simulado: la espera entre rondas de reintento lo adelanta (R14)
    crm = MemoryCrmStore({})
    api = CardApi(MemoryStore(pages), MemoryFxStore({"USD": fx}), Settings("c", "s", "k"),
                  Chaos("normal", seed=9, rng=random.Random(2), clock=lambda: now[0]), sleep=lambda s: None, crm_store=crm)
    client = ApiClient("http://fake/api", "c", "s", run_id="e2e", session=FakeSession(api), sleep=lambda s: None,
                       rng=random.Random(4))
    for resource in ("authorizations", "settlements"):
        assert bronze_api.ingest_cards_api(spark, cfg, client, resource, batch_days=200,
                                           sleep=lambda s: now.__setitem__(0, now[0] + s))["incomplete_days"] == []
    bronze_api.ingest_fx(spark, cfg, bronze_api.mirror_page_getter(client), ("USD",), "mirror", "e2e")
    # CRM: el generado se publica en la API (con el mismo caos) y se ingesta incremental
    generate_crm(spark, cfg)
    crm.data = crm_state(spark, cfg)
    fetch = bronze_crm.crm_api_fetcher(client)
    bronze_crm.ingest_crm(spark, cfg, fetch, "e2e")
    lead = next(r for r in crm.data["Lead"] if r["Andes_Key__c"] == "LEA-00001")
    lead.update(IsDeleted=True, SystemModstamp="2026-10-21T10:00:00.000+0000")   # un borrado posterior
    second = {r["object"]: r for r in bronze_crm.ingest_crm(spark, cfg, fetch, "e2e-2")}
    assert second["Lead"]["deleted"] == 1 and second["Account"]["rows"] < 10         # incremental: sólo lo nuevo
    bronze_api.flush_calls(spark, cfg, client)
    silver = load_all(spark, cfg)
    build_gold(spark, cfg)
    build_marts(spark, cfg)
    build_crm_gold(spark, cfg)
    return cfg, silver


def t(spark, cfg, layer, name):
    return spark.table(cfg.table(layer, name))


def test_movimientos_recuperan_la_verdad(spark, lake):
    cfg, _ = lake
    truth = t(spark, cfg, "sim", "transactions")
    silver = t(spark, cfg, "silver", "transactions")
    assert silver.count() == silver.select("movimiento_id").distinct().count()                 # DAT-01
    j = silver.join(truth, silver.movimiento_id == truth.txn_id)
    assert j.count() == silver.count()                                                        # nada inventado
    missing = truth.count() - silver.count()
    assert 0 < missing < truth.count() * 0.001                                                # DAT-10: sólo lo no enviado
    assert j.where(F.abs(F.col("importe") - F.col("signed_amount")) > 0.005).count() == 0     # DAT-02 y signo
    assert j.where(F.col("fecha_hora_utc") != F.col("ts_utc")).count() == 0                   # DAT-06
    assert j.where(F.col("moneda") != F.col("currency")).count() == 0                         # DAT-05
    assert silver.where("moneda_corregida").count() > 0


def test_movimientos_faltantes_detectados(spark, lake):
    cfg, _ = lake
    breaks = t(spark, cfg, "silver", "balance_breaks").count()
    missing = t(spark, cfg, "sim", "transactions").count() - t(spark, cfg, "silver", "transactions").count()
    assert 0 < breaks <= missing


def test_identidad_y_scd2(spark, lake):
    cfg, _ = lake
    xref = t(spark, cfg, "silver", "customer_xref")
    dups_truth = t(spark, cfg, "sim", "customers").where("is_legacy_duplicate").count()
    assert xref.where("clientes_de_la_persona > 1 AND cliente_id <> persona_id").count() == dups_truth   # DAT-08
    h = t(spark, cfg, "silver", "customers_history")
    assert h.where("is_current").count() == h.select("cliente_id").distinct().count()
    # la recarga completa de la migración no genera versiones: sólo los cambios reales
    changed = t(spark, cfg, "sim", "customers").where("segment_change_date IS NOT NULL OR email_change_date IS NOT NULL "
                                                      "OR closed_date IS NOT NULL").count()
    extra_versions = h.where("version > 1").count()
    assert 0 < extra_versions <= changed * 3


def test_tarjetas_v1_y_v2(spark, lake):
    cfg, _ = lake
    a = t(spark, cfg, "silver", "card_authorizations")
    # las que el procesador informa tarde y caen después del último día procesado todavía no llegaron (correcto)
    truth = t(spark, cfg, "sim", "authorizations").where(F.col("processing_date") <= F.lit(cfg.end_date.isoformat()))
    assert a.count() == a.select("auth_id").distinct().count() == truth.count()               # API-05/09, ni una de más
    overlap = a.where(f"fecha_proceso BETWEEN '{ref.API_V2_LAUNCH}' AND '{ref.API_V1_LAST_EXPORT}'")
    assert overlap.where("version_api = 'v1'").count() == 0                                   # gana v2
    j = a.join(truth, "auth_id")
    assert j.where(F.abs(F.col("importe_ars") - F.col("billing_amount_ars")) > 0.005).count() == 0
    assert a.where("billetera IS NOT NULL").count() > 0                                       # API-11 (drift)


def test_gold_y_gate(spark, lake):
    cfg, _ = lake
    assert t(spark, cfg, "gold", "fact_movimiento").where("cliente_id IS NULL").count() == 0
    fecha = t(spark, cfg, "gold", "dim_fecha")
    assert fecha.where("usd_oficial IS NULL AND fecha >= '2023-01-02'").count() == 0          # DAT-13
    c360 = t(spark, cfg, "gold", "mart_cliente_360")
    assert c360.count() == t(spark, cfg, "gold", "dim_cliente").where("is_current").count()
    results = gate(spark, cfg, "e2e")
    assert all(r["passed"] for r in results)
    warn = {r["rule"]: r["failed_rows"] for r in results if r["severity"] == "warn"}
    assert warn["archivos_corruptos"] == 1 and warn["cuit_valido"] > 0 and warn["personas_duplicadas"] > 0


def test_crm_match_con_el_core_contra_la_verdad(spark, lake):
    """DAT-09/16/17: cada cuenta del CRM que es cliente se une a SU persona del core, aunque el ID esté mal."""
    cfg, silver = lake
    assert silver["crm"]["crm_accounts"] > 0
    truth = spark.table(cfg.table("sim", "crm_accounts")).select(
        F.col("Andes_Key__c").alias("clave_carga"), "_true_customer_id", "_dirt")
    xref = spark.table(cfg.table("silver", "customer_xref")).select(
        F.col("cliente_id").alias("_true_customer_id"), F.col("persona_id").alias("_true_persona"))
    acc = spark.table(cfg.table("silver", "crm_accounts")).join(truth, "clave_carga").join(xref, "_true_customer_id", "left") \
        .withColumn("_true_persona", F.coalesce("_true_persona", "_true_customer_id"))
    customers = acc.where("_true_customer_id IS NOT NULL")
    # los que tienen CUIT o un ID bueno se unen bien; ninguno se une a OTRA persona
    assert customers.where("persona_id IS NOT NULL AND persona_id != _true_persona").count() == 0
    matched = customers.where("persona_id IS NOT NULL").count()
    assert matched / customers.count() > 0.95
    assert acc.where("_dirt = 'PROSPECTO'").where("persona_id IS NOT NULL").count() == 0
    # cada duplicada quedó marcada (y apunta a una principal que no es ella misma)
    dups = acc.where("_dirt = 'DUPLICADA'")
    assert dups.count() > 0 and dups.where("es_duplicada AND cuenta_principal_id != cuenta_crm_id").count() == dups.count()


def test_crm_gold_y_borrados(spark, lake):
    cfg, _ = lake
    leads = spark.table(cfg.table("silver", "crm_leads"))
    assert leads.count() == spark.table(cfg.table("sim", "crm_leads")).count() - 1      # el borrado no está
    assert leads.where("ya_es_cliente").count() > 0                                      # DAT-18 detectado
    casos = spark.table(cfg.table("gold", "fact_caso"))
    assert casos.where(F.col("descripcion").rlike("@|\\+54")).count() == 0                # DAT-11: sin PII
    opp = spark.table(cfg.table("gold", "fact_oportunidad"))
    assert opp.count() == spark.table(cfg.table("sim", "crm_opportunities")).count()
    assert spark.table(cfg.table("gold", "mart_pipeline_comercial")).count() > 0
