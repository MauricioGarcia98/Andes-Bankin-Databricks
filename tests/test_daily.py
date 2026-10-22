"""El día nuevo: el banco sigue vivo después de la historia, con las mismas reglas y sin romper los saldos."""

from dataclasses import replace
from datetime import date

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from andes_bank.config import local_config
from andes_bank.generator import daily, exports
from andes_bank.generator.truth import generate_truth
from andes_bank.generator.upload import api_uploads, core_uploads
from andes_bank.setup import create_namespace

pytestmark = pytest.mark.spark

BASE_END = date(2026, 9, 29)     # la historia termina el 29-sep: el día nuevo cruza un fin de mes y un día 10
UNTIL = date(2026, 10, 10)


@pytest.fixture(scope="module")
def bank(spark, tmp_path_factory):
    cfg = replace(local_config(str(tmp_path_factory.mktemp("dia"))), schema_prefix="dia_",
                  start_date=date(2026, 7, 1), end_date=BASE_END)
    create_namespace(spark, cfg)
    generate_truth(spark, cfg)
    exports.export_all(spark, cfg)
    # R20: la historia no entrega nada después de su último día (si no, el día nuevo lo encontraría ya subido a medias)
    late = [u.name for u in list(api_uploads(cfg)) + list(core_uploads(cfg)) if "/2026/09/3" in u.name or "/2026/10/" in u.name]
    assert late == [], late[:5]
    before = {t: spark.table(cfg.table("sim", t)).count() for t in ("authorizations", "transactions")}
    assert daily.days_to_generate(spark, cfg, UNTIL)[0] == date(2026, 9, 30)
    out = daily.run_new_days(spark, cfg, until=UNTIL)                 # sin SAS: genera y exporta, no sube
    return cfg, before, out


def t(spark, cfg, name):
    return spark.table(cfg.table("sim", name))


def test_genera_cada_dia_que_falta(spark, bank):
    cfg, before, out = bank
    assert [o["day"] for o in out][0] == "2026-09-30" and out[-1]["day"] == "2026-10-10" and len(out) == 11
    assert daily.last_generated_day(spark, cfg) == UNTIL and daily.days_to_generate(spark, cfg, UNTIL) == []
    # cada día trae compras y movimientos en una cantidad parecida a la de la historia
    hist_per_day = before["authorizations"] / (BASE_END - cfg.start_date).days
    assert all(0.5 * hist_per_day < o["authorizations"] < 1.6 * hist_per_day for o in out), (hist_per_day, out)
    assert all(o["transactions"] > 0 and o["balances"] > 0 for o in out)
    for name, key in (("authorizations", "auth_id"), ("transactions", "txn_id"), ("settlements", "settlement_id")):
        df = t(spark, cfg, name)
        assert df.count() == df.select(key).distinct().count(), name


def test_los_saldos_siguen_la_historia(spark, bank):
    cfg, _, _ = bank
    tx = t(spark, cfg, "transactions").join(t(spark, cfg, "accounts").select("account_key", "account_type"), "account_key")
    w = Window.partitionBy("account_key").orderBy("ts_utc", "txn_id")
    jump = tx.withColumn("prev", F.lag("balance_after").over(w)).where("prev IS NOT NULL") \
        .where(F.abs(F.col("prev") + F.col("signed_amount") - F.col("balance_after")) > 0.011)
    assert jump.count() == 0                                          # el primer día nuevo arranca del último saldo
    assert tx.where("account_type LIKE 'CA_%' AND balance_after < -0.01").count() == 0   # la caja nunca negativa
    oct10 = tx.where("txn_type = 'PAGO_TARJETA_CREDITO' AND to_date(ts_utc) = '2026-10-10'")
    assert oct10.count() > 0                                          # el resumen de septiembre se paga el 10
    # foto de fin de mes del 30-sep: todas las cuentas abiertas tienen saldo
    snap = t(spark, cfg, "balances").where("balance_date = '2026-09-30'")
    alive = t(spark, cfg, "accounts").where("opened_date <= '2026-09-30' AND (closed_date IS NULL OR closed_date > '2026-09-30')")
    assert snap.count() == alive.count()


def test_liquida_las_compras_de_la_historia(spark, bank):
    cfg, _, _ = bank
    s = t(spark, cfg, "settlements").where("settlement_date = '2026-09-30'").join(
        t(spark, cfg, "authorizations").select("auth_id", F.to_date("auth_ts").alias("auth_date")), "auth_id")
    assert s.where(F.col("auth_date") < F.lit(BASE_END)).count() > 0     # compras del 27 al 29-sep que se liquidan hoy


def test_regenerar_un_dia_es_idempotente(spark, bank):
    cfg, _, out = bank
    day = date(2026, 10, 5)
    totals = {n: t(spark, cfg, n).count() for n in ("authorizations", "settlements", "transactions", "balances")}
    again = daily.generate_day(spark, cfg, day)
    assert again == {k: v for k, v in next(o for o in out if o["day"] == "2026-10-05").items() if k in again}
    assert {n: t(spark, cfg, n).count() for n in totals} == totals


def test_exporta_y_sube_solo_el_dia(spark, bank):
    cfg, _, _ = bank
    day = date(2026, 10, 10)
    names = [u.name for u in core_uploads(cfg, start=day, end=day)]
    assert "movimientos/2026/10/10/MOVIMIENTOS_20261010.csv.gz" in names
    assert "saldos/2026/10/10/SALDOS_20261010.csv.gz" in names
    assert not any("clientes" in n for n in names)                     # sin novedades de clientes: no hay archivo
    pages = [u for u in api_uploads(cfg, start=day, end=day) if u.name.startswith("authorizations/")]
    manifest = next(u for u in pages if u.name.endswith("_manifest.json"))
    expected = t(spark, cfg, "authorizations").where(F.col("processing_date") == F.lit(day)).count()
    assert f'"records": {expected}' in manifest.data.decode() and expected > 0
    # los días anteriores siguen en el staging (el overwrite del día es dinámico)
    assert any(u.name.startswith("authorizations/2026/10/09/") for u in api_uploads(cfg, start=date(2026, 10, 9),
                                                                                    end=date(2026, 10, 9)))
