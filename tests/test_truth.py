"""Invariantes de la "verdad" del banco: si se rompen, las conciliaciones del lakehouse no tienen sentido."""

import pytest
from pyspark.sql import functions as F

from andes_bank.utils.ids import cbu_valid_col, cuit_valid_col

pytestmark = pytest.mark.spark


def t(spark, cfg, name):
    return spark.table(cfg.table("sim", name))


def test_reproducible_y_con_volumen(truth):
    _, counts = truth
    assert counts["customers"] > 1000 and counts["transactions"] > 10 * counts["accounts"]
    assert counts["authorizations"] > counts["settlements"] > 0


def test_ids_unicos_y_validos(spark, truth):
    cfg, _ = truth
    assert t(spark, cfg, "transactions").groupBy("txn_id").count().where("count > 1").count() == 0
    assert t(spark, cfg, "authorizations").groupBy("auth_id").count().where("count > 1").count() == 0
    assert t(spark, cfg, "customers").where(~cuit_valid_col(F.col("cuit"))).count() == 0
    assert t(spark, cfg, "accounts").where(~cbu_valid_col(F.col("cbu"))).count() == 0


def test_cajas_de_ahorro_nunca_negativas(spark, truth):
    cfg, _ = truth
    neg = (t(spark, cfg, "transactions").join(t(spark, cfg, "accounts").select("account_key", "account_type"), "account_key")
           .where("account_type LIKE 'CA_%' AND balance_after < -0.005"))
    assert neg.count() == 0


def test_compras_con_debito_estan_en_el_core(spark, truth):
    cfg, _ = truth
    approved = t(spark, cfg, "authorizations").where("card_type = 'DEBITO' AND status = 'APPROVED'")
    core = t(spark, cfg, "transactions").where("txn_type = 'COMPRA_DEBITO'")
    assert approved.count() == core.count()
    assert approved.join(core, approved.auth_id == core.reference, "left_anti").count() == 0


def test_saldo_de_cierre_coincide_con_el_ultimo_movimiento(spark, truth):
    cfg, _ = truth
    bal = t(spark, cfg, "balances").where("NOT snapshot")
    last = (t(spark, cfg, "transactions").groupBy("account_key", F.col("posting_date").alias("balance_date"))
            .agg(F.max_by("balance_after", F.struct("ts_utc", "txn_id")).alias("expected")))
    diff = bal.join(last, ["account_key", "balance_date"]).where(F.abs(F.col("closing_balance") - F.col("expected")) > 0.005)
    assert diff.count() == 0


def test_duplicados_de_la_fusion(spark, truth):
    cfg, _ = truth
    c = t(spark, cfg, "customers")
    dups = c.where("is_legacy_duplicate")
    assert dups.count() > 0
    # misma persona (DNI) con dos números de cliente: uno NOA- y otro CUY-
    same = dups.alias("d").join(c.where("NOT is_legacy_duplicate").alias("o"), F.col("d.person_id") == F.col("o.customer_key"))
    assert same.where("d.dni = o.dni AND o.customer_id LIKE 'NOA-%' AND d.customer_id LIKE 'CUY-%'").count() == dups.count()


def test_fraude_concentrado_en_tarjetas_comprometidas(spark, truth):
    cfg, _ = truth
    a = t(spark, cfg, "authorizations")
    assert a.where("is_fraud").count() > 0
    assert a.where("is_fraud").join(t(spark, cfg, "cards").where("NOT compromised"), "card_key").count() == 0
