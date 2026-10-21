"""Carga de SILVER: qué se escribe, dónde y cómo (los "qué" están en core.py y cards.py).

    silver.customers_history   silver.customer_xref   silver.accounts_history   silver.cards_history   (reconstrucción)
    silver.transactions        silver.balances        silver.card_authorizations silver.card_settlements (incremental)
    silver.fx_rates            silver.balance_breaks                                                      (reconstrucción)
    silver.quarantine          (filas que no se pueden usar, con el motivo; nunca se borran en silencio)
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.silver import cards, core
from andes_bank.utils.delta import get_watermark, merge_insert_only, merge_upsert_when, set_watermark


def _bronze(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame | None:
    t = cfg.table("bronze", name)
    return spark.table(t) if spark.catalog.tableExists(t) else None


def _new_rows(df: DataFrame | None, wm) -> DataFrame | None:
    if df is None:
        return None
    return df if wm is None else df.where(F.col("_ingested_at") > F.lit(wm))


def _max_ingested(*dfs: DataFrame | None):
    vals = [d.agg(F.max("_ingested_at")).first()[0] for d in dfs if d is not None]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


def _overwrite(df: DataFrame, cfg: ProjectConfig, name: str) -> int:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("silver", name))
    return df.sparkSession.table(cfg.table("silver", name)).count()


def _quarantine(spark: SparkSession, cfg: ProjectConfig, entity: str, df: DataFrame, key: str, reason: F.Column) -> int:
    """Manda a cuarentena las filas que no se pueden usar, con el motivo y la fila entera como JSON."""
    q = (df.withColumn("motivo", reason).where(F.col("motivo").isNotNull())
         .select(F.lit(entity).alias("entidad"), F.col(key).cast("string").alias("clave"), "motivo",
                 F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("fila"),
                 F.current_timestamp().alias("_quarantined_at")))
    n = q.count()
    if n:
        q.write.mode("append").saveAsTable(cfg.table("silver", "quarantine"))
    return n


# ------------------------------------------------------------------------------------------------ maestros
def load_masters(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    out = {}
    out["customers_history"] = _overwrite(core.customers_history(spark, cfg), cfg, "customers_history")
    out["customer_xref"] = _overwrite(core.customer_xref(spark.table(cfg.table("silver", "customers_history"))), cfg, "customer_xref")
    out["accounts_history"] = _overwrite(core.accounts_history(spark, cfg), cfg, "accounts_history")
    out["cards_history"] = _overwrite(core.cards_history(spark, cfg), cfg, "cards_history")
    out["fx_rates"] = _overwrite(cards.fx_rates(_bronze(spark, cfg, "fx_bcra"), _bronze(spark, cfg, "fx_mirror")), cfg, "fx_rates")
    return out


# ------------------------------------------------------------------------------------------------- hechos
def load_transactions(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    wm = get_watermark(spark, cfg, "transactions")
    legacy = _new_rows(_bronze(spark, cfg, "core_movimientos_legacy"), wm)
    new = _new_rows(_bronze(spark, cfg, "core_movimientos"), wm)
    if legacy is None and new is None:
        return {"nuevas": 0}
    top = _max_ingested(legacy, new)
    if top is None:
        return {"nuevas": 0}
    raw = core.transactions_from_bronze(legacy, new)
    accounts = spark.table(cfg.table("silver", "accounts_history")).where("is_current")
    df = core.fix_currency(core.dedupe_first_arrival(raw, "movimiento_id"), accounts)
    reason = (F.when(F.col("movimiento_id").isNull(), "sin_id")
              .when(F.col("fecha_hora_utc").isNull(), "fecha_invalida")
              .when(F.col("importe").isNull(), "importe_invalido")
              .when(~F.col("cuenta_conocida"), "cuenta_desconocida"))
    quarantined = _quarantine(spark, cfg, "transactions", df, "movimiento_id", reason)
    good = df.where(reason.isNull()).select(*core.TXN_COLUMNS)
    target = cfg.table("silver", "transactions")
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
    merge_insert_only(spark, good, target, ["movimiento_id"], cluster_by=["cuenta_id", "fecha_contable"])
    after = spark.table(target).count()
    set_watermark(spark, cfg, "transactions", top)
    return {"leidas": raw.count(), "nuevas": after - before, "cuarentena": quarantined}


def load_balances(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    wm = get_watermark(spark, cfg, "balances")
    legacy = _new_rows(_bronze(spark, cfg, "core_saldos_legacy"), wm)
    new = _new_rows(_bronze(spark, cfg, "core_saldos"), wm)
    top = _max_ingested(legacy, new)
    if top is None:
        return {"nuevas": 0}
    df = core.dedupe_first_arrival(core.balances_from_bronze(legacy, new), "balance_key")
    target = cfg.table("silver", "balances")
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
    merge_insert_only(spark, df.drop("_source_file"), target, ["balance_key"], cluster_by=["cuenta_id", "fecha_saldo"])
    set_watermark(spark, cfg, "balances", top)
    return {"nuevas": spark.table(target).count() - before}


def load_card_events(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    out = {}
    for entity, v1_name, v2_name, from_v1, from_v2, key, cols in (
        ("card_authorizations", "cards_v1_autorizaciones", "cards_api_authorizations", cards.auth_from_v1, cards.auth_from_v2,
         "auth_id", cards.AUTH_COLUMNS),
        ("card_settlements", "cards_v1_liquidaciones", "cards_api_settlements", cards.settlements_from_v1,
         cards.settlements_from_v2, "liquidacion_id", cards.SETTLEMENT_COLUMNS),
    ):
        wm = get_watermark(spark, cfg, entity)
        v1 = _new_rows(_bronze(spark, cfg, v1_name), wm)
        v2 = _new_rows(_bronze(spark, cfg, v2_name), wm)
        top = _max_ingested(v1, v2)
        if top is None:
            out[entity] = 0
            continue
        parts = [p for p in (from_v1(v1) if v1 is not None else None, from_v2(v2) if v2 is not None else None) if p is not None]
        df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
        df = cards.prefer_v2(df, key).select(*cols)
        target = cfg.table("silver", entity)
        before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
        # la v2 pisa a la v1 (mismo id por los dos caminos entre abr y jun de 2025); nunca al revés
        merge_upsert_when(spark, df, target, [key], update_when="s.version_api = 'v2' AND t.version_api = 'v1'",
                          cluster_by=[key])
        set_watermark(spark, cfg, entity, top)
        out[entity] = spark.table(target).count() - before
    return out


def load_balance_breaks(spark: SparkSession, cfg: ProjectConfig) -> int:
    return _overwrite(core.balance_breaks(spark.table(cfg.table("silver", "transactions"))), cfg, "balance_breaks")


QUARANTINE_DDL = "entidad STRING, clave STRING, motivo STRING, fila STRING, _quarantined_at TIMESTAMP"


def ensure_quarantine(spark: SparkSession, cfg: ProjectConfig) -> None:
    """La tabla existe aunque no haya nada en cuarentena (Genie y las reglas la consultan igual)."""
    table = cfg.table("silver", "quarantine")
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], QUARANTINE_DDL).write.saveAsTable(table)


def load_all(spark: SparkSession, cfg: ProjectConfig) -> dict:
    started = datetime.now(timezone.utc)
    ensure_quarantine(spark, cfg)
    out = {"masters": load_masters(spark, cfg)}
    out["transactions"] = load_transactions(spark, cfg)
    out["balances"] = load_balances(spark, cfg)
    out["cards"] = load_card_events(spark, cfg)
    out["balance_breaks"] = load_balance_breaks(spark, cfg)
    out["seconds"] = int((datetime.now(timezone.utc) - started).total_seconds())
    return out
