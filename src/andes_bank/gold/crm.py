"""GOLD del CRM: la cuenta del CRM unida a la persona del core, oportunidades y casos.

Las oportunidades y los casos de una cuenta duplicada se atribuyen a su cuenta principal: para Comercial, una
empresa es una sola aunque dos ejecutivos la hayan cargado dos veces.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig


def _s(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame:
    return spark.table(cfg.table("silver", name))


def dim_cuenta_crm(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    a = _s(spark, cfg, "crm_accounts")
    cli = spark.table(cfg.table("gold", "dim_cliente")).where("is_current AND cliente_id = persona_id") \
        .select("persona_id", F.col("nombre_completo").alias("nombre_core"), F.col("segmento").alias("segmento_core"))
    return a.join(cli, "persona_id", "left").select(
        "cuenta_crm_id", "nombre", "tipo", "industria", "provincia", "cuit", "persona_id", "nombre_core", "metodo_match",
        "problema_id_core", "es_duplicada", "cuenta_principal_id", "segmento_crm", "segmento_core",
        (F.col("segmento_core").isNotNull() & (F.col("segmento_crm") != F.col("segmento_core"))).alias("segmento_desactualizado"),
        F.to_date("creada_utc").alias("fecha_alta_crm"))


def _principal(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    return _s(spark, cfg, "crm_accounts").select("cuenta_crm_id", "cuenta_principal_id", "persona_id")


def fact_oportunidad(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    o = _s(spark, cfg, "crm_opportunities")
    return o.join(_principal(spark, cfg), "cuenta_crm_id", "left").select(
        "oportunidad_id", "cuenta_principal_id", "persona_id", "producto", "etapa", "cerrada", "ganada", "fecha_cierre",
        "monto_ars", F.to_date("creada_utc").alias("fecha_alta"))


def fact_caso(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    c = _s(spark, cfg, "crm_cases")
    hours = (F.unix_timestamp("cerrado_utc") - F.unix_timestamp("creado_utc")) / 3600
    return c.join(_principal(spark, cfg), "cuenta_crm_id", "left").select(
        "caso_id", "numero", "cuenta_principal_id", "persona_id", "asunto", "descripcion", "estado", "prioridad", "canal",
        "cerrado", F.to_date("creado_utc").alias("fecha_alta"), F.round(hours, 1).alias("horas_resolucion"))


def mart_pipeline_comercial(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Una fila por persona con actividad en el CRM: pipeline abierto, ganado y reclamos abiertos."""
    o = spark.table(cfg.table("gold", "fact_oportunidad")).where("persona_id IS NOT NULL").groupBy("persona_id").agg(
        F.sum(F.when(~F.col("cerrada"), 1).otherwise(0)).alias("oportunidades_abiertas"),
        F.sum(F.when(~F.col("cerrada"), F.col("monto_ars"))).alias("pipeline_abierto_ars"),
        F.sum(F.when(F.col("ganada"), F.col("monto_ars"))).alias("ganado_ars"),
        F.sum(F.when(F.col("ganada"), 1).otherwise(0)).alias("oportunidades_ganadas"))
    c = spark.table(cfg.table("gold", "fact_caso")).where("persona_id IS NOT NULL").groupBy("persona_id").agg(
        F.sum(F.when(~F.col("cerrado"), 1).otherwise(0)).alias("casos_abiertos"), F.count("*").alias("casos_totales"))
    cli = spark.table(cfg.table("gold", "dim_cliente")).where("is_current AND cliente_id = persona_id") \
        .select("persona_id", "nombre_completo", "segmento", "region")
    return (o.join(c, "persona_id", "full").join(cli, "persona_id", "left")
            .fillna(0, subset=["oportunidades_abiertas", "oportunidades_ganadas", "casos_abiertos", "casos_totales"]))


CRM_TABLES = {"dim_cuenta_crm": dim_cuenta_crm, "fact_oportunidad": fact_oportunidad, "fact_caso": fact_caso,
              "mart_pipeline_comercial": mart_pipeline_comercial}


def build_crm_gold(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    if not spark.catalog.tableExists(cfg.table("silver", "crm_accounts")):
        return {}
    out = {}
    for name, fn in CRM_TABLES.items():
        table = cfg.table("gold", name)
        fn(spark, cfg).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        out[name] = spark.table(table).count()
    return out
