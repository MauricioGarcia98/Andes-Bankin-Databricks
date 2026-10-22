"""Crea los esquemas y el Volume del proyecto (idempotente)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from andes_bank.config import LAYERS, ProjectConfig, is_databricks

SCHEMA_COMMENTS = {
    "sim": "Sistemas origen simulados: la verdad que genera el generador. El lakehouse no la lee.",
    "landing": "Volumes con la copia de lo que llegó de cada sistema origen, checkpoints y errores.",
    "bronze": "Datos crudos tal cual llegaron, una tabla por fuente, con metadatos de ingesta.",
    "silver": "Datos limpios, deduplicados, con historia (SCD2) y cuarentena.",
    "gold": "Modelo dimensional del banco: dimensiones y hechos.",
    "semantic": "Metric views y funciones certificadas para Genie.",
    "ops": "Operación de la plataforma: corridas, llamadas a APIs, calidad y conciliaciones.",
}


def create_namespace(spark: SparkSession, cfg: ProjectConfig) -> None:
    """Esquemas de todas las capas + el Volume de landing.

    El catálogo NO se crea acá: en Free Edition se crea una sola vez por SQL (ver construir/).
    """
    for layer in LAYERS:
        comment = SCHEMA_COMMENTS[layer].replace("'", "")
        if is_databricks():  # pragma: no cover
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)} COMMENT '{comment}'")
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_prefix}{layer}")
    if is_databricks():  # pragma: no cover
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('landing')}.{cfg.volume} "
                  "COMMENT 'Archivos copiados desde Azure, checkpoints de Auto Loader y errores de API'")


def create_ops_tables(spark: SparkSession, cfg: ProjectConfig) -> list[str]:
    """Crea vacías todas las tablas de operación. Así las vistas y los Genie Agents que las consultan existen desde
    el primer deploy, aunque la tarea que las llena todavía no haya corrido (o la API esté caída)."""
    from andes_bank.ingestion.bronze_api import API_CALLS_SCHEMA, RECON_SCHEMA
    from andes_bank.ingestion.landing_sync import LANDING_FILES_SCHEMA
    from andes_bank.quality.gate import RESULT_SCHEMA
    from andes_bank.silver.load import QUARANTINE_DDL
    from andes_bank.utils.delta import WATERMARK_SCHEMA

    tables = {
        cfg.table("ops", "api_calls"): API_CALLS_SCHEMA,
        cfg.table("ops", "api_reconciliation"): RECON_SCHEMA,
        cfg.table("ops", "landing_files"): LANDING_FILES_SCHEMA,
        cfg.table("ops", "dq_results"): RESULT_SCHEMA,
        cfg.table("ops", "silver_watermarks"): WATERMARK_SCHEMA,
        cfg.table("ops", "api_watermarks"): "resource STRING, last_complete_day DATE, updated_at TIMESTAMP",
        cfg.table("silver", "quarantine"): QUARANTINE_DDL,
    }
    created = []
    for name, schema in tables.items():
        if not spark.catalog.tableExists(name):
            spark.createDataFrame([], schema).write.saveAsTable(name)
            created.append(name)
    return created
