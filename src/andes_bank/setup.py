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
