"""Helpers para obtener/crear la SparkSession en Databricks y en local."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from andes_bank.config import is_databricks


def get_spark(app_name: str = "andes_bank") -> SparkSession:
    """En Databricks devuelve la sesión activa (serverless). En local crea una con Delta Lake OSS."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():  # pragma: no cover
        return SparkSession.builder.getOrCreate()
    return build_local_spark(app_name)


def build_local_spark(app_name: str = "andes_bank-local", warehouse_dir: str | None = None) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # en Databricks el formato por defecto es Delta; en Spark OSS es Parquet. Sin esto, saveAsTable crea tablas
        # Parquet en local y las opciones de Delta (replaceWhere) se ignoran EN SILENCIO (bitácora R09).
        .config("spark.sql.sources.default", "delta")
    )
    if warehouse_dir:
        # metastore Derby persistente: las tablas sobreviven entre ejecuciones locales
        builder = (
            builder.config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={warehouse_dir}/metastore_db;create=true")
        )
    if os.environ.get("ANDES_DELTA_JARS_IN_CLASSPATH") == "1":
        # entornos sin acceso a Maven: los jars de Delta ya están en pyspark/jars
        return builder.getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()
