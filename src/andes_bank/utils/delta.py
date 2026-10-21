"""Operaciones Delta reutilizables: crear si no existe, MERGE insert-only y watermarks por tabla."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig, is_databricks


def create_if_not_exists(spark: SparkSession, full_name: str, df: DataFrame, cluster_by: Sequence[str] = ()) -> bool:
    """Crea la tabla vacía con el esquema de `df`. En Databricks, con Liquid Clustering (reemplaza a PARTITIONED BY
    + ZORDER en tablas nuevas)."""
    if spark.catalog.tableExists(full_name):
        return False
    writer = df.limit(0).writeTo(full_name).using("delta")
    if is_databricks() and cluster_by:  # pragma: no cover
        writer = writer.clusterBy(*cluster_by)
    writer.create()
    return True


def merge_insert_only(spark: SparkSession, source: DataFrame, target: str, keys: Sequence[str],
                      cluster_by: Sequence[str] = ()) -> None:
    """Inserta sólo las claves que no existen: la primera copia que llegó gana (idempotente ante re-envíos)."""
    create_if_not_exists(spark, target, source, cluster_by)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    DeltaTable.forName(spark, target).alias("t").merge(source.alias("s"), cond).whenNotMatchedInsertAll().execute()


def merge_upsert_when(spark: SparkSession, source: DataFrame, target: str, keys: Sequence[str], update_when: str,
                      cluster_by: Sequence[str] = ()) -> None:
    """Inserta lo nuevo y actualiza lo existente sólo cuando se cumple `update_when` (ej.: la v2 pisa a la v1)."""
    create_if_not_exists(spark, target, source, cluster_by)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    (DeltaTable.forName(spark, target).alias("t").merge(source.alias("s"), cond)
     .whenMatchedUpdateAll(condition=update_when).whenNotMatchedInsertAll().execute())


WATERMARK_SCHEMA = T.StructType([T.StructField("entity", T.StringType()), T.StructField("last_ingested_at", T.TimestampType()),
                                 T.StructField("updated_at", T.TimestampType())])


def get_watermark(spark: SparkSession, cfg: ProjectConfig, entity: str):
    t = cfg.table("ops", "silver_watermarks")
    if not spark.catalog.tableExists(t):
        return None
    return spark.table(t).where(F.col("entity") == entity).agg(F.max("last_ingested_at")).first()[0]


def set_watermark(spark: SparkSession, cfg: ProjectConfig, entity: str, value) -> None:
    if value is None:
        return
    t = cfg.table("ops", "silver_watermarks")
    if not spark.catalog.tableExists(t):
        spark.createDataFrame([], WATERMARK_SCHEMA).write.saveAsTable(t)
    spark.createDataFrame([(entity, value, datetime.now(timezone.utc))], WATERMARK_SCHEMA) \
        .write.mode("overwrite").option("replaceWhere", f"entity = '{entity}'").saveAsTable(t)
