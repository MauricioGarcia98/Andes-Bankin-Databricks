# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Bronze de archivos (Auto Loader)
# MAGIC
# MAGIC Dos contratos por fuente del core (`*_legacy` antes de la migración de mar-2024, y el nuevo) y los exportes v1 del
# MAGIC procesador. Todo como texto, con linaje. Incremental: sólo procesa archivos nuevos.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.text("tables", "", "Tablas (vacío = todas)")
from andes_bank.ingestion.bronze_files import ingest_all

tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()] or None
out = ingest_all(spark, cfg, tables)
display(spark.createDataFrame(list(out.items()), "tabla string, filas_nuevas long"))
