# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Azure Blob → Volume (con validación en el borde)
# MAGIC
# MAGIC Copia lo nuevo de los contenedores `core` y `cards-legacy` al Volume, con la SAS de **sólo lectura**. Los archivos
# MAGIC corruptos van a `_errors/`, los duplicados no pasan. Resultado en `ops.landing_files` (ADR 0002).

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.ingestion.landing_sync import azure_files, run_sync

sas = dbutils.secrets.get("andes_bank", "blob_sas_read")
counts = run_sync(spark, cfg, azure_files(cfg.storage_account, sas, ["core", "cards-legacy"]))
print(counts)
display(spark.sql(f"""
  SELECT status, count(*) AS archivos, sum(data_rows) AS filas, max(synced_at) AS ultima
  FROM {cfg.table('ops', 'landing_files')} GROUP BY status ORDER BY archivos DESC"""))
