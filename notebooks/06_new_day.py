# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Día nuevo
# MAGIC
# MAGIC El banco sigue vivo: agrega a `sim.*` cada día que falta desde el último generado hasta ayer (autorizaciones,
# MAGIC liquidaciones, movimientos y saldos, con las mismas fórmulas que la historia), lo exporta sucio y lo sube a Azure.
# MAGIC Corre todas las mañanas antes del lakehouse (job `andes-bank-dia-nuevo`). Re-correrlo es seguro: un día ya
# MAGIC generado no se vuelve a generar, y uno que quedó a la mitad se borra y se rehace.

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import logging

from andes_bank.generator.daily import last_generated_day, run_new_days

logging.basicConfig(level=logging.INFO)
print("último día generado:", last_generated_day(spark, cfg), "| hasta:", cfg.end_date)
out = run_new_days(spark, cfg, dbutils.secrets.get("andes_bank", "blob_sas_write"))
print(out or "nada que generar: ya está al día")
display(spark.sql(f"SELECT * FROM {cfg.table('sim', 'generated_days')} ORDER BY day DESC LIMIT 10"))
