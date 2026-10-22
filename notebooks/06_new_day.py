# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Simular días nuevos
# MAGIC
# MAGIC El banco sigue después de la historia: agrega a `sim.*` los próximos `dias` días desde el último generado
# MAGIC (autorizaciones, liquidaciones, movimientos y saldos, con las mismas fórmulas que la historia), los exporta sucios
# MAGIC y los sube a Azure. Es una **simulación a pedido**, no sigue el calendario real. Deja en `ultimo_dia` hasta dónde
# MAGIC llegó, para que el lakehouse (la tarea siguiente del job) procese exactamente esos días.

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import logging

from andes_bank.generator.daily import last_generated_day, run_new_days

dbutils.widgets.text("dias", "1", "Días a simular")
dias = int(dbutils.widgets.get("dias"))
logging.basicConfig(level=logging.INFO)
print("último día generado:", last_generated_day(spark, cfg), "| días a simular:", dias)
out = run_new_days(spark, cfg, dbutils.secrets.get("andes_bank", "blob_sas_write"), days=dias)
ultimo = last_generated_day(spark, cfg)
dbutils.jobs.taskValues.set("ultimo_dia", ultimo.isoformat())
print(out, "| el lakehouse procesa hasta", ultimo)
display(spark.sql(f"SELECT * FROM {cfg.table('sim', 'generated_days')} ORDER BY day DESC LIMIT 10"))
