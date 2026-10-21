# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Espejo del BCRA
# MAGIC
# MAGIC Baja la serie **real** de cotizaciones del BCRA (API pública, paginada) y la deja en `fx-mirror/` para que la
# MAGIC Azure Function la sirva con caos. La fuente real se sigue usando en el lakehouse; el espejo es el respaldo.

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.generator.upload import upload_fx_mirror

print(upload_fx_mirror(cfg, dbutils.secrets.get("andes_bank", "blob_sas_write")))
