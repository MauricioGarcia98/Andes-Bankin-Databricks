# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Subir los exportes a Azure Blob
# MAGIC
# MAGIC Simula que cada sistema origen deja sus archivos en Azure: el core en `core/`, la integración vieja del
# MAGIC procesador en `cards-legacy/` y las páginas que sirve la API en `api-data/`. Usa la SAS de **escritura** del
# MAGIC secret scope (la del lakehouse es de sólo lectura).

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import logging
from datetime import date

from andes_bank.generator.upload import upload_all

logging.basicConfig(level=logging.INFO)
dbutils.widgets.text("upload_from", "", "Subir desde (vacío = todo)")
dbutils.widgets.text("parts", "core,cards_legacy,api_data", "Partes")
start = dbutils.widgets.get("upload_from")
parts = tuple(p.strip() for p in dbutils.widgets.get("parts").split(",") if p.strip())
sas = dbutils.secrets.get("andes_bank", "blob_sas_write")
print(upload_all(cfg, sas, start=date.fromisoformat(start) if start else None, parts=parts))
