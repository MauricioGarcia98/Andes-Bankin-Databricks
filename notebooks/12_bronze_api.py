# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Bronze de la API de tarjetas (v2)
# MAGIC
# MAGIC El cliente robusto (`andes_bank.ingestion.api_client`) recorre cada día desde el watermark, sobrevive al caos de la
# MAGIC API y concilia contra los totales. Cada pedido queda en `ops.api_calls`; cada día, en `ops.api_reconciliation`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import uuid
from datetime import date

from andes_bank.ingestion import bronze_api
from andes_bank.ingestion.api_client import ApiClient

dbutils.widgets.text("max_days", "", "Máximo de días por recurso (vacío = todos)")
max_days = int(dbutils.widgets.get("max_days")) if dbutils.widgets.get("max_days") else None
run_id = f"api-{uuid.uuid4().hex[:8]}"
sec = lambda k: dbutils.secrets.get("andes_bank", k)  # noqa: E731
client = ApiClient(sec("card_api_base_url"), sec("card_api_client_id"), sec("card_api_client_secret"), run_id=run_id)

for resource in ("authorizations", "settlements"):
    print(bronze_api.ingest_cards_api(spark, cfg, client, resource, max_days=max_days))

bronze_api.flush_calls(spark, cfg, client)

display(spark.sql(f"""
  SELECT endpoint, status_code, count(*) AS pedidos, round(avg(latency_ms)) AS latencia_ms
  FROM {cfg.table('ops', 'api_calls')} WHERE run_id = '{run_id}' GROUP BY ALL ORDER BY pedidos DESC"""))
