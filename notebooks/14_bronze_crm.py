# Databricks notebook source
# MAGIC %md
# MAGIC # 14 · Bronze del CRM comercial
# MAGIC
# MAGIC Incremental por `SystemModstamp` contra la API (`/crm/v1/<Objeto>?modified_since=`), que trae también los
# MAGIC borrados: un registro borrado en el CRM tiene que desaparecer del lakehouse. MERGE por Id: gana la versión más
# MAGIC nueva. Cada pedido queda en `ops.api_calls`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import uuid

from andes_bank.ingestion import bronze_api, bronze_crm
from andes_bank.ingestion.api_client import ApiClient

run_id = f"crm-{uuid.uuid4().hex[:8]}"
sec = lambda k: dbutils.secrets.get("andes_bank", k)  # noqa: E731
client = ApiClient(sec("card_api_base_url"), sec("card_api_client_id"), sec("card_api_client_secret"), run_id=run_id)
out = bronze_crm.ingest_crm(spark, cfg, bronze_crm.crm_api_fetcher(client), run_id)
bronze_api.flush_calls(spark, cfg, client)
display(spark.createDataFrame([(r["object"], r["since"], r["rows"], r.get("deleted", 0)) for r in out],
                              "objeto string, desde string, filas long, borrados long"))
