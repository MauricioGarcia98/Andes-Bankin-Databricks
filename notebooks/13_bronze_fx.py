# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Bronze de cotizaciones: BCRA real (principal) + espejo con caos (respaldo)
# MAGIC
# MAGIC Va separado de la API de tarjetas a propósito: si el procesador está caído, las cotizaciones tienen que entrar
# MAGIC igual. Si el espejo falla, se sigue con la fuente real (y queda el aviso).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import uuid

from andes_bank.ingestion import bronze_api
from andes_bank.ingestion.api_client import ApiClient

run_id = f"fx-{uuid.uuid4().hex[:8]}"
currencies = ("USD", "EUR", "BRL")
print("BCRA real:", bronze_api.ingest_fx(spark, cfg, bronze_api.bcra_page_getter(), currencies, "bcra", run_id))

sec = lambda k: dbutils.secrets.get("andes_bank", k)  # noqa: E731
client = ApiClient(sec("card_api_base_url"), sec("card_api_client_id"), sec("card_api_client_secret"), run_id=run_id,
                   max_retries=5)
try:
    print("espejo:", bronze_api.ingest_fx(spark, cfg, bronze_api.mirror_page_getter(client), currencies, "mirror", run_id))
except Exception as exc:
    print(f"AVISO: el espejo no respondió ({str(exc)[:200]}); se sigue con la fuente real del BCRA")
finally:
    bronze_api.flush_calls(spark, cfg, client)
