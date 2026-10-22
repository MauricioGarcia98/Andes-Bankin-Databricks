# Databricks notebook source
# MAGIC %md
# MAGIC # 14 · Bronze de Salesforce
# MAGIC
# MAGIC Incremental por `SystemModstamp` con `queryAll` (trae también lo que está en la papelera: un borrado en el CRM
# MAGIC tiene que desaparecer del lakehouse). MERGE por Id: gana la versión más nueva.

# COMMAND ----------

# MAGIC %pip install -q simple-salesforce==1.12.10

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import uuid

from andes_bank.ingestion import bronze_salesforce
from andes_bank.salesforce.org import connect

sec = lambda k: dbutils.secrets.get("andes_bank", k)  # noqa: E731
try:
    sec("sf_username")
except Exception:  # todavía no hay Salesforce (etapa 08): no se frena el resto del job
    dbutils.notebook.exit("Sin credenciales de Salesforce en el scope andes_bank: se saltea")
sf = connect(sec("sf_username"), sec("sf_password"), sec("sf_security_token"), sec("sf_login_url"))
out = bronze_salesforce.ingest_salesforce(spark, cfg, bronze_salesforce.simple_salesforce_query_all(sf),
                                          f"sf-{uuid.uuid4().hex[:8]}")
display(spark.createDataFrame([(r["object"], r["since"], r["rows"], r.get("deleted", 0)) for r in out],
                              "objeto string, desde string, filas long, borrados long"))
