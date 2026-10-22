# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Gold: modelo dimensional y marts
# MAGIC
# MAGIC Dimensiones (fecha con dólar oficial, sucursal, cliente SCD2, cuenta, tarjeta, comercio), hechos (movimientos,
# MAGIC autorizaciones con su liquidación, saldos diarios), los marts que usa Genie y el CRM unido al core.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.gold.crm import build_crm_gold
from andes_bank.gold.marts import build_marts
from andes_bank.gold.model import build_gold

out = build_gold(spark, cfg)
out.update(build_marts(spark, cfg))
out.update(build_crm_gold(spark, cfg))  # vacío si todavía no hay Salesforce
display(spark.createDataFrame(list(out.items()), "tabla string, filas long"))
