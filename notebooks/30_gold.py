# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Gold: modelo dimensional y marts
# MAGIC
# MAGIC Dimensiones (fecha con dólar oficial, sucursal, cliente SCD2, cuenta, tarjeta, comercio), hechos (movimientos,
# MAGIC autorizaciones con su liquidación, saldos diarios) y los marts que usa Genie.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.gold.marts import build_marts
from andes_bank.gold.model import build_gold

out = build_gold(spark, cfg)
out.update(build_marts(spark, cfg))
display(spark.createDataFrame(list(out.items()), "tabla string, filas long"))
