# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Silver
# MAGIC
# MAGIC Maestros con historia (SCD2) reconstruidos desde bronze; movimientos, saldos y tarjetas incrementales (MERGE
# MAGIC insert-only, la v2 pisa a la v1); conciliación de saldos y cuarentena. Ver `andes_bank.silver`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json

from andes_bank.silver.load import load_all

print(json.dumps(load_all(spark, cfg), indent=2, default=str))
