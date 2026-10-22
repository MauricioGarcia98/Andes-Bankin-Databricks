# Databricks notebook source
# MAGIC %md
# MAGIC # 35 · Gobierno y capa semántica
# MAGIC
# MAGIC Gold se reescribe en cada corrida, así que comentarios, claves, tags, máscaras y filtros se vuelven a aplicar
# MAGIC después (son idempotentes). Después, la capa semántica: vistas anchas, metric views y funciones certificadas.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.sql_runner import run_folder

print(run_folder(spark, cfg, "governance"))
print(run_folder(spark, cfg, "semantic"))
if spark.catalog.tableExists(cfg.table("gold", "dim_cuenta_crm")):  # el CRM llega en la etapa 08
    print(run_folder(spark, cfg, "crm"))
