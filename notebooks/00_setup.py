# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: esquemas y volumen
# MAGIC
# MAGIC **Antes del primer deploy** el catálogo tiene que existir. En Free Edition se crea una vez desde el SQL Editor:
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS andes_bank COMMENT 'Andes Bank: lakehouse bancario E2E';
# MAGIC ```
# MAGIC Este notebook crea las capas `sim, landing, bronze, silver, gold, semantic, ops` y el volumen `landing.raw`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.setup import create_namespace

create_namespace(spark, cfg)
display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}"))
