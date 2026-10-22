# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: esquemas y volumen
# MAGIC
# MAGIC **Antes del primer deploy** el catálogo tiene que existir. En Free Edition se crea una vez desde el SQL Editor:
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS andes_bank COMMENT 'Andes Bank: lakehouse bancario E2E';
# MAGIC ```
# MAGIC Este notebook crea las capas `sim, landing, bronze, silver, gold, semantic, ops` el volumen `landing.raw` y
# MAGIC las tablas de `ops` vacías (para que las vistas y Genie las encuentren aunque la tarea que las llena no haya corrido).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.setup import create_namespace, create_ops_tables

create_namespace(spark, cfg)
print("tablas de ops creadas:", create_ops_tables(spark, cfg))
display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}"))
