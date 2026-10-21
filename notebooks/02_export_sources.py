# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Exportes sucios de cada sistema origen
# MAGIC
# MAGIC El core (CSV viejo y nuevo, CDC), la integración v1 del procesador y los registros de la API v2, con la
# MAGIC suciedad del catálogo (docs/03). Quedan en `/Volumes/<catálogo>/landing/raw/_staging/`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.generator.exports import export_all

dbutils.widgets.text("only_sources", "", "Sólo estas fuentes (ej. core/tarjetas; vacío = todas)")
only = [x.strip() for x in dbutils.widgets.get("only_sources").split(",") if x.strip()] or None
for name, path in export_all(spark, cfg, only).items():
    print(f"{name:28s} {path}")
