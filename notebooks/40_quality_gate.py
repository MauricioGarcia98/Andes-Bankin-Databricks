# Databricks notebook source
# MAGIC %md
# MAGIC # 40 · Quality gate
# MAGIC
# MAGIC Evalúa las reglas (una por problema del catálogo) y **frena el job** si alguna regla `fail` no pasa. Las `warn`
# MAGIC son suciedad conocida y tratada que se sigue midiendo. Resultado en `ops.dq_results`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.quality.gate import gate

dbutils.widgets.text("run_id", "manual", "Run id")
run_id = dbutils.widgets.get("run_id")
try:
    gate(spark, cfg, run_id)
finally:
    display(spark.sql(f"""SELECT rule, severity, catalog_id, failed_rows, total_rows, passed, error
                          FROM {cfg.table('ops', 'dq_results')} WHERE run_id = '{run_id}'
                          ORDER BY passed, severity, rule"""))
