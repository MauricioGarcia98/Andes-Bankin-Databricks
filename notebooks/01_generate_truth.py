# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · La verdad del banco (`sim.*`)
# MAGIC
# MAGIC Genera clientes, cuentas, tarjetas, comercios, autorizaciones, liquidaciones, movimientos y saldos con una
# MAGIC semilla fija. Es "lo que pasó de verdad": el lakehouse no lo lee, lo usan las conciliaciones de control.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import time

from andes_bank.generator.truth import generate_truth

t0 = time.time()
counts = generate_truth(spark, cfg)
print(f"{time.time() - t0:.0f} s")
display(spark.createDataFrame(list(counts.items()), "tabla string, filas long"))
