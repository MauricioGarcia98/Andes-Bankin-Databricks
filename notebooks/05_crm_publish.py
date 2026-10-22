# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · CRM comercial: generar y publicar
# MAGIC
# MAGIC 1. Genera el CRM (`sim.crm_*`): ~1.900 registros a partir de una muestra fija de clientes PyME y Premium, con
# MAGIC    la suciedad del catálogo (ID del core vacío o del banco viejo, CUIT con y sin guiones, cuentas duplicadas...).
# MAGIC 2. Arma lo que "tiene adentro" el CRM (Id, fechas de alta y de modificación, borrados) y lo sube a
# MAGIC    `crm-data/<Objeto>.json` en Azure. La API lo sirve en `/crm/v1/<Objeto>`, con el mismo token y el mismo caos
# MAGIC    que la de tarjetas.
# MAGIC
# MAGIC Re-correr no cambia nada: los Id y las fechas salen de la semilla.

# COMMAND ----------

# MAGIC %pip install -q azure-storage-blob==12.24.0

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.generator.crm import crm_state, generate_crm
from andes_bank.generator.upload import upload_crm

print(generate_crm(spark, cfg))
print(upload_crm(cfg, dbutils.secrets.get("andes_bank", "blob_sas_write"), crm_state(spark, cfg)))
