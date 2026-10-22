# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Salesforce: preparar la org y cargar el CRM
# MAGIC
# MAGIC 1. Genera el CRM (`sim.crm_*`): ~1.900 registros a partir de una muestra fija de clientes PyME y Premium, con
# MAGIC    la suciedad del catálogo (ID del core vacío o del banco viejo, CUIT con y sin guiones, cuentas duplicadas...).
# MAGIC 2. Prepara la org: campos custom (Tooling API) y un permission set que los hace visibles (sin eso, el upsert
# MAGIC    falla con "No such column").
# MAGIC 3. Upsert con Bulk API 2.0 por `Andes_Key__c`: re-correr no duplica.
# MAGIC
# MAGIC Credenciales: secret scope `andes_bank` (`sf_username`, `sf_password`, `sf_security_token`, `sf_login_url`),
# MAGIC cargadas por `scripts/salesforce/01_secrets.sh`.

# COMMAND ----------

# MAGIC %pip install -q simple-salesforce==1.12.10

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from andes_bank.generator.crm import generate_crm
from andes_bank.salesforce.org import connect, prepare_org
from andes_bank.salesforce.seed import seed_salesforce

print(generate_crm(spark, cfg))
sec = lambda k: dbutils.secrets.get("andes_bank", k)  # noqa: E731
try:
    sec("sf_username")
except Exception:  # todavía no hay Salesforce (etapa 08): no se frena el resto del job
    dbutils.notebook.exit("Sin credenciales de Salesforce en el scope andes_bank: se saltea")
sf = connect(sec("sf_username"), sec("sf_password"), sec("sf_security_token"), sec("sf_login_url"))
print(prepare_org(sf, sec("sf_username")))

# COMMAND ----------

results = seed_salesforce(spark, cfg, sf)
for r in results:
    print(r)
assert all(r["fallidos"] == 0 for r in results), "hubo registros rechazados por Salesforce (ver 'errores')"
