"""Carga del CRM generado (`sim.crm_*`) en Salesforce con Bulk API 2.0 (upsert por `Andes_Key__c`).

Upsert = re-correr no duplica. Los hijos (contactos, oportunidades, casos) se relacionan con su cuenta por la clave
externa (`Account.Andes_Key__c`), sin conocer los Id de Salesforce. Orden: primero cuentas, después el resto.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from andes_bank.config import ProjectConfig

LOAD_ORDER = [("accounts", "Account"), ("contacts", "Contact"), ("leads", "Lead"),
              ("opportunities", "Opportunity"), ("cases", "Case")]


def sf_rows(spark: SparkSession, cfg: ProjectConfig, obj: str) -> list[dict]:
    """Filas de `sim.crm_<obj>` sin las columnas de verdad (`_true_*`, `_dirt`) ni los nulos, con la relación
    `Account_Andes_Key__c` escrita como Salesforce la espera (`Account.Andes_Key__c`)."""
    df = spark.table(cfg.table("sim", f"crm_{obj}"))
    cols = [c for c in df.columns if not c.startswith("_")]
    rel = {c: c.replace("Account_", "Account.", 1) if c.startswith("Account_") else c for c in cols}
    return [{rel[k]: v for k, v in row.asDict().items() if v is not None} for row in df.select(*cols).collect()]


def upsert(sf, sobject: str, rows: list[dict]) -> dict:
    results = getattr(sf.bulk2, sobject).upsert(records=rows, external_id_field="Andes_Key__c")
    ok = sum(int(r.get("numberRecordsProcessed", 0)) - int(r.get("numberRecordsFailed", 0)) for r in results)
    failed = sum(int(r.get("numberRecordsFailed", 0)) for r in results)
    out = {"objeto": sobject, "enviados": len(rows), "ok": ok, "fallidos": failed}
    if failed:
        bad = next(r for r in results if int(r.get("numberRecordsFailed", 0)))
        out["errores"] = getattr(sf.bulk2, sobject).get_failed_records(bad["job_id"])[:800]
    return out


def seed_salesforce(spark: SparkSession, cfg: ProjectConfig, sf) -> list[dict]:
    return [upsert(sf, sobject, sf_rows(spark, cfg, obj)) for obj, sobject in LOAD_ORDER]
