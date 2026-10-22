"""BRONZE de Salesforce: incremental por `SystemModstamp`, con borrados (`queryAll`).

Patrón: SOQL `WHERE SystemModstamp >= watermark` (con solapamiento: lo repetido lo absorbe el MERGE) incluyendo los
registros en la papelera (`IsDeleted`). Cada registro se guarda CRUDO (JSON como texto) y se hace MERGE por Id:
gana la versión con `SystemModstamp` más nuevo. El watermark (`ops.sf_watermarks`) es el mayor `SystemModstamp` visto.

`query_all` se inyecta (en Databricks es `simple_salesforce`; en los tests, una función con datos fijos).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig

AUDIT = ["Id", "IsDeleted", "CreatedDate", "SystemModstamp"]
OBJECTS = {
    # objeto: (tabla bronze, campos)
    "Account": ("sf_account", ["Name", "Type", "Industry", "BillingState", "BillingCity", "Phone", "CUIT__c",
                               "Core_Customer_Id__c", "Segmento__c", "Andes_Key__c", "OwnerId"]),
    "Contact": ("sf_contact", ["AccountId", "FirstName", "LastName", "Email", "Phone", "Title", "Andes_Key__c"]),
    "Lead": ("sf_lead", ["FirstName", "LastName", "Company", "Email", "LeadSource", "Status", "IsConverted",
                         "CUIT__c", "Andes_Key__c"]),
    "Opportunity": ("sf_opportunity", ["AccountId", "Name", "StageName", "CloseDate", "Amount", "IsClosed", "IsWon",
                                       "Producto__c", "Andes_Key__c"]),
    "Case": ("sf_case", ["AccountId", "CaseNumber", "Subject", "Description", "Status", "Priority", "Origin",
                         "IsClosed", "ClosedDate", "Andes_Key__c"]),
}
BRONZE_SF_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType()), T.StructField("raw", T.StringType()),
    T.StructField("system_modstamp", T.TimestampType()), T.StructField("is_deleted", T.BooleanType()),
    T.StructField("_run_id", T.StringType()), T.StructField("_ingested_at", T.TimestampType()),
])
WATERMARK_SCHEMA = T.StructType([T.StructField("object", T.StringType()),
                                 T.StructField("last_modstamp", T.TimestampType()),
                                 T.StructField("updated_at", T.TimestampType())])


def _ts(value: str) -> datetime:
    """'2026-10-21T23:45:45.000+0000' → datetime UTC."""
    return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def soql(obj: str, since: datetime | None) -> str:
    fields = ", ".join(AUDIT + OBJECTS[obj][1])
    where = f" WHERE SystemModstamp >= {since.strftime('%Y-%m-%dT%H:%M:%SZ')}" if since else ""
    return f"SELECT {fields} FROM {obj}{where} ORDER BY SystemModstamp"


def sf_watermark(spark: SparkSession, cfg: ProjectConfig, obj: str) -> datetime | None:
    t = cfg.table("ops", "sf_watermarks")
    if not spark.catalog.tableExists(t):
        return None
    row = spark.table(t).where(F.col("object") == obj).agg(F.max("last_modstamp")).first()
    return row[0].replace(tzinfo=timezone.utc) if row and row[0] else None


def _set_sf_watermark(spark: SparkSession, cfg: ProjectConfig, obj: str, ts: datetime) -> None:
    t = cfg.table("ops", "sf_watermarks")
    if not spark.catalog.tableExists(t):
        spark.createDataFrame([], WATERMARK_SCHEMA).write.saveAsTable(t)
    spark.createDataFrame([(obj, ts, datetime.now(timezone.utc))], WATERMARK_SCHEMA).createOrReplaceTempView("_wm")
    spark.sql(f"""MERGE INTO {t} t USING _wm s ON t.object = s.object
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def ingest_object(spark: SparkSession, cfg: ProjectConfig, query_all: Callable[[str], list[dict]], obj: str,
                  run_id: str) -> dict:
    table = cfg.table("bronze", OBJECTS[obj][0])
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], BRONZE_SF_SCHEMA).write.saveAsTable(table)
    since = sf_watermark(spark, cfg, obj)
    records = [{k: v for k, v in r.items() if k != "attributes"} for r in query_all(soql(obj, since))]
    if not records:
        return {"object": obj, "since": str(since), "rows": 0}
    now = datetime.now(timezone.utc)
    rows = [(r["Id"], json.dumps(r, ensure_ascii=False), _ts(r["SystemModstamp"]), bool(r.get("IsDeleted")),
             run_id, now) for r in records]
    latest: dict[str, tuple] = {}
    for row in rows:                       # una versión por Id (la más nueva); QUALIFY no existe en Spark OSS
        if row[0] not in latest or row[2] >= latest[row[0]][2]:
            latest[row[0]] = row
    rows = list(latest.values())
    spark.createDataFrame(rows, BRONZE_SF_SCHEMA).createOrReplaceTempView("_sf_batch")
    spark.sql(f"""
        MERGE INTO {table} t
        USING _sf_batch s
        ON t.record_id = s.record_id
        WHEN MATCHED AND s.system_modstamp >= t.system_modstamp THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    _set_sf_watermark(spark, cfg, obj, max(r[2] for r in rows))
    return {"object": obj, "since": str(since), "rows": len(rows), "deleted": sum(r[3] for r in rows)}


def ingest_salesforce(spark: SparkSession, cfg: ProjectConfig, query_all: Callable[[str], list[dict]],
                      run_id: str) -> list[dict]:
    return [ingest_object(spark, cfg, query_all, obj, run_id) for obj in OBJECTS]


def simple_salesforce_query_all(sf) -> Callable[[str], list[dict]]:
    """`query_all` con papelera incluida (queryAll) sobre una conexión de simple_salesforce."""
    return lambda q: sf.query_all(q, include_deleted=True)["records"]
