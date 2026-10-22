"""BRONZE del CRM comercial: incremental por `SystemModstamp`, con borrados.

Patrón: se le piden a la API (`/crm/v1/<Objeto>?modified_since=`) los registros modificados desde el watermark (con
solapamiento: lo repetido lo absorbe el MERGE), incluidos los borrados (`IsDeleted`). Cada registro se guarda CRUDO
(JSON como texto) y se hace MERGE por Id: gana la versión con `SystemModstamp` más nuevo. El watermark
(`ops.crm_watermarks`) es el mayor `SystemModstamp` visto.

`fetch(objeto, desde)` se inyecta: en Databricks y en los tests es `crm_api_fetcher` sobre un `ApiClient`
(reintentos, token, caos); en un test puntual puede ser una función con datos fijos.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig
from andes_bank.ingestion.api_client import ApiClient

AUDIT = ["Id", "IsDeleted", "CreatedDate", "SystemModstamp"]
OBJECTS = {
    # objeto: (tabla bronze, campos)
    "Account": ("crm_account", ["Name", "Type", "Industry", "BillingState", "BillingCity", "Phone", "CUIT__c",
                                "Core_Customer_Id__c", "Segmento__c", "Andes_Key__c"]),
    "Contact": ("crm_contact", ["AccountId", "FirstName", "LastName", "Email", "Phone", "Title", "Andes_Key__c"]),
    "Lead": ("crm_lead", ["FirstName", "LastName", "Company", "Email", "LeadSource", "Status", "IsConverted",
                          "CUIT__c", "Andes_Key__c"]),
    "Opportunity": ("crm_opportunity", ["AccountId", "Name", "StageName", "CloseDate", "Amount", "IsClosed", "IsWon",
                                        "Producto__c", "Andes_Key__c"]),
    "Case": ("crm_case", ["AccountId", "CaseNumber", "Subject", "Description", "Status", "Priority", "Origin",
                          "IsClosed", "ClosedDate", "Andes_Key__c"]),
}
BRONZE_CRM_SCHEMA = T.StructType([
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


def crm_watermark(spark: SparkSession, cfg: ProjectConfig, obj: str) -> datetime | None:
    t = cfg.table("ops", "crm_watermarks")
    if not spark.catalog.tableExists(t):
        return None
    row = spark.table(t).where(F.col("object") == obj).agg(F.max("last_modstamp")).first()
    return row[0].replace(tzinfo=timezone.utc) if row and row[0] else None


def _set_crm_watermark(spark: SparkSession, cfg: ProjectConfig, obj: str, ts: datetime) -> None:
    t = cfg.table("ops", "crm_watermarks")
    if not spark.catalog.tableExists(t):
        spark.createDataFrame([], WATERMARK_SCHEMA).write.saveAsTable(t)
    spark.createDataFrame([(obj, ts, datetime.now(timezone.utc))], WATERMARK_SCHEMA).createOrReplaceTempView("_wm")
    spark.sql(f"""MERGE INTO {t} t USING _wm s ON t.object = s.object
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def ingest_object(spark: SparkSession, cfg: ProjectConfig, fetch: Callable[[str, datetime | None], list[dict]],
                  obj: str, run_id: str) -> dict:
    table = cfg.table("bronze", OBJECTS[obj][0])
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], BRONZE_CRM_SCHEMA).write.saveAsTable(table)
    since = crm_watermark(spark, cfg, obj)
    fields = AUDIT + OBJECTS[obj][1]
    records = [{f: r.get(f) for f in fields} for r in fetch(obj, since)]
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
    spark.createDataFrame(rows, BRONZE_CRM_SCHEMA).createOrReplaceTempView("_crm_batch")
    spark.sql(f"""
        MERGE INTO {table} t
        USING _crm_batch s
        ON t.record_id = s.record_id
        WHEN MATCHED AND s.system_modstamp >= t.system_modstamp THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    _set_crm_watermark(spark, cfg, obj, max(r[2] for r in rows))
    return {"object": obj, "since": str(since), "rows": len(rows), "deleted": sum(r[3] for r in rows)}


def ingest_crm(spark: SparkSession, cfg: ProjectConfig, fetch: Callable[[str, datetime | None], list[dict]],
               run_id: str) -> list[dict]:
    return [ingest_object(spark, cfg, fetch, obj, run_id) for obj in OBJECTS]


def crm_api_fetcher(client: ApiClient, max_passes: int = 3) -> Callable[[str, datetime | None], list[dict]]:
    """`fetch` sobre la API del CRM: sigue el cursor hasta el final y concilia contra `total`.

    El `ApiClient` ya reintenta cada pedido (429, 5xx, token vencido, JSON cortado). Si igual el recorrido queda
    corto contra el `total` de la primera página, se recorre otra vez.
    """

    def fetch(obj: str, since: datetime | None) -> list[dict]:
        params = {"modified_since": since.strftime("%Y-%m-%dT%H:%M:%S")} if since else {}
        got, total = 0, 0
        for _ in range(max_passes):
            out: dict[str, dict] = {}
            body = client.get(f"crm/v1/{obj}", params)
            total = body["total"]
            while True:
                out.update({r["Id"]: r for r in body["data"]})
                if not body.get("has_more"):
                    break
                body = client.get(f"crm/v1/{obj}", {"cursor": body["next_cursor"]})
            got = len(out)
            if got >= total:
                return list(out.values())
        raise RuntimeError(f"CRM {obj}: llegaron {got} de {total} registros después de {max_passes} recorridos")

    return fetch
