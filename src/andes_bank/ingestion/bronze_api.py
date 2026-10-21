"""BRONZE de APIs: tarjetas (API v2 del procesador) y cotizaciones (BCRA real + espejo con caos).

Cada registro se guarda CRUDO (el JSON tal cual vino, como texto) más su id y metadatos de ingesta. Silver lo parsea
con un esquema explícito; así un campo nuevo (wallet_type, mar-2026) no rompe bronze y se ve en silver.

Idempotencia: cada día se escribe con `replaceWhere` sobre su fecha. Re-ingestar un día lo reemplaza, no lo duplica.
Watermark: `ops.api_watermarks` guarda el último día COMPLETO (conciliado contra los totales) de cada recurso. Un
día incompleto no avanza el watermark: la próxima corrida lo vuelve a pedir.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig
from andes_bank.generator.reference import API_V2_LAUNCH
from andes_bank.ingestion.api_client import ApiClient, DayResult

RESOURCES = {
    # recurso: (tabla bronze, campo id, parámetro de fecha)
    "authorizations": ("cards_api_authorizations", "auth_id", "processing_date"),
    "settlements": ("cards_api_settlements", "settlement_id", "settlement_date"),
}

API_CALLS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("called_at", T.TimestampType()),
    T.StructField("method", T.StringType()), T.StructField("endpoint", T.StringType()),
    T.StructField("params", T.StringType()), T.StructField("status_code", T.IntegerType()),
    T.StructField("attempt", T.IntegerType()), T.StructField("latency_ms", T.LongType()),
    T.StructField("error", T.StringType()), T.StructField("rows", T.LongType()),
])
RECON_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("resource", T.StringType()),
    T.StructField("day", T.DateType()), T.StructField("expected", T.LongType()), T.StructField("received", T.LongType()),
    T.StructField("passes", T.IntegerType()), T.StructField("pages_read", T.IntegerType()),
    T.StructField("duplicates_seen", T.IntegerType()), T.StructField("complete", T.BooleanType()),
    T.StructField("checked_at", T.TimestampType()),
])
BRONZE_API_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType()), T.StructField("raw", T.StringType()),
    T.StructField("_window_date", T.DateType()), T.StructField("_api_version", T.StringType()),
    T.StructField("_run_id", T.StringType()), T.StructField("_ingested_at", T.TimestampType()),
])


def _ensure(spark: SparkSession, table: str, schema: T.StructType) -> None:
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], schema).write.saveAsTable(table)


def watermark(spark: SparkSession, cfg: ProjectConfig, resource: str) -> date | None:
    t = cfg.table("ops", "api_watermarks")
    if not spark.catalog.tableExists(t):
        return None
    row = spark.table(t).where(F.col("resource") == resource).agg(F.max("last_complete_day")).first()
    return row[0] if row else None


def _set_watermark(spark: SparkSession, cfg: ProjectConfig, resource: str, day: date) -> None:
    t = cfg.table("ops", "api_watermarks")
    _ensure(spark, t, T.StructType([T.StructField("resource", T.StringType()),
                                    T.StructField("last_complete_day", T.DateType()),
                                    T.StructField("updated_at", T.TimestampType())]))
    spark.createDataFrame([(resource, day, datetime.now(timezone.utc))], spark.table(t).schema) \
        .write.mode("overwrite").option("replaceWhere", f"resource = '{resource}'").saveAsTable(t)


def days_to_fetch(spark: SparkSession, cfg: ProjectConfig, resource: str, until: date | None = None,
                  max_days: int | None = None) -> list[date]:
    last = watermark(spark, cfg, resource)
    start = (last + timedelta(days=1)) if last else API_V2_LAUNCH
    end = min(until or cfg.end_date, cfg.end_date)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return days[:max_days] if max_days else days


def write_day_batch(spark: SparkSession, cfg: ProjectConfig, resource: str, results: list[DayResult], run_id: str) -> int:
    table = cfg.table("bronze", RESOURCES[resource][0])
    _ensure(spark, table, BRONZE_API_SCHEMA)
    now = datetime.now(timezone.utc)
    rows = [(rid, json.dumps(rec, ensure_ascii=False), date.fromisoformat(r.day), "v2", run_id, now)
            for r in results for rid, rec in r.records.items()]
    days = ", ".join(f"'{r.day}'" for r in results)
    (spark.createDataFrame(rows, BRONZE_API_SCHEMA).write.mode("overwrite")
     .option("replaceWhere", f"_window_date IN ({days})").saveAsTable(table))
    return len(rows)


def ingest_cards_api(spark: SparkSession, cfg: ProjectConfig, client: ApiClient, resource: str,
                     until: date | None = None, max_days: int | None = None, parallel_days: int = 4,
                     batch_days: int = 30) -> dict:
    """Ingesta incremental por día desde el watermark. Devuelve un resumen de la corrida."""
    _, id_field, date_param = RESOURCES[resource]
    days = days_to_fetch(spark, cfg, resource, until, max_days)
    summary = {"resource": resource, "days": len(days), "rows": 0, "incomplete_days": []}
    recon_table = cfg.table("ops", "api_reconciliation")
    _ensure(spark, recon_table, RECON_SCHEMA)
    for i in range(0, len(days), batch_days):
        batch = days[i: i + batch_days]
        with ThreadPoolExecutor(max_workers=parallel_days) as pool:
            results = list(pool.map(lambda d: client.fetch_day(resource, d.isoformat(), id_field, date_param), batch))
        now = datetime.now(timezone.utc)
        spark.createDataFrame([(client.run_id, resource, date.fromisoformat(r.day), r.expected, len(r.records), r.passes,
                                r.pages_read, r.duplicates_seen, r.complete, now) for r in results], RECON_SCHEMA) \
            .write.mode("append").saveAsTable(recon_table)
        summary["rows"] += write_day_batch(spark, cfg, resource, results, client.run_id)
        flush_calls(spark, cfg, client)
        # el watermark avanza hasta el último día completo SIN huecos antes
        complete_prefix = None
        for r in results:
            if not r.complete:
                summary["incomplete_days"].append(r.day)
                break
            complete_prefix = date.fromisoformat(r.day)
        if complete_prefix:
            _set_watermark(spark, cfg, resource, complete_prefix)
        if summary["incomplete_days"]:
            break
    return summary


def flush_calls(spark: SparkSession, cfg: ProjectConfig, client: ApiClient) -> int:
    """Vuelca a ops.api_calls los pedidos registrados por el cliente (y los saca de memoria)."""
    with client._lock:
        calls, client.calls = client.calls, []
    if calls:
        table = cfg.table("ops", "api_calls")
        _ensure(spark, table, API_CALLS_SCHEMA)
        spark.createDataFrame(calls, API_CALLS_SCHEMA).write.mode("append").saveAsTable(table)
    return len(calls)


# ------------------------------------------------------------------------------------------------- FX
FX_SCHEMA = T.StructType([
    T.StructField("fecha", T.StringType()), T.StructField("moneda", T.StringType()), T.StructField("raw", T.StringType()),
    T.StructField("_source", T.StringType()), T.StructField("_run_id", T.StringType()),
    T.StructField("_ingested_at", T.TimestampType()),
])


def _fx_rows(results: list[dict], currency: str, source: str, run_id: str) -> list[tuple]:
    now = datetime.now(timezone.utc)
    return [(day.get("fecha"), currency, json.dumps(d, ensure_ascii=False), source, run_id, now)
            for day in results for d in day.get("detalle", [])]


def ingest_fx(spark: SparkSession, cfg: ProjectConfig, get_page, currencies: tuple[str, ...], source: str,
              run_id: str, start: date | None = None, end: date | None = None) -> int:
    """Cotizaciones paginadas (offset/limit de a 1.000) → bronze.fx_<source>, reemplazando el rango pedido.

    `get_page(currency, desde, hasta, offset)` devuelve el cuerpo de la respuesta (BCRA real o espejo).
    """
    start, end = start or cfg.start_date, end or cfg.end_date
    table = cfg.table("bronze", f"fx_{source}")
    _ensure(spark, table, FX_SCHEMA)
    rows: list[tuple] = []
    for cur in currencies:
        offset = 0
        while True:
            body = get_page(cur, start.isoformat(), end.isoformat(), offset)
            rows += _fx_rows(body.get("results", []), cur, source, run_id)
            offset += 1000
            if offset >= body.get("metadata", {}).get("resultset", {}).get("count", 0):
                break
    cond = f"fecha BETWEEN '{start}' AND '{end}' AND moneda IN ({', '.join(repr(c) for c in currencies)})"
    spark.createDataFrame(rows, FX_SCHEMA).write.mode("overwrite").option("replaceWhere", cond).saveAsTable(table)
    return len(rows)


def bcra_page_getter(session=None, retries: int = 5, sleep=None):
    """Lector de la API REAL del BCRA (sin token). También falla a veces: se reintenta con backoff."""
    import time

    import requests

    s = session or requests.Session()
    sleep = sleep or time.sleep
    url = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{}"

    def get(currency: str, desde: str, hasta: str, offset: int) -> dict:
        for attempt in range(1, retries + 1):
            try:
                r = s.get(url.format(currency), timeout=30,
                          params={"fechadesde": desde, "fechahasta": hasta, "offset": offset, "limit": 1000})
                if r.status_code == 200:
                    return r.json()
            except Exception:
                pass
            sleep(2 ** attempt)
        raise RuntimeError(f"BCRA no respondió para {currency} {desde}..{hasta} offset={offset}")

    return get


def mirror_page_getter(client: ApiClient):
    def get(currency: str, desde: str, hasta: str, offset: int) -> dict:
        return client.get(f"fx/estadisticascambiarias/v1.0/Cotizaciones/{currency}",
                          {"fechadesde": desde, "fechahasta": hasta, "offset": offset, "limit": 1000})

    return get
