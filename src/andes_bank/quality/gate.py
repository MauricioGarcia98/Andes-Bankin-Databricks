"""Reglas de calidad y QUALITY GATE.

Cada regla es una consulta que cuenta filas que NO cumplen. Severidad:
  * fail → si hay una sola fila mala, el gate frena la corrida (no se publica gold a Genie con datos rotos);
  * warn → se registra y se alerta, pero se sigue (es suciedad conocida y ya tratada, que hay que seguir midiendo).

Todo queda en `ops.dq_results`: el Genie Agent "Plataforma de datos" responde sobre esa tabla.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig


@dataclass(frozen=True)
class Rule:
    name: str
    table: str            # capa.tabla (se resuelve con el catálogo)
    failed_sql: str       # cantidad de filas que NO cumplen ({t} = la tabla, {c} = el catálogo con prefijo)
    total_sql: str        # contra cuántas filas se mide
    severity: str         # fail | warn
    catalog_id: str       # ID del problema en docs/03 (o "-" si es una regla general)
    description: str


RULES = [
    Rule("movimiento_unico", "silver.transactions", "SELECT count(*) - count(DISTINCT movimiento_id) FROM {t}",
         "SELECT count(*) FROM {t}", "fail", "DAT-01", "Cada movimiento aparece una sola vez"),
    Rule("importe_informado", "silver.transactions", "SELECT count(*) FROM {t} WHERE importe IS NULL",
         "SELECT count(*) FROM {t}", "fail", "DAT-02", "Todos los importes se pudieron interpretar"),
    Rule("moneda_de_la_cuenta", "silver.transactions",
         "SELECT count(*) FROM {t} m JOIN {s}.accounts_history a ON a.cuenta_id = m.cuenta_id AND a.is_current "
         "WHERE m.moneda <> a.moneda", "SELECT count(*) FROM {t}", "fail", "DAT-05",
         "La moneda de cada movimiento es la de su cuenta"),
    Rule("moneda_corregida", "silver.transactions", "SELECT count(*) FROM {t} WHERE moneda_corregida",
         "SELECT count(*) FROM {t}", "warn", "DAT-05", "Movimientos que el core informó con la moneda equivocada"),
    Rule("hora_en_utc", "silver.transactions", "SELECT count(*) FROM {t} WHERE fecha_hora_utc IS NULL",
         "SELECT count(*) FROM {t}", "fail", "DAT-06", "Todos los movimientos tienen hora UTC"),
    Rule("cuit_valido", "silver.customers_history", "SELECT count(*) FROM {t} WHERE is_current AND NOT cuit_valido",
         "SELECT count(*) FROM {t} WHERE is_current", "warn", "DAT-07", "CUIT con dígito verificador correcto"),
    Rule("cbu_valido", "silver.accounts_history", "SELECT count(*) FROM {t} WHERE is_current AND NOT cbu_valido",
         "SELECT count(*) FROM {t} WHERE is_current", "warn", "DAT-07", "CBU de 22 dígitos con verificadores correctos"),
    Rule("personas_duplicadas", "silver.customer_xref", "SELECT count(*) FROM {t} WHERE clientes_de_la_persona > 1",
         "SELECT count(*) FROM {t}", "warn", "DAT-08", "Clientes que son la misma persona (fusión 2008)"),
    Rule("observaciones_sin_pii", "silver.customers_history",
         "SELECT count(*) FROM {t} WHERE observaciones RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}'",
         "SELECT count(*) FROM {t}", "fail", "DAT-11", "Ningún email queda en texto libre después de silver"),
    Rule("saldos_concilian", "silver.balance_breaks", "SELECT count(*) FROM {t}",
         "SELECT count(*) FROM {s}.transactions", "warn", "DAT-10", "Movimientos faltantes detectados por saldo"),
    Rule("autorizacion_unica", "silver.card_authorizations", "SELECT count(*) - count(DISTINCT auth_id) FROM {t}",
         "SELECT count(*) FROM {t}", "fail", "API-05", "Cada autorización aparece una sola vez (v1 y v2 incluidas)"),
    Rule("dias_api_completos", "ops.api_reconciliation",
         "SELECT count(*) FROM (SELECT resource, day, max_by(complete, checked_at) AS ok FROM {t} GROUP BY ALL) WHERE NOT ok",
         "SELECT count(DISTINCT resource, day) FROM {t}", "fail", "API-06", "Cada día pedido a la API quedó completo"),
    Rule("archivos_corruptos", "ops.landing_files", "SELECT count(*) FROM {t} WHERE status = 'CORRUPT'",
         "SELECT count(*) FROM {t}", "warn", "ING-03", "Archivos que no se pudieron descomprimir"),
    Rule("archivos_vacios", "ops.landing_files", "SELECT count(*) FROM {t} WHERE status = 'EMPTY'",
         "SELECT count(*) FROM {t}", "warn", "ING-02", "Archivos que llegaron sin filas"),
    Rule("archivos_duplicados", "ops.landing_files", "SELECT count(*) FROM {t} WHERE status = 'DUPLICATE_CONTENT'",
         "SELECT count(*) FROM {t}", "warn", "ING-01", "Archivos con el mismo contenido que otro"),
    Rule("cotizacion_dia_habil", "gold.dim_fecha", "SELECT count(*) FROM {t} WHERE es_dia_habil AND NOT cotizacion_publicada",
         "SELECT count(*) FROM {t} WHERE es_dia_habil", "warn", "DAT-13", "Días hábiles sin cotización publicada"),
    Rule("dolar_siempre_disponible", "gold.dim_fecha", "SELECT count(*) FROM {t} WHERE usd_oficial IS NULL AND fecha >= "
         "(SELECT min(fecha) FROM {s}.fx_rates)", "SELECT count(*) FROM {t}", "fail", "DAT-13",
         "Todo día tiene dólar oficial (el último publicado)"),
    Rule("movimiento_con_cliente", "gold.fact_movimiento", "SELECT count(*) FROM {t} WHERE cliente_id IS NULL",
         "SELECT count(*) FROM {t}", "fail", "-", "Todo movimiento se puede atribuir a un cliente"),
    Rule("compras_debito_conciliadas", "gold.mart_conciliacion_tarjetas", "SELECT coalesce(sum(solo_en_procesador), 0) FROM {t}",
         "SELECT coalesce(sum(compras_procesador), 0) FROM {t}", "warn", "DAT-10",
         "Compras con débito del procesador que no aparecen en el core"),
]

RESULT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("rule", T.StringType()), T.StructField("table_name", T.StringType()),
    T.StructField("severity", T.StringType()), T.StructField("catalog_id", T.StringType()),
    T.StructField("description", T.StringType()), T.StructField("failed_rows", T.LongType()),
    T.StructField("total_rows", T.LongType()), T.StructField("passed", T.BooleanType()),
    T.StructField("error", T.StringType()), T.StructField("checked_at", T.TimestampType()),
])


class QualityGateError(Exception):
    pass


def evaluate(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> list[dict]:
    results, now = [], datetime.now(timezone.utc)
    for r in RULES:
        layer, name = r.table.split(".")
        table = cfg.table(layer, name)
        fmt = {"t": table, "s": cfg.schema("silver")}
        if not spark.catalog.tableExists(table):
            results.append(dict(run_id=run_id, rule=r.name, table_name=table, severity=r.severity, catalog_id=r.catalog_id,
                                description=r.description, failed_rows=None, total_rows=None, passed=r.severity != "fail",
                                error="tabla inexistente", checked_at=now))
            continue
        try:
            failed = spark.sql(r.failed_sql.format(**fmt)).first()[0] or 0
            total = spark.sql(r.total_sql.format(**fmt)).first()[0] or 0
            results.append(dict(run_id=run_id, rule=r.name, table_name=table, severity=r.severity, catalog_id=r.catalog_id,
                                description=r.description, failed_rows=int(failed), total_rows=int(total),
                                passed=r.severity != "fail" or failed == 0, error=None, checked_at=now))
        except Exception as exc:
            results.append(dict(run_id=run_id, rule=r.name, table_name=table, severity=r.severity, catalog_id=r.catalog_id,
                                description=r.description, failed_rows=None, total_rows=None, passed=False,
                                error=str(exc)[:500], checked_at=now))
    spark.createDataFrame(results, RESULT_SCHEMA).write.mode("append").saveAsTable(cfg.table("ops", "dq_results"))
    return results


def gate(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> list[dict]:
    """Evalúa y FRENA (excepción) si alguna regla `fail` no pasa."""
    results = evaluate(spark, cfg, run_id)
    broken = [r for r in results if not r["passed"]]
    if broken:
        detail = "; ".join(f"{r['rule']} ({r['failed_rows']} filas{', ' + r['error'] if r['error'] else ''})" for r in broken)
        raise QualityGateError(f"Quality gate: {len(broken)} reglas bloqueantes fallaron: {detail}")
    return results
