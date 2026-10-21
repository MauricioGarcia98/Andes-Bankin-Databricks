"""GOLD: el modelo dimensional del banco (lo que consultan Genie, los dashboards y las áreas).

Dimensiones                         Hechos (grano)
  dim_fecha (con dólar oficial)       fact_movimiento      un movimiento de cuenta
  dim_sucursal                        fact_autorizacion    una autorización de tarjeta (+ su liquidación)
  dim_cliente (SCD2 + persona)        fact_saldo_diario    el saldo de cierre de una cuenta en un día informado
  dim_cuenta · dim_tarjeta · dim_comercio

Convenciones: montos en su moneda (`importe`) y SIEMPRE también en pesos y en dólares al oficial del día
(`importe_ars`, `importe_usd`), para que nadie tenga que convertir a mano en una pregunta a Genie.
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.generator import reference as ref

TZ = "America/Argentina/Buenos_Aires"
MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre",
         "diciembre"]
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def _s(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame:
    return spark.table(cfg.table("silver", name))


# --------------------------------------------------------------------------------------------- dimensiones
def dim_fecha(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Calendario con feriados y la cotización oficial (USD, EUR, BRL). Fines de semana y feriados repiten la
    última cotización publicada (DAT-13): el BCRA no publica esos días."""
    days = [cfg.start_date + timedelta(days=i) for i in range(cfg.n_days)]
    cal = spark.createDataFrame(
        [(d, d.year, d.month, MESES[d.month - 1], (d.month - 1) // 3 + 1, DIAS[d.weekday()], d.weekday() >= 5,
          d.isoformat() in ref.HOLIDAYS) for d in days],
        "fecha date, anio int, mes int, nombre_mes string, trimestre int, dia_semana string, es_fin_de_semana boolean, "
        "es_feriado boolean")
    fx = _s(spark, cfg, "fx_rates").groupBy("fecha").pivot("moneda", ["USD", "EUR", "BRL"]).agg(F.first("ars_por_unidad"))
    src = _s(spark, cfg, "fx_rates").where("moneda = 'USD'").select("fecha", F.col("fx_fuente").alias("fx_fuente_usd"))
    w = Window.orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)
    return (
        cal.join(fx, "fecha", "left").join(src, "fecha", "left")
        .withColumn("cotizacion_publicada", F.col("USD").isNotNull())
        .withColumn("usd_oficial", F.last("USD", ignorenulls=True).over(w).cast("decimal(18,4)"))
        .withColumn("eur_oficial", F.last("EUR", ignorenulls=True).over(w).cast("decimal(18,4)"))
        .withColumn("brl_oficial", F.last("BRL", ignorenulls=True).over(w).cast("decimal(18,4)"))
        .withColumn("es_dia_habil", ~F.col("es_fin_de_semana") & ~F.col("es_feriado"))
        .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
        .withColumn("es_fin_de_mes", F.col("fecha") == F.last_day("fecha"))
        .drop("USD", "EUR", "BRL")
    )


def dim_sucursal(spark: SparkSession) -> DataFrame:
    """Maestro interno de sucursales (no viene de un sistema origen: es referencia del banco)."""
    return spark.createDataFrame(ref.branches()).select(
        F.col("branch_id").cast("int").alias("sucursal_id"), F.col("branch_name").alias("sucursal"),
        F.col("city").alias("ciudad"), F.col("province").alias("provincia"), F.col("region").alias("region"),
        F.col("opened_year").cast("int").alias("anio_apertura"))


def dim_cliente(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    h = _s(spark, cfg, "customers_history")
    xref = _s(spark, cfg, "customer_xref").select("cliente_id", "persona_id", F.col("regla").alias("regla_identidad"))
    suc = dim_sucursal(spark).select("sucursal_id", "region")
    age = F.floor(F.months_between(F.lit(cfg.end_date.isoformat()).cast("date"), "fecha_nacimiento") / 12).cast("int")
    return (
        h.join(xref, "cliente_id", "left").join(suc, "sucursal_id", "left")
        .withColumn("persona_id", F.coalesce("persona_id", "cliente_id"))
        .withColumn("cliente_sk", F.concat_ws("#", "cliente_id", F.col("version").cast("string")))
        .withColumn("edad", age)
        .withColumn("rango_edad", F.when(F.col("tipo_persona") == "JURIDICA", "EMPRESA").when(F.col("edad") < 30, "18-29")
                    .when(F.col("edad") < 45, "30-44").when(F.col("edad") < 60, "45-59").otherwise("60+"))
        .withColumn("banco_origen", F.when(F.col("cliente_id").startswith("CUY-"), "Cuyo (original)")
                    .when(F.col("cliente_id").startswith("NOA-"), "NOA (adquirido 2008)").otherwise("Andes Bank"))
        .select("cliente_sk", "cliente_id", "persona_id", "regla_identidad", "nombre_completo", "tipo_persona", "segmento",
                "region", "provincia", "localidad", "sucursal_id", "fecha_alta", "fecha_nacimiento", "edad", "rango_edad",
                "banco_origen", "dni", "cuit", "cuit_valido", "email", "telefono", "estado", "valid_from", "valid_to",
                "is_current", "version")
    )


def dim_cuenta(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    a = _s(spark, cfg, "accounts_history").where("is_current")
    return a.select("cuenta_id", "cliente_id", "tipo_cuenta", "moneda", "sucursal_id", "fecha_apertura", "fecha_cierre",
                    "limite_descubierto", "cbu", "cbu_valido", "estado")


def dim_tarjeta(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    c = _s(spark, cfg, "cards_history").where("is_current")
    return c.select("token_tarjeta", "cliente_id", "cuenta_id", F.col("tipo").alias("tipo_tarjeta"), "marca", "ultimos4",
                    "fecha_emision", F.col("limite").alias("limite_credito"), "estado")


def dim_comercio(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    a = _s(spark, cfg, "card_authorizations")
    w = Window.partitionBy("comercio_id").orderBy(F.desc("autorizada_utc"))
    rubro = F.create_map(*[x for k, v in ref.MCC.items() for x in (F.lit(k), F.lit(v[0]))])
    return (
        a.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
        .select("comercio_id", F.col("comercio_nombre").alias("comercio"), "mcc", rubro[F.col("mcc")].alias("rubro"),
                "pais_comercio", "ciudad_comercio", (F.col("pais_comercio") != "AR").alias("es_exterior"),
                F.col("mcc").isin("5732", "5944", "7995", "5815", "4511").alias("rubro_riesgoso"))
    )


# -------------------------------------------------------------------------------------------------- hechos
def _fx(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    return spark.table(cfg.table("gold", "dim_fecha")).select("fecha", "usd_oficial")


def fact_movimiento(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    t = _s(spark, cfg, "transactions")
    acc = spark.table(cfg.table("gold", "dim_cuenta")).select("cuenta_id", "cliente_id", "tipo_cuenta", "sucursal_id")
    xref = _s(spark, cfg, "customer_xref").select("cliente_id", "persona_id")
    fx = _fx(spark, cfg).withColumnRenamed("fecha", "fecha_contable")
    return (
        t.join(acc, "cuenta_id", "left").join(xref, "cliente_id", "left").join(F.broadcast(fx), "fecha_contable", "left")
        .withColumn("persona_id", F.coalesce("persona_id", "cliente_id"))
        .withColumn("importe_ars", F.when(F.col("moneda") == "USD", F.col("importe") * F.col("usd_oficial"))
                    .otherwise(F.col("importe")).cast("decimal(18,2)"))
        .withColumn("importe_usd", F.when(F.col("moneda") == "USD", F.col("importe"))
                    .otherwise(F.col("importe") / F.col("usd_oficial")).cast("decimal(18,2)"))
        .withColumn("es_credito", F.col("importe") > 0)
        .select("movimiento_id", "cuenta_id", "cliente_id", "persona_id", "tipo_cuenta", "sucursal_id", "fecha_contable",
                "fecha_hora_utc", "fecha_hora_local", "concepto", "canal", "canal_digital", "importe", "moneda", "importe_ars",
                "importe_usd", "es_credito", "saldo", "referencia", "moneda_corregida", "sistema_origen")
    )


def fact_autorizacion(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    a = _s(spark, cfg, "card_authorizations")
    s = (_s(spark, cfg, "card_settlements").groupBy("auth_id")
         .agg(F.min("fecha_liquidacion").alias("fecha_liquidacion"), F.sum("importe_ars").alias("importe_liquidado_ars")))
    card = spark.table(cfg.table("gold", "dim_tarjeta")).select("token_tarjeta", "cliente_id")
    fx = _fx(spark, cfg)
    local = F.from_utc_timestamp("autorizada_utc", TZ)
    return (
        a.join(s, "auth_id", "left").join(card, "token_tarjeta", "left")
        .withColumn("fecha", F.to_date(local)).withColumn("hora_local", F.hour(local))
        .join(F.broadcast(fx), "fecha", "left")
        .withColumn("importe_usd", F.when(F.col("moneda") == "USD", F.col("importe"))
                    .otherwise(F.col("importe_ars") / F.col("usd_oficial")).cast("decimal(18,2)"))
        .withColumn("aprobada", F.col("estado") == "APPROVED")
        .withColumn("rechazo_por_sospecha", F.col("codigo_respuesta") == "59_SOSPECHA_FRAUDE")
        .withColumn("liquidada", F.col("fecha_liquidacion").isNotNull())
        .withColumn("dias_a_liquidar", F.datediff("fecha_liquidacion", "fecha"))
        .withColumn("diferencia_liquidacion_ars", (F.col("importe_liquidado_ars") - F.col("importe_ars")).cast("decimal(18,2)"))
        .withColumn("llego_tarde", F.col("fecha_proceso") > F.col("fecha"))
        .select("auth_id", "token_tarjeta", "cliente_id", "tipo_tarjeta", "marca", "comercio_id", "mcc", "pais_comercio",
                "fecha", "hora_local", "autorizada_utc", "fecha_proceso", "llego_tarde", "importe", "moneda", "importe_ars",
                "importe_usd", "estado", "aprobada", "codigo_respuesta", "rechazo_por_sospecha", "modo_ingreso", "billetera",
                "liquidada", "fecha_liquidacion", "dias_a_liquidar", "importe_liquidado_ars", "diferencia_liquidacion_ars",
                "version_api")
    )


def fact_saldo_diario(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    b = _s(spark, cfg, "balances")
    acc = spark.table(cfg.table("gold", "dim_cuenta")).select("cuenta_id", "cliente_id", "tipo_cuenta", "sucursal_id",
                                                              F.col("moneda").alias("moneda_cuenta"))
    fx = _fx(spark, cfg).withColumnRenamed("fecha", "fecha_saldo")
    return (
        b.join(acc, "cuenta_id", "left").join(F.broadcast(fx), "fecha_saldo", "left")
        .withColumn("moneda", F.coalesce("moneda_cuenta", "moneda"))
        .withColumn("saldo_ars", F.when(F.col("moneda") == "USD", F.col("saldo_cierre") * F.col("usd_oficial"))
                    .otherwise(F.col("saldo_cierre")).cast("decimal(18,2)"))
        .withColumn("saldo_usd", F.when(F.col("moneda") == "USD", F.col("saldo_cierre"))
                    .otherwise(F.col("saldo_cierre") / F.col("usd_oficial")).cast("decimal(18,2)"))
        .withColumn("es_fin_de_mes", F.col("fecha_saldo") == F.last_day("fecha_saldo"))
        .select(F.col("fecha_saldo").alias("fecha"), "cuenta_id", "cliente_id", "tipo_cuenta", "sucursal_id", "moneda",
                F.col("saldo_cierre").alias("saldo"), "saldo_ars", "saldo_usd", "es_fin_de_mes")
    )


DIMENSIONS = {"dim_fecha": dim_fecha, "dim_cliente": dim_cliente, "dim_cuenta": dim_cuenta, "dim_tarjeta": dim_tarjeta,
              "dim_comercio": dim_comercio}
FACTS = {"fact_movimiento": fact_movimiento, "fact_autorizacion": fact_autorizacion, "fact_saldo_diario": fact_saldo_diario}
CLUSTER = {"fact_movimiento": ["fecha_contable", "cliente_id"], "fact_autorizacion": ["fecha", "comercio_id"],
           "fact_saldo_diario": ["fecha", "cuenta_id"], "dim_cliente": ["cliente_id"]}


def _write(spark: SparkSession, cfg: ProjectConfig, name: str, df: DataFrame) -> int:
    from andes_bank.config import is_databricks

    table = cfg.table("gold", name)
    writer = df.write.mode("overwrite").option("overwriteSchema", "true")
    if is_databricks() and name in CLUSTER:  # pragma: no cover
        writer = writer.clusterBy(*CLUSTER[name])
    writer.saveAsTable(table)
    return spark.table(table).count()


def build_gold(spark: SparkSession, cfg: ProjectConfig, only: list[str] | None = None) -> dict[str, int]:
    """Dimensiones primero (los hechos las usan), después hechos. Reconstrucción completa: gold es derivado."""
    out = {}
    out["dim_sucursal"] = _write(spark, cfg, "dim_sucursal", dim_sucursal(spark))
    for name, fn in {**DIMENSIONS, **FACTS}.items():
        if only and name not in only:
            continue
        out[name] = _write(spark, cfg, name, fn(spark, cfg))
    return out

