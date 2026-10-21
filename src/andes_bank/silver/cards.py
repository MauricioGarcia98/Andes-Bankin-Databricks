"""SILVER del procesador de tarjetas y de las cotizaciones.

Autorizaciones y liquidaciones llegan por DOS caminos (API-09):
  * la integración vieja (v1): un JSON por día, montos en centavos, epoch ms, estado A/R, código de 2 dígitos;
  * la API v2: JSON canónico (montos decimales, ISO-8601, códigos completos).
Entre abr y jun de 2025 llegan por los dos: el mismo `auth_id` dos veces, con formatos distintos. Gana la v2.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.silver.transforms import response_code_v1

AUTH_V2_SCHEMA = T.StructType([
    T.StructField("auth_id", T.StringType()), T.StructField("card_token", T.StringType()),
    T.StructField("pan_last4", T.StringType()), T.StructField("brand", T.StringType()),
    T.StructField("card_type", T.StringType()), T.StructField("merchant_id", T.StringType()),
    T.StructField("merchant_name", T.StringType()), T.StructField("mcc", T.StringType()),
    T.StructField("merchant_country", T.StringType()), T.StructField("merchant_city", T.StringType()),
    T.StructField("amount", T.DecimalType(18, 2)), T.StructField("currency", T.StringType()),
    T.StructField("billing_amount_ars", T.DecimalType(18, 2)), T.StructField("authorized_at", T.StringType()),
    T.StructField("processing_date", T.StringType()), T.StructField("status", T.StringType()),
    T.StructField("response_code", T.StringType()), T.StructField("entry_mode", T.StringType()),
    T.StructField("wallet_type", T.StringType()),          # aparece en mar-2026 (schema drift)
])
SETTLEMENT_V2_SCHEMA = T.StructType([
    T.StructField("settlement_id", T.StringType()), T.StructField("auth_id", T.StringType()),
    T.StructField("card_token", T.StringType()), T.StructField("merchant_id", T.StringType()),
    T.StructField("settlement_date", T.StringType()), T.StructField("settled_amount", T.DecimalType(18, 2)),
    T.StructField("currency", T.StringType()), T.StructField("settled_billing_ars", T.DecimalType(18, 2)),
    T.StructField("batch_id", T.StringType()), T.StructField("brand", T.StringType()),
])

AUTH_COLUMNS = ["auth_id", "token_tarjeta", "ultimos4", "marca", "tipo_tarjeta", "comercio_id", "comercio_nombre", "mcc",
                "pais_comercio", "ciudad_comercio", "importe", "moneda", "importe_ars", "autorizada_utc", "fecha_proceso",
                "estado", "codigo_respuesta", "modo_ingreso", "billetera", "version_api", "_ingested_at"]


def auth_from_v2(bronze: DataFrame) -> DataFrame:
    a = bronze.withColumn("j", F.from_json("raw", AUTH_V2_SCHEMA)).select("j.*", "_ingested_at")
    return a.select(
        "auth_id", F.col("card_token").alias("token_tarjeta"), F.col("pan_last4").alias("ultimos4"),
        F.col("brand").alias("marca"), F.col("card_type").alias("tipo_tarjeta"), F.col("merchant_id").alias("comercio_id"),
        F.col("merchant_name").alias("comercio_nombre"), "mcc", F.col("merchant_country").alias("pais_comercio"),
        F.col("merchant_city").alias("ciudad_comercio"), F.col("amount").alias("importe"), F.col("currency").alias("moneda"),
        F.col("billing_amount_ars").alias("importe_ars"),
        F.to_timestamp("authorized_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("autorizada_utc"),
        F.to_date("processing_date").alias("fecha_proceso"), F.col("status").alias("estado"),
        F.col("response_code").alias("codigo_respuesta"), F.col("entry_mode").alias("modo_ingreso"),
        F.col("wallet_type").alias("billetera"), F.lit("v2").alias("version_api"), "_ingested_at",
    )


def auth_from_v1(bronze: DataFrame) -> DataFrame:
    """v1 → modelo canónico: centavos / 100, epoch ms → timestamp UTC, A/R → APPROVED/DECLINED."""
    return bronze.select(
        F.col("id_autorizacion").alias("auth_id"), F.col("token_tarjeta"), F.col("ultimos4"), F.col("marca"),
        F.col("tipo_tarjeta"), F.col("comercio_id"), F.col("comercio_nombre"), F.col("rubro_mcc").alias("mcc"),
        F.col("pais").alias("pais_comercio"), F.col("ciudad").alias("ciudad_comercio"),
        (F.col("monto_centavos").cast("decimal(20,0)") / 100).cast("decimal(18,2)").alias("importe"), F.col("moneda"),
        (F.col("monto_pesos_centavos").cast("decimal(20,0)") / 100).cast("decimal(18,2)").alias("importe_ars"),
        F.timestamp_millis(F.col("fecha_hora_ms").cast("long")).alias("autorizada_utc"),
        F.col("_delivery_date").alias("fecha_proceso"),
        F.when(F.col("estado") == "A", "APPROVED").when(F.col("estado") == "R", "DECLINED").alias("estado"),
        response_code_v1(F.col("codigo_respuesta")).alias("codigo_respuesta"), F.col("modo_ingreso"),
        F.lit(None).cast("string").alias("billetera"), F.lit("v1").alias("version_api"), "_ingested_at",
    )


def prefer_v2(df: DataFrame, key: str) -> DataFrame:
    """Mismo id por los dos caminos: gana v2; entre iguales, la primera que llegó."""
    w = Window.partitionBy(key).orderBy(F.when(F.col("version_api") == "v2", 0).otherwise(1), "_ingested_at")
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


SETTLEMENT_COLUMNS = ["liquidacion_id", "auth_id", "token_tarjeta", "comercio_id", "fecha_liquidacion", "importe", "moneda",
                      "importe_ars", "lote", "version_api", "_ingested_at"]


def settlements_from_v2(bronze: DataFrame) -> DataFrame:
    s = bronze.withColumn("j", F.from_json("raw", SETTLEMENT_V2_SCHEMA)).select("j.*", "_ingested_at")
    return s.select(
        F.col("settlement_id").alias("liquidacion_id"), "auth_id", F.col("card_token").alias("token_tarjeta"),
        F.col("merchant_id").alias("comercio_id"), F.to_date("settlement_date").alias("fecha_liquidacion"),
        F.col("settled_amount").alias("importe"), F.col("currency").alias("moneda"),
        F.col("settled_billing_ars").alias("importe_ars"), F.col("batch_id").alias("lote"), F.lit("v2").alias("version_api"),
        "_ingested_at")


def settlements_from_v1(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("id_liquidacion").alias("liquidacion_id"), F.col("id_autorizacion").alias("auth_id"), "token_tarjeta",
        "comercio_id", F.to_date(F.col("fecha_liquidacion").cast("string"), "yyyyMMdd").alias("fecha_liquidacion"),
        (F.col("monto_centavos").cast("decimal(20,0)") / 100).cast("decimal(18,2)").alias("importe"), "moneda",
        (F.col("monto_pesos_centavos").cast("decimal(20,0)") / 100).cast("decimal(18,2)").alias("importe_ars"),
        F.col("lote"), F.lit("v1").alias("version_api"), "_ingested_at")


# ------------------------------------------------------------------------------------------------- FX
FX_DETAIL_SCHEMA = T.StructType([
    T.StructField("codigoMoneda", T.StringType()), T.StructField("descripcion", T.StringType()),
    T.StructField("tipoPase", T.StringType()), T.StructField("tipoCotizacion", T.StringType()),
])
CURRENCY_ALIASES = {"U$S": "USD", "DOL": "USD", "U$D": "USD"}


def fx_rates(bcra: DataFrame | None, mirror: DataFrame | None) -> DataFrame:
    """Cotización oficial por día y moneda. Fuente principal: BCRA real; respaldo: el espejo (con su suciedad).

    El espejo manda a veces el valor como texto con coma ('960,5000') y el código 'U$S' en vez de 'USD'.
    """
    alias = F.create_map(*[x for k, v in CURRENCY_ALIASES.items() for x in (F.lit(k), F.lit(v))])

    def parse(df: DataFrame, source: str, priority: int) -> DataFrame:
        d = df.withColumn("j", F.from_json("raw", FX_DETAIL_SCHEMA))
        code = F.coalesce(alias[F.col("j.codigoMoneda")], F.col("j.codigoMoneda"))
        return d.select(
            F.to_date("fecha").alias("fecha"), F.col("moneda").alias("moneda_pedida"), code.alias("moneda"),
            F.regexp_replace(F.col("j.tipoCotizacion"), ",", ".").cast("decimal(18,6)").alias("ars_por_unidad"),
            F.lit(source).alias("fx_fuente"), F.lit(priority).alias("_prio"))

    parts = [p for p in (parse(bcra, "BCRA", 1) if bcra is not None else None,
                         parse(mirror, "ESPEJO", 2) if mirror is not None else None) if p is not None]
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    w = Window.partitionBy("fecha", "moneda").orderBy("_prio")
    return (df.where(F.col("fecha").isNotNull() & (F.col("ars_por_unidad") > 0) & (F.col("moneda") == F.col("moneda_pedida")))
            .withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_prio", "moneda_pedida"))
