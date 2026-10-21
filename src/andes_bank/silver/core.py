"""SILVER del core bancario: clientes, cuentas y tarjetas con historia (SCD2), movimientos y saldos limpios.

Dos contratos de bronze (antes y después de la migración de mar-2024) → UNA tabla silver por entidad.

Estrategia de carga:
  * Maestros (clientes, cuentas, tarjetas): son chicos (miles de eventos CDC). Se RECONSTRUYE la historia completa
    desde bronze en cada corrida: es determinístico, idempotente y no tiene casos borde de MERGE.
  * Hechos (movimientos, saldos): son grandes. Se procesa sólo lo nuevo de bronze (watermark por `_ingested_at`) y se
    hace MERGE insert-only por clave: la primera copia que llegó gana (DAT-01).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.silver.transforms import (
    ar_number,
    currency_from_code,
    has_pii,
    local_to_utc,
    normalize_name,
    only_digits,
    parse_date,
    parse_local_ts,
    redact_pii,
    title_case,
)
from andes_bank.utils.ids import cbu_valid_col, cuit_valid_col


def _bronze(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame | None:
    t = cfg.table("bronze", name)
    return spark.table(t) if spark.catalog.tableExists(t) else None


def _union(*dfs: DataFrame | None) -> DataFrame:
    dfs = [d for d in dfs if d is not None]
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out


# ------------------------------------------------------------------------------------------------ SCD2
def scd2_from_events(events: DataFrame, key: str, attrs: list[str], ts_col: str = "event_ts_utc",
                     order_cols: tuple[str, ...] = ("_delivery_date", "_ingested_at")) -> DataFrame:
    """Historia tipo 2 a partir de eventos CDC (op I/U/D con la foto completa del registro).

    * Un evento sólo abre una versión nueva si CAMBIÓ algo (hash de los atributos). La recarga completa del core
      nuevo (mar-2024) re-envía todos los clientes como 'I': no genera versiones nuevas.
    * 'D' cierra la versión vigente y abre una con estado BAJA.
    """
    w = Window.partitionBy(key).orderBy(ts_col, *order_cols)
    e = (
        events.withColumn("estado", F.when(F.col("op") == "D", "BAJA").otherwise("ACTIVO"))
        .withColumn("_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(a).cast("string"), F.lit("∅")) for a in attrs + ["estado"]]), 256))
        .withColumn("_prev_hash", F.lag("_hash").over(w))
        .where(F.col("_prev_hash").isNull() | (F.col("_hash") != F.col("_prev_hash")))
    )
    w2 = Window.partitionBy(key).orderBy(ts_col, *order_cols)
    return (
        e.withColumn("valid_from", F.col(ts_col))
        .withColumn("valid_to", F.lead(ts_col).over(w2))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version", F.row_number().over(w2))
        .drop("_hash", "_prev_hash")
    )


# --------------------------------------------------------------------------------------------- clientes
def customer_events(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    legacy = _bronze(spark, cfg, "core_clientes_legacy")
    new = _bronze(spark, cfg, "core_clientes")
    parts = []
    if legacy is not None:
        parts.append(legacy.select(
            F.col("OPERACION").alias("op"), F.col("NRO_CLIENTE").alias("cliente_id"),
            F.col("APELLIDO_NOMBRE").alias("nombre_raw"),
            F.when(F.col("TIPO_PERSONA") == "J", "JURIDICA").otherwise("HUMANA").alias("tipo_persona"),
            F.col("NRO_DOC").alias("dni_raw"), F.col("CUIT_CUIL").alias("cuit_raw"), F.col("FEC_NAC").alias("fecha_nac_raw"),
            F.col("EMAIL").alias("email"), F.col("TELEFONO").alias("telefono"), F.col("LOCALIDAD").alias("localidad"),
            F.col("PROVINCIA").alias("provincia"), F.col("SUCURSAL").alias("sucursal_raw"), F.col("SEGMENTO").alias("segmento"),
            F.col("FEC_ALTA").alias("fecha_alta_raw"), F.col("FEC_NOVEDAD").alias("novedad_raw"),
            F.col("OBSERVACIONES").alias("observaciones_raw"), F.lit("core_legacy").alias("sistema_origen"),
            "_source_file", "_delivery_date", "_ingested_at"))
    if new is not None:
        parts.append(new.select(
            "op", "cliente_id", F.col("nombre_completo").alias("nombre_raw"), "tipo_persona", F.col("dni").alias("dni_raw"),
            F.col("cuit").alias("cuit_raw"), F.col("fecha_nacimiento").alias("fecha_nac_raw"), "email", "telefono",
            "localidad", "provincia", F.col("sucursal_id").alias("sucursal_raw"), "segmento",
            F.col("fecha_alta").alias("fecha_alta_raw"), F.col("fecha_novedad").alias("novedad_raw"),
            F.col("observaciones").alias("observaciones_raw"), F.lit("core").alias("sistema_origen"),
            "_source_file", "_delivery_date", "_ingested_at"))
    df = _union(*parts)
    return (
        df.withColumn("nombre_completo", title_case(F.col("nombre_raw")))
        .withColumn("nombre_normalizado", normalize_name(F.col("nombre_raw")))
        .withColumn("dni", F.when(F.col("tipo_persona") == "HUMANA", only_digits(F.col("dni_raw")).cast("bigint")))
        .withColumn("cuit", only_digits(F.col("cuit_raw")))
        .withColumn("cuit_valido", cuit_valid_col(F.col("cuit")))
        .withColumn("fecha_nacimiento", parse_date(F.col("fecha_nac_raw")))
        .withColumn("fecha_alta", parse_date(F.col("fecha_alta_raw")))
        .withColumn("sucursal_id", F.col("sucursal_raw").cast("int"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("event_ts_utc", local_to_utc(parse_local_ts(F.col("novedad_raw"))))
        .withColumn("observaciones_con_pii", has_pii(F.col("observaciones_raw")))
        .withColumn("observaciones", redact_pii(F.col("observaciones_raw")))
    )


CUSTOMER_ATTRS = ["nombre_completo", "tipo_persona", "dni", "cuit", "fecha_nacimiento", "email", "telefono", "localidad",
                  "provincia", "sucursal_id", "segmento", "fecha_alta"]


# SCD mixto: el nombre que se muestra es tipo 1 (siempre el último que mandó el core: el sistema viejo lo mandaba sin
# acentos y el nuevo con acentos, y eso NO es un cambio de negocio). Para detectar cambios se usa el nombre normalizado.
CUSTOMER_SCD2_ATTRS = ["nombre_normalizado"] + [a for a in CUSTOMER_ATTRS if a != "nombre_completo"]


def customers_history(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    events = customer_events(spark, cfg)
    h = scd2_from_events(events, "cliente_id", CUSTOMER_SCD2_ATTRS)
    w = Window.partitionBy("cliente_id").orderBy(F.desc("event_ts_utc"), F.desc("_delivery_date"), F.desc("_ingested_at"))
    latest_name = (events.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
                   .select("cliente_id", F.col("nombre_completo").alias("_nombre_actual")))
    h = h.join(latest_name, "cliente_id").withColumn("nombre_completo", F.col("_nombre_actual")).drop("_nombre_actual")
    return h.select("cliente_id", *CUSTOMER_ATTRS, "nombre_normalizado", "cuit_valido", "observaciones",
                    "observaciones_con_pii", "estado", "valid_from", "valid_to", "is_current", "version",
                    "sistema_origen", "_source_file")


def customer_xref(history: DataFrame) -> DataFrame:
    """Resolución de identidad (DAT-08): la misma persona con dos números de cliente (fusión Cuyo/NOA 2008).

    Regla: mismo DNI y mismo nombre normalizado → misma persona. La persona canónica es el número de cliente más
    viejo (el que tiene fecha de alta menor). Sólo personas humanas (las jurídicas se identifican por CUIT).
    """
    cur = history.where("is_current AND tipo_persona = 'HUMANA' AND dni IS NOT NULL") \
        .select("cliente_id", "dni", "nombre_normalizado", "fecha_alta")
    w = Window.partitionBy("dni", "nombre_normalizado").orderBy("fecha_alta", "cliente_id")
    return (
        cur.withColumn("persona_id", F.first("cliente_id").over(w))
        .withColumn("clientes_de_la_persona", F.count("*").over(Window.partitionBy("dni", "nombre_normalizado")))
        .withColumn("regla", F.when(F.col("clientes_de_la_persona") > 1, "DNI+NOMBRE").otherwise("UNICO"))
        .select("cliente_id", "persona_id", "dni", "clientes_de_la_persona", "regla")
    )


# ---------------------------------------------------------------------------------------------- cuentas
def account_events(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    legacy = _bronze(spark, cfg, "core_cuentas_legacy")
    new = _bronze(spark, cfg, "core_cuentas")
    parts = []
    if legacy is not None:
        parts.append(legacy.select(
            F.col("OPERACION").alias("op"), F.col("NRO_CUENTA").alias("cuenta_id"), F.col("NRO_CLIENTE").alias("cliente_id"),
            F.col("TIPO_CUENTA").alias("tipo_cuenta"), currency_from_code(F.col("COD_MONEDA")).alias("moneda"),
            F.col("CBU").alias("cbu"), F.col("SUCURSAL").cast("int").alias("sucursal_id"),
            parse_date(F.col("FEC_APERTURA")).alias("fecha_apertura"), parse_date(F.col("FEC_CIERRE")).alias("fecha_cierre"),
            ar_number(F.col("LIMITE_DESCUBIERTO")).alias("limite_descubierto"), F.lit("core_legacy").alias("sistema_origen"),
            "_source_file", "_delivery_date", "_ingested_at"))
    if new is not None:
        parts.append(new.select(
            "op", "cuenta_id", "cliente_id", "tipo_cuenta", "moneda", "cbu", F.col("sucursal_id").cast("int").alias("sucursal_id"),
            parse_date(F.col("fecha_apertura")).alias("fecha_apertura"), parse_date(F.col("fecha_cierre")).alias("fecha_cierre"),
            ar_number(F.col("limite_descubierto")).alias("limite_descubierto"), F.lit("core").alias("sistema_origen"),
            "_source_file", "_delivery_date", "_ingested_at"))
    # las cuentas no traen hora de novedad: el evento ocurre al inicio del día de entrega (hora local)
    return (_union(*parts)
            .withColumn("event_ts_utc", local_to_utc(F.to_timestamp("_delivery_date")))
            .withColumn("cbu_valido", cbu_valid_col(F.col("cbu"))))


ACCOUNT_ATTRS = ["cliente_id", "tipo_cuenta", "moneda", "cbu", "sucursal_id", "fecha_apertura", "fecha_cierre",
                 "limite_descubierto"]


def accounts_history(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    h = scd2_from_events(account_events(spark, cfg), "cuenta_id", ACCOUNT_ATTRS)
    return h.select("cuenta_id", *ACCOUNT_ATTRS, "cbu_valido", "estado", "valid_from", "valid_to", "is_current", "version",
                    "sistema_origen", "_source_file")


# --------------------------------------------------------------------------------------------- tarjetas
def card_events(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    legacy = _bronze(spark, cfg, "core_tarjetas_legacy")
    new = _bronze(spark, cfg, "core_tarjetas")
    parts = []
    if legacy is not None:
        parts.append(legacy.select(
            F.col("OPERACION").alias("op"), F.col("TOKEN_TARJETA").alias("token_tarjeta"), F.col("NRO_CLIENTE").alias("cliente_id"),
            F.col("NRO_CUENTA").alias("cuenta_id"), F.col("TIPO").alias("tipo"), F.col("MARCA").alias("marca"),
            F.col("ULTIMOS4").alias("ultimos4"), parse_date(F.col("FEC_EMISION")).alias("fecha_emision"),
            ar_number(F.col("LIMITE")).alias("limite"), "_source_file", "_delivery_date", "_ingested_at"))
    if new is not None:
        parts.append(new.select(
            "op", "token_tarjeta", "cliente_id", "cuenta_id", "tipo", "marca", "ultimos4",
            parse_date(F.col("fecha_emision")).alias("fecha_emision"), ar_number(F.col("limite")).alias("limite"),
            "_source_file", "_delivery_date", "_ingested_at"))
    return _union(*parts).withColumn("event_ts_utc", local_to_utc(F.to_timestamp("_delivery_date")))


CARD_ATTRS = ["cliente_id", "cuenta_id", "tipo", "marca", "ultimos4", "fecha_emision", "limite"]


def cards_history(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    h = scd2_from_events(card_events(spark, cfg), "token_tarjeta", CARD_ATTRS)
    return h.select("token_tarjeta", *CARD_ATTRS, "estado", "valid_from", "valid_to", "is_current", "version")


# ------------------------------------------------------------------------------------------ movimientos
def transactions_from_bronze(legacy: DataFrame | None, new: DataFrame | None) -> DataFrame:
    """Unifica los dos contratos de movimientos. Importe con signo (+ crédito, - débito), en su moneda."""
    parts = []
    if legacy is not None:
        parts.append(legacy.select(
            F.col("NRO_MOVIMIENTO").alias("movimiento_id"), F.col("NRO_CUENTA").alias("cuenta_id"),
            F.col("FECHA_HORA").alias("fecha_hora_raw"), F.col("FECHA_CONTABLE").alias("fecha_contable_raw"),
            F.col("CONCEPTO").alias("concepto"),
            F.when(F.col("DEB_CRED") == "D", -ar_number(F.col("IMPORTE"))).otherwise(ar_number(F.col("IMPORTE"))).alias("importe"),
            currency_from_code(F.col("COD_MONEDA")).alias("moneda_informada"), ar_number(F.col("SALDO")).alias("saldo"),
            F.col("CANAL").alias("canal"), F.col("REFERENCIA").alias("referencia"), F.col("ARCHIVO").alias("archivo"),
            F.lit(None).cast("string").alias("canal_digital"), F.lit("core_legacy").alias("sistema_origen"),
            "_source_file", "_delivery_date", "_ingested_at", F.col("IMPORTE").alias("importe_raw")))
    if new is not None:
        parts.append(new.select(
            "movimiento_id", "cuenta_id", F.col("fecha_hora").alias("fecha_hora_raw"),
            F.col("fecha_contable").alias("fecha_contable_raw"), "concepto", ar_number(F.col("importe")).alias("importe"),
            F.col("moneda").alias("moneda_informada"), ar_number(F.col("saldo")).alias("saldo"), "canal", "referencia",
            "archivo", F.col("canal_digital") if "canal_digital" in new.columns else F.lit(None).cast("string").alias("canal_digital"),
            F.lit("core").alias("sistema_origen"), "_source_file", "_delivery_date", "_ingested_at",
            F.col("importe").alias("importe_raw")))
    df = _union(*parts)
    local = parse_local_ts(F.col("fecha_hora_raw"))
    return (
        df.withColumn("fecha_hora_local", local)
        .withColumn("fecha_hora_utc", local_to_utc(local))
        .withColumn("fecha_contable", parse_date(F.col("fecha_contable_raw")))
        .withColumn("canal_digital", F.when(F.col("canal_digital").isNotNull(), F.col("canal_digital") == "S")
                    .otherwise(F.col("canal").isin("APP", "HOMEBANKING")))
    )


def dedupe_first_arrival(df: DataFrame, key: str) -> DataFrame:
    """Gana la primera copia que llegó (DAT-01): menor fecha de entrega, después menor momento de ingesta."""
    w = Window.partitionBy(key).orderBy("_delivery_date", "_ingested_at", "_source_file")
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def fix_currency(txn: DataFrame, accounts_current: DataFrame) -> DataFrame:
    """DAT-05: la moneda del movimiento es la de la cuenta. Si el core informó otra, se corrige y se marca."""
    acc = accounts_current.select("cuenta_id", F.col("moneda").alias("moneda_cuenta"))
    return (
        txn.join(acc, "cuenta_id", "left")
        .withColumn("moneda", F.coalesce("moneda_cuenta", "moneda_informada"))
        .withColumn("moneda_corregida", F.col("moneda_cuenta").isNotNull() & (F.col("moneda_cuenta") != F.col("moneda_informada")))
        .withColumn("cuenta_conocida", F.col("moneda_cuenta").isNotNull())
        .drop("moneda_cuenta")
    )


TXN_COLUMNS = ["movimiento_id", "cuenta_id", "fecha_hora_local", "fecha_hora_utc", "fecha_contable", "concepto", "importe",
               "moneda", "moneda_informada", "moneda_corregida", "saldo", "canal", "canal_digital", "referencia", "archivo",
               "sistema_origen", "_source_file", "_delivery_date", "_ingested_at"]


# ----------------------------------------------------------------------------------------------- saldos
def balances_from_bronze(legacy: DataFrame | None, new: DataFrame | None) -> DataFrame:
    parts = []
    if legacy is not None:
        parts.append(legacy.select(F.col("NRO_CUENTA").alias("cuenta_id"), parse_date(F.col("FECHA_SALDO")).alias("fecha_saldo"),
                                   ar_number(F.col("SALDO_CIERRE")).alias("saldo_cierre"),
                                   currency_from_code(F.col("COD_MONEDA")).alias("moneda"),
                                   "_source_file", "_delivery_date", "_ingested_at"))
    if new is not None:
        parts.append(new.select("cuenta_id", parse_date(F.col("fecha_saldo")).alias("fecha_saldo"),
                                ar_number(F.col("saldo_cierre")).alias("saldo_cierre"), "moneda",
                                "_source_file", "_delivery_date", "_ingested_at"))
    return _union(*parts).withColumn("balance_key", F.concat_ws("|", "cuenta_id", F.col("fecha_saldo").cast("string")))


def balance_breaks(txn: DataFrame) -> DataFrame:
    """DAT-10: movimientos faltantes. Cada movimiento trae el saldo posterior; si saldo_anterior + importe ≠ saldo,
    entre esos dos movimientos falta al menos uno (el core no lo mandó)."""
    w = Window.partitionBy("cuenta_id").orderBy("fecha_hora_utc", "movimiento_id")
    return (
        txn.select("cuenta_id", "movimiento_id", "fecha_hora_utc", "fecha_contable", "importe", "saldo", "moneda")
        .withColumn("saldo_anterior", F.lag("saldo").over(w))
        .withColumn("movimiento_anterior", F.lag("movimiento_id").over(w))
        .withColumn("diferencia", F.col("saldo") - (F.col("saldo_anterior") + F.col("importe")))
        .where(F.col("saldo_anterior").isNotNull() & (F.abs("diferencia") > 0.01))
        .withColumn("causa_probable", F.lit("MOVIMIENTO_FALTANTE"))
    )
