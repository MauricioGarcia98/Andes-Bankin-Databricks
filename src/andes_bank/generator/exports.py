"""Lo que cada sistema origen ENTREGA, con la suciedad del catálogo (docs/03).

La verdad (`sim.*`) es limpia. Acá se ensucia exactamente como lo haría cada sistema real:

  core bancario   → CSV diarios: CDC de clientes y cuentas, movimientos y saldos.
                    Antes de la migración (mar-2024): latin1, ';', dd/MM/yyyy, '1.234,56', columnas en mayúscula.
                    Después: UTF-8, ',', ISO, gzip, snake_case. Y desde 2025 una columna nueva (canal_digital).
  procesador      → integración vieja (v1): un JSON por día con montos en centavos y epoch ms (hasta jun-2025).
                    API v2: los registros canónicos que sirve la Azure Function (desde abr-2025).

Todo se escribe en el Volume (`_staging/`) con una carpeta por fecha de entrega; `upload.py` lo sube a Azure con el
nombre de archivo que usaría cada sistema.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.generator import reference as ref
from andes_bank.generator.rand import uniform

TZ = "America/Argentina/Buenos_Aires"

# Incidentes de entrega puntuales (catálogo C). Las fechas son de entrega (carpeta).
LATE_FILE = (date(2025, 8, 12), date(2025, 8, 15))       # ING-04/06: el archivo del 12 llega el 15
EMPTY_FILE = date(2025, 5, 2)                             # ING-02: llega vacío; al día siguiente, el reenvío
CORRUPT_FILE = date(2025, 11, 3)                          # ING-03: llega truncado (lo hace upload.py); reenvío al día siguiente
DUPLICATE_FILE = date(2026, 2, 10)                        # ING-01: el mismo archivo se sube dos veces (upload.py)
DUPLICATE_ROW_SHARE = 0.003                               # DAT-01
MISSING_ROW_SHARE = 0.0002                                # DAT-10
WRONG_CURRENCY_SHARE = 0.002                              # DAT-05 (sobre movimientos de cuentas en USD)
INVALID_CUIT_SHARE = 0.005                                # DAT-07
SHORT_CBU_SHARE = 0.002                                   # DAT-07
PII_NOTE_SHARE = 0.03                                     # DAT-11


def _d(value: date) -> Column:
    return F.lit(value.isoformat()).cast("date")


def _only_day(df: DataFrame, col: str, day: date | None) -> DataFrame:
    """Con `day` (el job "día nuevo"), sólo lo que se entrega ese día; sin `day`, toda la historia."""
    return df if day is None else df.where(F.col(col) == _d(day))


def _delivered(df: DataFrame, cfg: ProjectConfig, day: date | None) -> DataFrame:
    """Lo entregado hasta `end_date` (y sólo `day` si viene). Sin el tope, la historia entregaba días que todavía no
    habían terminado (compras informadas tarde, saldos de movimientos de después de las 21 h) y el día nuevo los
    encontraba ya subidos, a medias (bitácora R20)."""
    return _only_day(df.where(F.col("delivery_date") <= _d(cfg.end_date)), "delivery_date", day)


def _ar_number(col: Column) -> Column:
    """1234.5 → '1.234,50' (formato argentino del core viejo)."""
    return F.translate(F.format_number(col, 2), ",.", ".,")


def _local(ts_utc: Column) -> Column:
    return F.from_utc_timestamp(ts_utc, TZ)


def _write_csv(df: DataFrame, path: str, legacy: bool) -> None:
    """Un archivo por fecha de entrega. Legacy: latin1, ';' y sin comprimir; nuevo: UTF-8, ',' y gzip.

    Overwrite *dinámico*: sólo se reemplazan las carpetas de fecha presentes en `df` (regenerar es idempotente).
    """
    if "file_date" not in df.columns:
        df = df.withColumn("file_date", F.col("delivery_date"))
    # carpeta = fecha de entrega; subcarpeta = fecha del archivo (casi siempre la misma; distinta si llegó tarde)
    writer = (
        df.repartition("delivery_date", "file_date").write.mode("overwrite").partitionBy("delivery_date", "file_date")
        .option("header", "true").option("partitionOverwriteMode", "dynamic")
    )
    if legacy:
        writer = writer.option("sep", ";").option("encoding", "ISO-8859-1")
    else:
        writer = writer.option("sep", ",").option("compression", "gzip")
    writer.csv(path)


def _split_write(df: DataFrame, path: str, legacy_cols: list[Column], new_cols: list[Column],
                 drift_cols: list[Column] | None = None) -> None:
    pre = df.where(F.col("delivery_date") < _d(ref.CORE_MIGRATION_DATE))
    post = df.where(F.col("delivery_date") >= _d(ref.CORE_MIGRATION_DATE))
    keep = ["delivery_date"] + (["file_date"] if "file_date" in df.columns else [])
    _write_csv(pre.select(*legacy_cols, *keep), path, legacy=True)
    if drift_cols:
        before = post.where(F.col("delivery_date") < _d(ref.CANAL_DIGITAL_SINCE))
        after = post.where(F.col("delivery_date") >= _d(ref.CANAL_DIGITAL_SINCE))
        _write_csv(before.select(*new_cols, *keep), path, legacy=False)
        _write_csv(after.select(*new_cols, *drift_cols, *keep), path, legacy=False)
    else:
        _write_csv(post.select(*new_cols, *keep), path, legacy=False)


# ------------------------------------------------------------------------------------- core: clientes
def customer_cdc(customers: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Eventos CDC de clientes (op I/U/D) con la foto completa del registro en cada evento."""
    s = str(cfg.seed)
    start, mig = _d(cfg.start_date), _d(ref.CORE_MIGRATION_DATE)
    c = customers.withColumn("u_cuit", uniform("customer_key", salt=f"{s}-x-cuit"))
    alive_at = lambda d: (F.col("customer_since") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d))  # noqa: E731
    seg_at = lambda d: F.when(F.col("segment_change_date").isNotNull() & (F.col("segment_change_date") <= d),  # noqa: E731
                              F.col("new_segment")).otherwise(F.col("segment"))
    mail_at = lambda d: F.when(F.col("email_change_date").isNotNull() & (F.col("email_change_date") <= d),  # noqa: E731
                               F.col("new_email")).otherwise(F.col("email"))
    events = [
        # carga inicial (foto de todos los clientes vivos al empezar el período)
        c.where(alive_at(start)).withColumn("delivery_date", start).withColumn("op", F.lit("I")),
        # altas del período
        c.where(F.col("customer_since") > start).withColumn("delivery_date", F.col("customer_since")).withColumn("op", F.lit("I")),
        # cambios de segmento y de email
        c.where(F.col("segment_change_date").isNotNull() & alive_at(F.col("segment_change_date")))
        .withColumn("delivery_date", F.col("segment_change_date")).withColumn("op", F.lit("U")),
        c.where(F.col("email_change_date").isNotNull() & alive_at(F.col("email_change_date")))
        .withColumn("delivery_date", F.col("email_change_date")).withColumn("op", F.lit("U")),
        # bajas
        c.where(F.col("closed_date").isNotNull() & (F.col("closed_date") > start))
        .withColumn("delivery_date", F.col("closed_date")).withColumn("op", F.lit("D")),
        # el core nuevo arranca con una carga completa: todos los vivos otra vez como 'I' (el lakehouse no tiene
        # que duplicarlos)
        c.where(alive_at(mig)).withColumn("delivery_date", mig).withColumn("op", F.lit("I")),
    ]
    df = events[0]
    for e in events[1:]:
        df = df.unionByName(e)
    df = (
        df.where(F.col("delivery_date") <= _d(cfg.end_date))
        .withColumn("segment_now", seg_at(F.col("delivery_date")))
        .withColumn("email_now", mail_at(F.col("delivery_date")))
        # DAT-07: un CUIT mal cargado desde siempre (dígito verificador cambiado)
        .withColumn("cuit_out", F.when(F.col("u_cuit") < INVALID_CUIT_SHARE,
                                       F.concat(F.substring("cuit", 1, 10), ((F.substring("cuit", 11, 1).cast("int") + 3) % 10).cast("string")))
                    .otherwise(F.col("cuit")))
        # DAT-11: datos personales escritos a mano en observaciones
        .withColumn("observaciones", F.when(uniform("customer_key", salt=f"{s}-x-pii") < PII_NOTE_SHARE,
                                            F.concat(F.lit("Contactar al "), F.col("phone"), F.lit(" o a "), F.col("email_now"),
                                                     F.lit(" por actualización de datos")))
                    .when(uniform("customer_key", salt=f"{s}-x-obs") < 0.05, F.lit("Cliente con firma registrada en sucursal")))
        .withColumn("event_ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("delivery_date")) + 3 * 3600
                                                        + (F.col("customer_key") % 36_000)))
    )
    return df


def write_customers(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> str:
    c = _only_day(customer_cdc(spark.table(cfg.table("sim", "customers")), cfg), "delivery_date", day)
    path = cfg.staging_path("core/clientes")
    legacy = [
        F.col("op").alias("OPERACION"), F.col("customer_id").alias("NRO_CLIENTE"),
        F.upper(F.translate("full_name", "áéíóúÁÉÍÓÚ", "aeiouAEIOU")).alias("APELLIDO_NOMBRE"),
        F.when(F.col("person_type") == "HUMANA", "F").otherwise("J").alias("TIPO_PERSONA"),
        # DNI con puntos en el sistema viejo de Cuyo
        F.when(F.col("region") == "CUYO", F.translate(F.format_number("dni", 0), ",", ".")).otherwise(F.col("dni").cast("string")).alias("NRO_DOC"),
        F.col("cuit_out").alias("CUIT_CUIL"), F.date_format("birth_date", "dd/MM/yyyy").alias("FEC_NAC"),
        F.col("email_now").alias("EMAIL"), F.col("phone").alias("TELEFONO"), F.col("city").alias("LOCALIDAD"),
        F.col("province").alias("PROVINCIA"), F.lpad(F.col("branch_id").cast("string"), 4, "0").alias("SUCURSAL"),
        F.col("segment_now").alias("SEGMENTO"), F.date_format("customer_since", "dd/MM/yyyy").alias("FEC_ALTA"),
        F.date_format(_local(F.col("event_ts_utc")), "dd/MM/yyyy HH:mm:ss").alias("FEC_NOVEDAD"),
        F.col("observaciones").alias("OBSERVACIONES"),
    ]
    new = [
        F.col("op"), F.col("customer_id").alias("cliente_id"), F.col("full_name").alias("nombre_completo"),
        F.col("person_type").alias("tipo_persona"), F.col("dni").cast("string").alias("dni"), F.col("cuit_out").alias("cuit"),
        F.col("birth_date").cast("string").alias("fecha_nacimiento"), F.col("email_now").alias("email"),
        F.col("phone").alias("telefono"), F.col("city").alias("localidad"), F.col("province").alias("provincia"),
        F.col("branch_id").alias("sucursal_id"), F.col("segment_now").alias("segmento"),
        F.col("customer_since").cast("string").alias("fecha_alta"),
        F.date_format(_local(F.col("event_ts_utc")), "yyyy-MM-dd'T'HH:mm:ss").alias("fecha_novedad"),
        F.col("observaciones"),
    ]
    _split_write(c, path, legacy, new)
    return path


# -------------------------------------------------------------------------------------- core: cuentas
def write_accounts(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> str:
    s = str(cfg.seed)
    a = spark.table(cfg.table("sim", "accounts"))
    start, mig = _d(cfg.start_date), _d(ref.CORE_MIGRATION_DATE)
    alive_at = lambda d: (F.col("opened_date") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d))  # noqa: E731
    df = (
        a.where(alive_at(start)).withColumn("delivery_date", start).withColumn("op", F.lit("I"))
        .unionByName(a.where(F.col("opened_date") > start).withColumn("delivery_date", F.col("opened_date")).withColumn("op", F.lit("I")))
        .unionByName(a.where(F.col("closed_date").isNotNull() & (F.col("closed_date") > start))
                     .withColumn("delivery_date", F.col("closed_date")).withColumn("op", F.lit("D")))
        .unionByName(a.where(alive_at(mig)).withColumn("delivery_date", mig).withColumn("op", F.lit("I")))
        .where(F.col("delivery_date") <= _d(cfg.end_date))
        # DAT-07: CBU de 21 dígitos (se comió un dígito al cargarlo)
        .withColumn("cbu_out", F.when(uniform("account_key", salt=f"{s}-x-cbu") < SHORT_CBU_SHARE, F.substring("cbu", 1, 21))
                    .otherwise(F.col("cbu")))
    )
    df = _only_day(df, "delivery_date", day)
    legacy = [
        F.col("op").alias("OPERACION"), F.col("account_id").alias("NRO_CUENTA"), F.col("customer_id").alias("NRO_CLIENTE"),
        F.col("account_type").alias("TIPO_CUENTA"), F.when(F.col("currency") == "USD", "2").otherwise("1").alias("COD_MONEDA"),
        F.col("cbu_out").alias("CBU"), F.lpad(F.col("branch_id").cast("string"), 4, "0").alias("SUCURSAL"),
        F.date_format("opened_date", "dd/MM/yyyy").alias("FEC_APERTURA"), F.date_format("closed_date", "dd/MM/yyyy").alias("FEC_CIERRE"),
        _ar_number(F.col("overdraft_limit_2023").cast("double")).alias("LIMITE_DESCUBIERTO"),
    ]
    new = [
        F.col("op"), F.col("account_id").alias("cuenta_id"), F.col("customer_id").alias("cliente_id"),
        F.col("account_type").alias("tipo_cuenta"), F.col("currency").alias("moneda"), F.col("cbu_out").alias("cbu"),
        F.col("branch_id").alias("sucursal_id"), F.col("opened_date").cast("string").alias("fecha_apertura"),
        F.col("closed_date").cast("string").alias("fecha_cierre"), F.col("overdraft_limit_2023").cast("decimal(18,2)").alias("limite_descubierto"),
    ]
    path = cfg.staging_path("core/cuentas")
    _split_write(df, path, legacy, new)
    return path


# -------------------------------------------------------------------------------------- core: tarjetas
def write_cards(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> str:
    """Maestro de tarjetas del core: une el token que conoce el procesador con el cliente y la cuenta."""
    c = (spark.table(cfg.table("sim", "cards"))
         .join(spark.table(cfg.table("sim", "customers")).select("customer_key", "customer_id"), "customer_key")
         .join(spark.table(cfg.table("sim", "accounts")).select(F.col("account_key").alias("ca_ars_account_key"),
                                                                 F.col("account_id")), "ca_ars_account_key"))
    start, mig = _d(cfg.start_date), _d(ref.CORE_MIGRATION_DATE)
    alive_at = lambda d: (F.col("issued_date") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d))  # noqa: E731
    df = (
        c.where(alive_at(start)).withColumn("delivery_date", start).withColumn("op", F.lit("I"))
        .unionByName(c.where(F.col("issued_date") > start).withColumn("delivery_date", F.col("issued_date")).withColumn("op", F.lit("I")))
        .unionByName(c.where(F.col("closed_date").isNotNull() & (F.col("closed_date") > start))
                     .withColumn("delivery_date", F.col("closed_date")).withColumn("op", F.lit("D")))
        .unionByName(c.where(alive_at(mig)).withColumn("delivery_date", mig).withColumn("op", F.lit("I")))
        .where(F.col("delivery_date") <= _d(cfg.end_date))
    )
    df = _only_day(df, "delivery_date", day)
    legacy = [
        F.col("op").alias("OPERACION"), F.col("card_token").alias("TOKEN_TARJETA"), F.col("customer_id").alias("NRO_CLIENTE"),
        F.col("account_id").alias("NRO_CUENTA"), F.col("card_type").alias("TIPO"), F.col("brand").alias("MARCA"),
        F.col("pan_last4").alias("ULTIMOS4"), F.date_format("issued_date", "dd/MM/yyyy").alias("FEC_EMISION"),
        _ar_number(F.col("credit_limit_2023").cast("double")).alias("LIMITE"),
    ]
    new = [
        F.col("op"), F.col("card_token").alias("token_tarjeta"), F.col("customer_id").alias("cliente_id"),
        F.col("account_id").alias("cuenta_id"), F.col("card_type").alias("tipo"), F.col("brand").alias("marca"),
        F.col("pan_last4").alias("ultimos4"), F.col("issued_date").cast("string").alias("fecha_emision"),
        F.col("credit_limit_2023").cast("decimal(18,2)").alias("limite"),
    ]
    path = cfg.staging_path("core/tarjetas")
    _split_write(df, path, legacy, new)
    return path


# ---------------------------------------------------------------------------------- core: movimientos
def transaction_export(txn: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Movimientos tal como los entrega el core: con duplicados, faltantes, moneda mal y un día que llega tarde."""
    s = str(cfg.seed)
    u = lambda salt: uniform("txn_id", salt=f"{s}-x-{salt}")  # noqa: E731
    base = (
        txn.where(u("miss") >= MISSING_ROW_SHARE)                                     # DAT-10
        .withColumn("delivery_date", F.col("posting_date"))
        .withColumn("currency_out", F.when((F.col("currency") == "USD") & (u("cur") < WRONG_CURRENCY_SHARE), "ARS")
                    .otherwise(F.col("currency")))                                    # DAT-05
        .withColumn("file_date", F.col("posting_date"))
    )
    dups = (base.where(u("dup") < DUPLICATE_ROW_SHARE)                               # DAT-01: re-enviados en el archivo
            .withColumn("delivery_date", F.date_add("delivery_date", 1))                # del día siguiente
            .withColumn("file_date", F.col("delivery_date")))
    df = base.unionByName(dups).where(F.col("delivery_date") <= _d(cfg.end_date))
    late_from, late_to = LATE_FILE
    df = (
        df.withColumn("delivery_date", F.when(F.col("delivery_date") == _d(late_from), _d(late_to))
                      .otherwise(F.col("delivery_date")))                             # ING-04/06
        # ING-02: el archivo del 2-may llega vacío; los datos vienen en el reenvío del día siguiente
        .withColumn("delivery_date", F.when(F.col("delivery_date") == _d(EMPTY_FILE), F.date_add(_d(EMPTY_FILE), 1))
                    .otherwise(F.col("delivery_date")))
        .withColumn("canal_digital", F.when(F.col("channel").isin("APP", "HOMEBANKING"), "S").otherwise("N"))
    )
    return df


def write_transactions(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> str:
    txn = spark.table(cfg.table("sim", "transactions"))
    if day is not None:   # lo del día y lo del anterior (los re-envíos llegan al día siguiente: DAT-01)
        txn = txn.where(F.col("posting_date").between(_d(day - timedelta(days=1)), _d(day)))
    df = _only_day(transaction_export(txn, cfg), "delivery_date", day)
    legacy = [
        F.col("txn_id").alias("NRO_MOVIMIENTO"), F.col("account_id").alias("NRO_CUENTA"),
        F.date_format(_local(F.col("ts_utc")), "dd/MM/yyyy HH:mm:ss").alias("FECHA_HORA"),
        F.date_format("posting_date", "dd/MM/yyyy").alias("FECHA_CONTABLE"), F.col("txn_type").alias("CONCEPTO"),
        F.when(F.col("signed_amount") >= 0, "C").otherwise("D").alias("DEB_CRED"),
        _ar_number(F.col("amount")).alias("IMPORTE"), F.when(F.col("currency_out") == "USD", "2").otherwise("1").alias("COD_MONEDA"),
        _ar_number(F.col("balance_after")).alias("SALDO"), F.col("channel").alias("CANAL"), F.col("reference").alias("REFERENCIA"),
        F.date_format("file_date", "yyyyMMdd").alias("ARCHIVO"),
    ]
    new = [
        F.col("txn_id").alias("movimiento_id"), F.col("account_id").alias("cuenta_id"),
        F.date_format(_local(F.col("ts_utc")), "yyyy-MM-dd HH:mm:ss").alias("fecha_hora"),
        F.col("posting_date").cast("string").alias("fecha_contable"), F.col("txn_type").alias("concepto"),
        F.col("signed_amount").cast("decimal(18,2)").alias("importe"), F.col("currency_out").alias("moneda"),
        F.col("balance_after").cast("decimal(18,2)").alias("saldo"),
        F.col("channel").alias("canal"), F.col("reference").alias("referencia"),
        F.date_format("file_date", "yyyyMMdd").alias("archivo"),
    ]
    path = cfg.staging_path("core/movimientos")
    _split_write(df, path, legacy, new, drift_cols=[F.col("canal_digital")])
    return path


def write_balances(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> str:
    b = _delivered(spark.table(cfg.table("sim", "balances")).withColumn("delivery_date", F.col("balance_date")), cfg, day)
    legacy = [
        F.col("account_id").alias("NRO_CUENTA"), F.date_format("balance_date", "dd/MM/yyyy").alias("FECHA_SALDO"),
        _ar_number(F.col("closing_balance")).alias("SALDO_CIERRE"),
        F.when(F.col("currency") == "USD", "2").otherwise("1").alias("COD_MONEDA"),
    ]
    new = [
        F.col("account_id").alias("cuenta_id"), F.col("balance_date").cast("string").alias("fecha_saldo"),
        F.col("closing_balance").cast("decimal(18,2)").alias("saldo_cierre"), F.col("currency").alias("moneda"),
    ]
    path = cfg.staging_path("core/saldos")
    _split_write(b, path, legacy, new)
    return path


# ----------------------------------------------------------------------------------------- procesador
def write_cards_legacy(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> dict[str, str]:
    """Integración vieja con el procesador (v1): un JSON por día de proceso, montos en centavos, epoch ms."""
    a = spark.table(cfg.table("sim", "authorizations")).where(F.col("processing_date") <= _d(ref.API_V1_LAST_EXPORT))
    auth = a.select(
        F.col("auth_id").alias("id_autorizacion"), F.col("card_token").alias("token_tarjeta"), F.col("pan_last4").alias("ultimos4"),
        F.col("brand").alias("marca"), F.col("card_type").alias("tipo_tarjeta"), F.col("merchant_id").alias("comercio_id"),
        F.col("merchant_name").alias("comercio_nombre"), F.col("mcc").alias("rubro_mcc"), F.col("merchant_country").alias("pais"),
        F.col("merchant_city").alias("ciudad"), F.round(F.col("amount") * 100).cast("long").alias("monto_centavos"),
        F.col("currency").alias("moneda"), F.round(F.col("billing_amount_ars") * 100).cast("long").alias("monto_pesos_centavos"),
        (F.unix_timestamp("auth_ts") * 1000).alias("fecha_hora_ms"),
        F.when(F.col("status") == "APPROVED", "A").otherwise("R").alias("estado"),
        F.substring("response_code", 1, 2).alias("codigo_respuesta"), F.col("entry_mode").alias("modo_ingreso"),
        F.col("processing_date").alias("delivery_date"),
    )
    s = spark.table(cfg.table("sim", "settlements")).where(F.col("settlement_date") <= _d(ref.API_V1_LAST_EXPORT))
    sett = s.select(
        F.col("settlement_id").alias("id_liquidacion"), F.col("auth_id").alias("id_autorizacion"),
        F.col("card_token").alias("token_tarjeta"), F.col("merchant_id").alias("comercio_id"),
        F.date_format("settlement_date", "yyyyMMdd").alias("fecha_liquidacion"),
        F.round(F.col("settled_amount") * 100).cast("long").alias("monto_centavos"), F.col("currency").alias("moneda"),
        F.round(F.col("settled_billing_ars") * 100).cast("long").alias("monto_pesos_centavos"), F.col("batch_id").alias("lote"),
        F.col("settlement_date").alias("delivery_date"),
    )
    out = {}
    for name, df in (("autorizaciones", auth), ("liquidaciones", sett)):
        path = cfg.staging_path(f"cards_legacy/{name}")
        (_delivered(df, cfg, day).repartition("delivery_date").write.mode("overwrite")
         .option("partitionOverwriteMode", "dynamic").partitionBy("delivery_date").json(path))
        out[name] = path
    return out


def write_api_data(spark: SparkSession, cfg: ProjectConfig, day: date | None = None) -> dict[str, str]:
    """Registros canónicos v2 que sirve la API, un JSONL por día de proceso (upload.py los parte en páginas)."""
    a = spark.table(cfg.table("sim", "authorizations")).where(F.col("processing_date") >= _d(ref.API_V2_LAUNCH))
    auth = a.select(
        "auth_id", "card_token", "pan_last4", "brand", "card_type", "merchant_id", "merchant_name", "mcc", "merchant_country",
        "merchant_city", "amount", "currency", "billing_amount_ars",
        F.date_format("auth_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("authorized_at"),
        F.col("processing_date").cast("string").alias("processing_date"), "status", "response_code", "entry_mode",
        # schema drift: el campo existe recién desde mar-2026 (antes no viene en el JSON)
        "wallet_type", F.col("processing_date").alias("delivery_date"),
    )
    s = spark.table(cfg.table("sim", "settlements")).where(F.col("settlement_date") >= _d(ref.API_V2_LAUNCH))
    sett = s.select(
        "settlement_id", "auth_id", "card_token", "merchant_id", F.col("settlement_date").cast("string").alias("settlement_date"),
        "settled_amount", "currency", "settled_billing_ars", "batch_id", "brand", F.col("settlement_date").alias("delivery_date"),
    )
    out = {}
    for name, df in (("authorizations", auth), ("settlements", sett)):
        path = cfg.staging_path(f"api_data/{name}")
        (_delivered(df, cfg, day).repartition("delivery_date").sortWithinPartitions("delivery_date", df.columns[0])
         .write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("delivery_date").json(path))
        out[name] = path
    return out


WRITERS = {
    "core/clientes": write_customers, "core/cuentas": write_accounts, "core/tarjetas": write_cards,
    "core/movimientos": write_transactions, "core/saldos": write_balances,
}


def export_all(spark: SparkSession, cfg: ProjectConfig, only: list[str] | None = None,
               day: date | None = None) -> dict[str, str]:
    """Exporta todas las fuentes, o sólo las de `only` (ej. ["core/tarjetas"], ["cards_legacy"], ["api_data"]).

    Con `day` exporta sólo lo que se entrega ese día (job "día nuevo"): el overwrite es dinámico, así que las carpetas
    de los demás días quedan como estaban.
    """
    paths = {name: fn(spark, cfg, day) for name, fn in WRITERS.items() if not only or name in only}
    if not only or "cards_legacy" in only:
        paths.update({f"cards_legacy/{k}": v for k, v in write_cards_legacy(spark, cfg, day).items()})
    if not only or "api_data" in only:
        paths.update({f"api_data/{k}": v for k, v in write_api_data(spark, cfg, day).items()})
    return paths
