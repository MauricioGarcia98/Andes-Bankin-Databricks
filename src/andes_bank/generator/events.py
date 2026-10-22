"""Lo que le pasa al banco todos los días: compras con tarjeta, liquidaciones, movimientos y saldos.

El orden importa porque los sistemas están conectados, igual que en un banco real:

    autorizaciones (procesador) ──► liquidaciones (procesador)
          │ compras con débito aprobadas                │
          ▼                                             ▼
    movimientos del core ◄── pago del resumen de la tarjeta de crédito (día 10 del mes siguiente)
          │
          ▼
    saldos de cierre (core)

Así el lakehouse puede (y tiene que) conciliar un sistema contra el otro.
"""

from __future__ import annotations

from datetime import date, datetime, time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.generator import reference as ref
from andes_bank.generator.rand import (
    int_between,
    lognormal,
    poisson_approx,
    uniform,
    weighted_pick,
    zipf_index,
)

TZ_OFFSET_HOURS = 3     # Argentina: UTC-3 todo el año (sin horario de verano)
POSTING_CUTOFF_HOUR = 21  # el core cierra el día contable a las 21 h: lo posterior va al día siguiente
RISKY_MCC = ("5732", "5944", "7995", "5815", "4511")

# hora del día (local) en la que se hacen las compras y los movimientos
HOUR_WEIGHTS = [0.3, 0.2, 0.1, 0.1, 0.1, 0.2, 0.6, 1.5, 3, 4.5, 5.5, 6, 6.5, 6, 5.5, 5.5, 6, 6.5, 7, 7, 6, 4.5, 2.5, 1]


def _ts(d: date):
    return F.lit(datetime.combine(d, time()).isoformat()).cast("timestamp")


def _month(ts_col) -> F.Column:
    return F.date_format(ts_col, "yyyy-MM")


def _random_ts(key_cols, salt: str, start_col, end_col) -> F.Column:
    """Instante al azar entre dos fechas (fin excluido), con la distribución horaria de un banco real.

    Devuelve un timestamp en UTC: se arma en hora local y se le suman 3 horas.
    """
    span = F.greatest(F.datediff(end_col, start_col), F.lit(1))
    day = F.date_add(start_col, F.floor(uniform(*key_cols, salt=f"{salt}-d") * span).cast("int"))
    hour = weighted_pick(uniform(*key_cols, salt=f"{salt}-h"), list(range(24)), HOUR_WEIGHTS)
    secs = hour * 3600 + int_between(uniform(*key_cols, salt=f"{salt}-s"), 0, 3599) + TZ_OFFSET_HOURS * 3600
    return F.timestamp_seconds(F.unix_timestamp(F.to_timestamp(day)) + secs)


def posting_date(ts_utc) -> F.Column:
    """Fecha contable del core: hora local; después de las 21 h pasa al día siguiente."""
    local = ts_utc - F.expr(f"INTERVAL {TZ_OFFSET_HOURS} HOURS")
    return F.when(F.hour(local) >= POSTING_CUTOFF_HOUR, F.date_add(F.to_date(local), 1)).otherwise(F.to_date(local))


def _mcc_map(idx: int) -> F.Column:
    return F.create_map(*[x for k, v in ref.MCC.items() for x in (F.lit(k), F.lit(v[idx]))])


# --------------------------------------------------------------------------------- autorizaciones
def authorizations_df(cards: DataFrame, merchants: DataFrame, months: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Autorizaciones del procesador (formato canónico v2). `is_fraud` es verdad: el procesador no la informa."""
    s = str(cfg.seed)
    p = cfg.profile
    end_excl = F.date_add(F.lit(cfg.end_date.isoformat()).cast("date"), 1)
    active = (
        cards.withColumn("act_start", F.greatest("issued_date", F.lit(cfg.start_date.isoformat()).cast("date")))
        .withColumn("act_end", F.least(F.coalesce("closed_date", end_excl), end_excl))
        .where("act_end > act_start")
    )
    rate = F.lit(p.card_purchases_month) * F.when(F.col("card_type") == "CREDITO", 1.15).otherwise(1.0)
    n = poisson_approx(uniform("card_key", salt=f"{s}-na1"), uniform("card_key", salt=f"{s}-na2"),
                       rate * F.datediff("act_end", "act_start") / 30.4)
    normal = (
        active.withColumn("i", F.explode(F.sequence(F.lit(1), F.greatest(n, F.lit(0)))))
        .where("i > 0")
        .withColumn("auth_ts", _random_ts(["card_key", "i"], f"{s}-ats", F.col("act_start"), F.col("act_end")))
        .withColumn("merchant_key", zipf_index(uniform("card_key", "i", salt=f"{s}-m"), cfg.n_merchants, 1.7))
        .withColumn("is_fraud", F.lit(False))
    )
    # fraude: tarjetas comprometidas reciben una ráfaga de compras en rubros riesgosos el día del compromiso
    risky = merchants.where(F.col("mcc").isin(*RISKY_MCC) | (F.col("country") != "AR")).select("merchant_key")
    risky_keys = [r.merchant_key for r in risky.orderBy("merchant_key").limit(300).collect()] or [1]
    burst = (
        active.where("compromised AND compromise_date >= act_start AND compromise_date < act_end")
        .withColumn("i", F.explode(F.sequence(F.lit(100_001), F.lit(100_000) + int_between(uniform("card_key", salt=f"{s}-nb"), 3, 12))))
        .withColumn("auth_ts", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("compromise_date"))
                                                   + int_between(uniform("card_key", salt=f"{s}-bh"), 0, 18) * 3600
                                                   + int_between(uniform("card_key", "i", salt=f"{s}-bs"), 0, 21_600)))
        .withColumn("merchant_key", F.element_at(F.array(*[F.lit(k) for k in risky_keys]),
                                                 int_between(uniform("card_key", "i", salt=f"{s}-bm"), 1, len(risky_keys))))
        .withColumn("is_fraud", F.lit(True))
    )
    cols = ["card_key", "card_token", "customer_key", "ca_ars_account_key", "card_type", "brand", "pan_last4",
            "credit_limit_2023", "i", "auth_ts", "merchant_key", "is_fraud"]
    auth = normal.select(cols).unionByName(burst.select(cols))
    auth = (
        auth.join(F.broadcast(merchants.select("merchant_key", "merchant_id", "merchant_name", "mcc", "country", "city",
                                               "currency", "ecommerce")), "merchant_key")
        .withColumn("month", _month("auth_ts"))
        .join(F.broadcast(months), "month")
    )
    u = lambda salt: uniform("card_key", "i", salt=f"{s}-a-{salt}")  # noqa: E731
    median_ars = _mcc_map(1)[F.col("mcc")] * F.col("price_index") * F.when(F.col("is_fraud"), 6.0).otherwise(1.0)
    sigma = 0.9
    amount_ars = lognormal(u("am1"), u("am2"), median_ars, sigma)
    amount_usd = lognormal(u("am1"), u("am2"), _mcc_map(1)[F.col("mcc")] / 250.0 * F.when(F.col("is_fraud"), 4.0).otherwise(1.0), sigma)
    auth = (
        auth.withColumn("amount", F.round(F.when(F.col("currency") == "USD", amount_usd).otherwise(amount_ars), 2))
        .withColumn("billing_amount_ars", F.round(F.when(F.col("currency") == "USD", F.col("amount") * F.col("usd_official"))
                                                  .otherwise(F.col("amount")), 2))
    )
    reasons = list(ref.DECLINE_REASONS)
    probs = list(ref.DECLINE_REASONS.values())
    decline_total = sum(probs)
    auth = (
        auth.withColumn("u_dec", u("dec"))
        .withColumn("response_code", F.when(F.col("is_fraud"), F.when(u("fd") < 0.4, "59_SOSPECHA_FRAUDE").otherwise("00_APROBADA"))
                    .when(F.col("u_dec") < decline_total,
                          weighted_pick(F.col("u_dec") / decline_total, reasons, probs))
                    .otherwise("00_APROBADA"))
        # la tarjeta de débito no tiene límite; la de crédito no rechaza por fondos
        .withColumn("response_code", F.when((F.col("card_type") == "DEBITO") & (F.col("response_code") == "61_EXCEDE_LIMITE"),
                                            "51_FONDOS_INSUFICIENTES")
                    .when((F.col("card_type") == "CREDITO") & (F.col("response_code") == "51_FONDOS_INSUFICIENTES"),
                          "61_EXCEDE_LIMITE").otherwise(F.col("response_code")))
        .withColumn("status", F.when(F.col("response_code") == "00_APROBADA", "APPROVED").otherwise("DECLINED"))
        .withColumn("entry_mode", F.when(F.col("ecommerce"), "ECOMMERCE")
                    .otherwise(weighted_pick(u("em"), ["CONTACTLESS", "CHIP", "MAGSTRIPE"], [0.55, 0.40, 0.05])))
        .withColumn("wallet_type", F.when((F.col("auth_ts") >= _ts(ref.WALLET_TYPE_SINCE)) & (F.col("entry_mode") == "CONTACTLESS")
                                          & (u("wal") < 0.35), F.when(u("wal2") < 0.55, "GOOGLE_PAY").otherwise("APPLE_PAY")))
        # el procesador informa algunas autorizaciones 1 a 3 días tarde (llegan en la "fecha de proceso" siguiente)
        .withColumn("processing_date", F.date_add(F.to_date("auth_ts"),
                                                  F.when(u("late") < 0.988, 0).when(u("late") < 0.998, 1).otherwise(int_between(u("late2"), 2, 3))))
        .withColumn("auth_id", F.concat(F.lit("A"), F.upper(F.substring(F.sha2(F.concat_ws("|", F.lit(s), "card_key", "i"), 256), 1, 19))))
    )
    return auth.select(
        "auth_id", "card_key", "card_token", "customer_key", "ca_ars_account_key", "card_type", "brand", "pan_last4",
        "merchant_key", "merchant_id", "merchant_name", "mcc", F.col("country").alias("merchant_country"),
        F.col("city").alias("merchant_city"), "amount", "currency", "billing_amount_ars", "usd_official", "auth_ts",
        "processing_date", "status", "response_code", "entry_mode", "wallet_type", "is_fraud",
    )


def settlements_df(auth: DataFrame, months: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Liquidaciones: las aprobadas se liquidan 1-3 días después. Algunas se reversan o nunca llegan (DAT-15)."""
    s = str(cfg.seed)
    u = lambda salt: uniform("auth_id", salt=f"{s}-st-{salt}")  # noqa: E731
    st = (
        auth.where("status = 'APPROVED'")
        .where(u("rev") >= 0.017)                 # 1,5 % reversadas + 0,2 % que nunca se liquidan
        .withColumn("settlement_date", F.date_add(F.to_date("auth_ts"), int_between(u("d"), 1, 3)))
        .where(F.col("settlement_date") <= F.lit(cfg.end_date.isoformat()).cast("date"))
        .withColumn("month", F.date_format("settlement_date", "yyyy-MM"))
        .drop("usd_official").join(F.broadcast(months.select("month", "usd_official")), "month")
        # propina en restaurantes y tipo de cambio del día de liquidación en compras del exterior
        .withColumn("settled_amount", F.round(F.when((F.col("mcc") == "5812") & (u("tip") < 0.5),
                                                     F.col("amount") * (1 + u("tipp") * 0.15)).otherwise(F.col("amount")), 2))
        .withColumn("settled_billing_ars", F.round(F.when(F.col("currency") == "USD", F.col("settled_amount") * F.col("usd_official"))
                                                   .otherwise(F.col("settled_amount")), 2))
        .withColumn("settlement_id", F.concat(F.lit("S"), F.substring("auth_id", 2, 19)))
        .withColumn("batch_id", F.concat(F.lit("B"), F.date_format("settlement_date", "yyyyMMdd"), F.lit("-"), F.col("brand")))
    )
    return st.select("settlement_id", "auth_id", "card_token", "merchant_id", "settlement_date", "settled_amount", "currency",
                     "settled_billing_ars", "batch_id", "brand")


# ------------------------------------------------------------------------------------ movimientos
def transactions_df(customers: DataFrame, accounts: DataFrame, auth: DataFrame, months: DataFrame,
                    cfg: ProjectConfig) -> DataFrame:
    """Movimientos del core (verdad), con el saldo posterior de cada uno."""
    s = str(cfg.seed)
    p = cfg.profile
    start = F.lit(cfg.start_date.isoformat()).cast("date")
    end_excl = F.date_add(F.lit(cfg.end_date.isoformat()).cast("date"), 1)
    acc = (
        accounts.join(customers.select("customer_key", "segment", "monthly_income_2023", "has_salary", "is_legacy_duplicate"),
                      "customer_key")
        .withColumn("act_start", F.greatest("opened_date", start))
        .withColumn("act_end", F.least(F.coalesce("closed_date", end_excl), end_excl))
        .where("act_end > act_start")
        .withColumn("income_factor", F.pow(F.col("monthly_income_2023") / ref.SALARY_MEDIAN_ARS_2023, 0.8))
    )

    # 1) ingresos mensuales: sueldo (o transferencia de ingresos) en la caja de ahorro en pesos
    month_starts = F.sequence(F.trunc("act_start", "month"), F.trunc(F.date_sub("act_end", 1), "month"), F.expr("INTERVAL 1 MONTH"))
    income = (
        acc.where("account_type = 'CA_ARS' AND NOT is_legacy_duplicate")
        .withColumn("m", F.explode(month_starts))
        .withColumn("i", F.datediff("m", start))
        .withColumn("day", F.date_add("m", int_between(uniform("account_key", "i", salt=f"{s}-sd"), 0, 4)))
        .where((F.col("day") >= F.col("act_start")) & (F.col("day") < F.col("act_end")))
        .where(F.col("has_salary") | (uniform("account_key", "i", salt=f"{s}-inc") < 0.7))
        .withColumn("txn_type", F.when(F.col("has_salary"), "ACREDITACION_SUELDO").otherwise("TRANSFERENCIA_RECIBIDA"))
        .withColumn("ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("day")) + (TZ_OFFSET_HOURS + 6) * 3600
                                                  + int_between(uniform("account_key", "i", salt=f"{s}-st"), 0, 7200)))
        .withColumn("month", _month("ts_utc")).join(F.broadcast(months), "month")
        .withColumn("amount", F.round(F.col("monthly_income_2023") * F.col("price_index")
                                      * (0.97 + 0.06 * uniform("account_key", "i", salt=f"{s}-sa")), 2))
        .withColumn("channel", F.lit("TRANSFERENCIA"))
        .withColumn("reference", F.lit(None).cast("string"))
    )

    # 2) movimientos "propios" de cada cuenta
    types = list(ref.TXN_TYPES)
    rate = F.lit(p.txn_per_account_month) * F.when(F.col("account_type") == "CA_USD", 0.25) \
        .when(F.col("account_type") == "CC_ARS", 2.0).otherwise(1.0) * F.when(F.col("is_legacy_duplicate"), 0.2).otherwise(1.0)
    n = poisson_approx(uniform("account_key", salt=f"{s}-n1"), uniform("account_key", salt=f"{s}-n2"),
                       rate * F.datediff("act_end", "act_start") / 30.4)
    own = (
        acc.withColumn("i", F.explode(F.sequence(F.lit(1), F.greatest(n, F.lit(0))))).where("i > 0")
        # en la cuenta corriente (pymes y premium) entran las cobranzas de sus clientes: más créditos y más grandes
        .withColumn("txn_type", F.when(F.col("account_type") == "CC_ARS",
                                       weighted_pick(uniform("account_key", "i", salt=f"{s}-tt"), types, [v[4] for v in ref.TXN_TYPES.values()]))
                    .otherwise(weighted_pick(uniform("account_key", "i", salt=f"{s}-tt"), types, [v[3] for v in ref.TXN_TYPES.values()])))
        .withColumn("ts_utc", _random_ts(["account_key", "i"], f"{s}-ts", F.col("act_start"), F.col("act_end")))
        .withColumn("month", _month("ts_utc")).join(F.broadcast(months), "month")
    )
    med = F.create_map(*[x for k, v in ref.TXN_TYPES.items() for x in (F.lit(k), F.lit(v[1]))])[F.col("txn_type")]
    sig = F.create_map(*[x for k, v in ref.TXN_TYPES.items() for x in (F.lit(k), F.lit(v[2]))])[F.col("txn_type")]
    u1, u2 = uniform("account_key", "i", salt=f"{s}-a1"), uniform("account_key", "i", salt=f"{s}-a2")
    med = med * F.when((F.col("account_type") == "CC_ARS") & (F.col("txn_type") == "TRANSFERENCIA_RECIBIDA"), 3.0).otherwise(1.0)
    ars = med * F.col("price_index") * F.col("income_factor") * F.exp(sig * F.sqrt(F.lit(-2.0) * F.log(F.greatest(u1, F.lit(1e-12))))
                                                                      * F.cos(F.lit(6.283185307179586) * u2))
    usd = lognormal(u1, u2, 150.0, 1.0)
    own = (
        own.withColumn("amount", F.round(F.when(F.col("currency") == "USD", usd).otherwise(ars), 2))
        .withColumn("channel", F.when(F.col("txn_type") == "EXTRACCION_CAJERO", "CAJERO")
                    .when(F.col("txn_type") == "DEBITO_AUTOMATICO", "DEBITO_AUTOMATICO")
                    .when(F.col("txn_type") == "DEPOSITO_EFECTIVO", F.when(uniform("account_key", "i", salt=f"{s}-ch") < 0.6, "SUCURSAL").otherwise("CAJERO"))
                    .when(F.col("txn_type").isin("COMISION_MANTENIMIENTO", "INTERESES"), "SISTEMA")
                    .otherwise(weighted_pick(uniform("account_key", "i", salt=f"{s}-ch2"), ["APP", "HOMEBANKING", "SUCURSAL"], [0.62, 0.3, 0.08])))
        .withColumn("reference", F.lit(None).cast("string"))
    )

    # 3) compras con tarjeta de débito aprobadas → débito en la caja de ahorro en pesos
    debit = (
        auth.where("card_type = 'DEBITO' AND status = 'APPROVED'")
        .select(F.col("ca_ars_account_key").alias("account_key"), F.col("auth_ts").alias("ts_utc"),
                F.col("billing_amount_ars").alias("amount"), F.col("auth_id").alias("reference"))
        .withColumn("txn_type", F.lit("COMPRA_DEBITO")).withColumn("channel", F.lit("TARJETA_DEBITO"))
        .withColumn("i", F.lit(None).cast("int"))
    )

    # 4) pago del resumen de la tarjeta de crédito: día 10 del mes siguiente, por lo consumido en el mes
    credit = (
        auth.where("card_type = 'CREDITO' AND status = 'APPROVED'")
        .groupBy(F.col("ca_ars_account_key").alias("account_key"), F.trunc("auth_ts", "month").alias("m"))
        .agg(F.round(F.sum("billing_amount_ars"), 2).alias("amount"))
        .withColumn("day", F.date_add(F.add_months("m", 1), 9))
        .where(F.col("day") <= F.lit(cfg.end_date.isoformat()).cast("date"))
        .withColumn("ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("day")) + (TZ_OFFSET_HOURS + 7) * 3600))
        .withColumn("txn_type", F.lit("PAGO_TARJETA_CREDITO")).withColumn("channel", F.lit("SISTEMA"))
        .withColumn("reference", F.concat(F.lit("RESUMEN-"), F.date_format("m", "yyyyMM")))
        .withColumn("i", F.lit(None).cast("int"))
        .select("account_key", "ts_utc", "amount", "reference", "txn_type", "channel", "i")
    )

    sign = F.when(F.col("txn_type").isin("ACREDITACION_SUELDO", "TRANSFERENCIA_RECIBIDA", "DEPOSITO_EFECTIVO", "INTERESES"), 1).otherwise(-1)
    cols = ["account_key", "ts_utc", "amount", "reference", "txn_type", "channel"]
    txn = (
        income.select(*cols).unionByName(own.select(*cols)).unionByName(debit.select(*cols)).unionByName(credit.select(*cols))
        .withColumn("signed_amount", F.col("amount") * sign)
        .withColumn("txn_id", F.concat(F.lit("T"), F.upper(F.substring(
            F.sha2(F.concat_ws("|", F.lit(s), "account_key", F.col("ts_utc").cast("string"), "txn_type", "amount", F.coalesce("reference", F.lit(""))), 256), 1, 19))))
    )
    # plazo fijo a fin de mes: lo que excede 2,5 ingresos mensuales sale de la cuenta (si no, los saldos crecen sin
    # techo). Con B_m = saldo acumulado a fin del mes m y T_m = el techo, lo invertido acumulado es
    # S_m = max(0, max_{k<=m}(B_k - T_k)) y la inversión del mes es S_m - S_{m-1}: un máximo corrido, sin bucles.
    w = Window.partitionBy("account_key").orderBy("ts_utc", "txn_id").rowsBetween(Window.unboundedPreceding, 0)
    txn = txn.withColumn("cum", F.sum("signed_amount").over(w))
    w_m = Window.partitionBy("account_key", "m").orderBy(F.desc("ts_utc"), F.desc("txn_id"))
    w_run = Window.partitionBy("account_key").orderBy("m").rowsBetween(Window.unboundedPreceding, 0)
    w_prev = Window.partitionBy("account_key").orderBy("m")
    sweeps = (
        txn.withColumn("m", F.trunc("ts_utc", "month"))
        .withColumn("rn", F.row_number().over(w_m)).where("rn = 1")
        .join(acc.where("currency = 'ARS'").select("account_key", "monthly_income_2023", "act_end"), "account_key")
        .withColumn("month", F.date_format("m", "yyyy-MM")).join(F.broadcast(months.select("month", "price_index")), "month")
        .withColumn("excess", F.col("cum") - F.lit(ref.SWEEP_INCOME_MONTHS) * F.col("monthly_income_2023") * F.col("price_index"))
        .withColumn("s_cum", F.greatest(F.lit(0.0), F.max("excess").over(w_run)))
        .withColumn("amount", F.round(F.col("s_cum") - F.coalesce(F.lag("s_cum").over(w_prev), F.lit(0.0)), 2))
        .withColumn("day", F.last_day("m"))
        .where((F.col("amount") > 0) & (F.col("day") < F.col("act_end")))
        .withColumn("ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("day")) + (TZ_OFFSET_HOURS + 20) * 3600 + 1800))
        .withColumn("txn_type", F.lit("CONSTITUCION_PLAZO_FIJO")).withColumn("channel", F.lit("HOMEBANKING"))
        .withColumn("reference", F.concat(F.lit("PF-"), F.date_format("m", "yyyyMM")))
        .withColumn("signed_amount", -F.col("amount"))
        .withColumn("txn_id", F.concat(F.lit("TP"), F.lpad(F.col("account_key").cast("string"), 12, "0"), F.date_format("m", "yyyyMM")))
        .select(*cols, "signed_amount", "txn_id")
    )
    txn = txn.drop("cum").unionByName(sweeps).withColumn("cum", F.sum("signed_amount").over(w))

    # saldo inicial: lo justo para que la caja de ahorro nunca quede negativa (+ un colchón), así los saldos son creíbles
    opening = (
        txn.groupBy("account_key").agg(F.min("cum").alias("min_cum"))
        .join(acc.select("account_key", "act_start", "opened_date", "monthly_income_2023", "currency", "overdraft_limit_2023"), "account_key")
        .withColumn("opening", F.round(F.greatest(F.lit(0.0), -F.col("min_cum") - F.col("overdraft_limit_2023"))
                                       + F.when(F.col("currency") == "USD", 500.0).otherwise(F.col("monthly_income_2023") * 0.3), 2))
    )
    open_txn = (
        opening.withColumn("ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp("act_start")) + TZ_OFFSET_HOURS * 3600 + 1))
        .withColumn("txn_type", F.when(F.col("opened_date") < start, "SALDO_INICIAL").otherwise("DEPOSITO_EFECTIVO"))
        .withColumn("channel", F.when(F.col("opened_date") < start, "SISTEMA").otherwise("SUCURSAL"))
        .withColumn("amount", F.col("opening")).withColumn("signed_amount", F.col("opening"))
        .withColumn("reference", F.lit(None).cast("string"))
        .withColumn("txn_id", F.concat(F.lit("T0"), F.lpad(F.col("account_key").cast("string"), 18, "0")))
    )
    all_txn = txn.drop("cum").unionByName(open_txn.select(*cols, "signed_amount", "txn_id"))
    w2 = Window.partitionBy("account_key").orderBy("ts_utc", "txn_id").rowsBetween(Window.unboundedPreceding, 0)
    return (
        all_txn.withColumn("balance_after", F.round(F.sum("signed_amount").over(w2), 2))
        .join(accounts.select("account_key", "account_id", "currency", "customer_key"), "account_key")
        .withColumn("posting_date", posting_date(F.col("ts_utc")))
        .select("txn_id", "account_key", "account_id", "customer_key", "ts_utc", "posting_date", "txn_type", "channel",
                "amount", "signed_amount", "currency", "balance_after", "reference")
    )


def balances_df(txn: DataFrame, accounts: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Saldo de cierre que informa el core: cuentas con movimiento ese día + foto de fin de mes de todas."""
    w_last = Window.partitionBy("account_key", "posting_date").orderBy(F.desc("ts_utc"), F.desc("txn_id"))
    daily = (
        txn.withColumn("rn", F.row_number().over(w_last)).where("rn = 1")
        .select("account_key", F.col("posting_date").alias("balance_date"), F.col("balance_after").alias("closing_balance"))
        .withColumn("snapshot", F.lit(False))
    )
    start = F.lit(cfg.start_date.isoformat()).cast("date")
    end = F.lit(cfg.end_date.isoformat()).cast("date")
    month_ends = (
        accounts.select("account_key", "opened_date", "closed_date")
        .withColumn("balance_date", F.explode(F.sequence(F.last_day(start), F.last_day(end), F.expr("INTERVAL 1 MONTH"))))
        .withColumn("balance_date", F.last_day("balance_date"))
        .where((F.col("balance_date") >= F.col("opened_date")) & (F.col("balance_date") <= end)
               & (F.col("closed_date").isNull() | (F.col("balance_date") < F.col("closed_date"))))
        .select("account_key", "balance_date", F.lit(None).cast("double").alias("closing_balance"), F.lit(True).alias("snapshot"))
    )
    w = Window.partitionBy("account_key").orderBy("balance_date", "snapshot").rowsBetween(Window.unboundedPreceding, 0)
    both = (
        daily.unionByName(month_ends)
        .withColumn("closing_balance", F.last("closing_balance", ignorenulls=True).over(w))
        .where(F.col("closing_balance").isNotNull())
    )
    # si el fin de mes tuvo movimiento, queda una sola fila (la del día)
    w_dedup = Window.partitionBy("account_key", "balance_date").orderBy("snapshot")
    return (
        both.withColumn("rn", F.row_number().over(w_dedup)).where("rn = 1").drop("rn")
        .join(accounts.select("account_key", "account_id", "currency"), "account_key")
        .select("account_key", "account_id", "balance_date", "closing_balance", "currency", "snapshot")
    )
