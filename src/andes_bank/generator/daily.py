"""El día nuevo: el banco sigue vivo después de la historia.

`generate_truth` arma de una vez la historia hasta `HISTORY_END` (20-oct-2026). Desde ahí, el job "día nuevo" corre
todas las mañanas y agrega a `sim.*` los días que faltan hasta ayer, los exporta sucios y los sube a Azure. Así el
lakehouse, que corre después, tiene archivos, páginas de la API y cotizaciones nuevas cada día.

Qué pasa en un día nuevo (con las MISMAS fórmulas que la historia: `events.py`):

    autorizaciones del día  → cada tarjeta activa hace ~1,6 compras por mes (4 ensayos de Bernoulli por día) y, muy
                              de vez en cuando, le roban los datos (ráfaga de compras en rubros riesgosos)
    liquidaciones del día   → las compras aprobadas de 1 a 3 días antes que se liquidan hoy
    movimientos del día     → sueldo (días 1 a 5), movimientos propios, compras con débito, pago del resumen (día 10),
                              plazo fijo (fin de mes) y una transferencia de cobertura si la caja quedaría negativa
    saldos del día          → las cuentas que se movieron + la foto de fin de mes

Qué NO cambia: clientes, cuentas, tarjetas, comercios y el CRM quedan como al final de la historia.

Idempotente: antes de generar un día se borra de `sim.*` lo que haya de ese día (una corrida que falló a la mitad).
Los días generados quedan en `sim.generated_days` (la historia escribe ahí su último día).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from andes_bank.config import HISTORY_END, ProjectConfig
from andes_bank.generator import events, exports
from andes_bank.generator import reference as ref
from andes_bank.generator.rand import int_between, uniform, zipf_index

log = logging.getLogger(__name__)

DAILY_I0 = 1_000_000           # `i` de los eventos del día nuevo: no choca con los de la historia (1..n y 100.001+)
TRIALS = 4                     # eventos por día = 4 ensayos de Bernoulli: con medias chicas es casi un Poisson
DAILY_COMPROMISE = 0.004 / 365  # probabilidad diaria de que le roben los datos a una tarjeta (0,4 % por año)
GENERATED_DAYS_DDL = ("day DATE, generated_at TIMESTAMP, authorizations BIGINT, settlements BIGINT, transactions BIGINT, "
                      "balances BIGINT")


def _d(value: date) -> F.Column:
    return F.lit(value.isoformat()).cast("date")


def _ts(value: datetime) -> str:
    return f"timestamp'{value:%Y-%m-%d %H:%M:%S}'"


def local_day_bounds(day: date) -> tuple[datetime, datetime]:
    """[00:00, 24:00) del día en hora argentina, expresado en UTC (la convención de `sim`)."""
    start = datetime.combine(day, time()) + timedelta(hours=events.TZ_OFFSET_HOURS)
    return start, start + timedelta(days=1)


# --------------------------------------------------------------------------------------------- qué días
def record_generated_day(spark: SparkSession, cfg: ProjectConfig, day: date, counts: dict[str, int]) -> None:
    table = cfg.table("sim", "generated_days")
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], GENERATED_DAYS_DDL).write.saveAsTable(table)
    spark.sql(f"DELETE FROM {table} WHERE day = DATE'{day}'")
    row = (day, datetime.now(timezone.utc), *(counts.get(k, 0) for k in ("authorizations", "settlements", "transactions", "balances")))
    spark.createDataFrame([row], GENERATED_DAYS_DDL).write.mode("append").saveAsTable(table)


def last_generated_day(spark: SparkSession, cfg: ProjectConfig) -> date:
    table = cfg.table("sim", "generated_days")
    if spark.catalog.tableExists(table):
        last = spark.table(table).agg(F.max("day")).first()[0]
        if last:
            return last
    return HISTORY_END


def days_to_generate(spark: SparkSession, cfg: ProjectConfig, until: date | None = None) -> list[date]:
    """Del día siguiente al último generado hasta `until` (por defecto `cfg.end_date`, que en el job es ayer)."""
    last, until = last_generated_day(spark, cfg), until or cfg.end_date
    return [last + timedelta(days=i) for i in range(1, (until - last).days + 1)]


def ensure_months(spark: SparkSession, cfg: ProjectConfig, day: date) -> None:
    """Agrega a `sim.months` los meses que falten hasta `day` (índice de precios y dólar de referencia)."""
    table = cfg.table("sim", "months")
    have = {r.month for r in spark.table(table).select("month").collect()}
    rows = [r for r in ref.monthly_index_rows(cfg.start_date, day) if r[0] not in have]
    if rows:
        spark.createDataFrame(rows, "month string, price_index double, usd_official double") \
            .write.mode("append").saveAsTable(table)


# ------------------------------------------------------------------------------------------------ eventos
def _bernoulli(df: DataFrame, key: str, day_index: int, rate: F.Column, salt: str) -> DataFrame:
    """Una fila por evento del día: TRIALS ensayos con probabilidad rate / TRIALS cada uno (columna `k`)."""
    return (df.withColumn("k", F.explode(F.sequence(F.lit(1), F.lit(TRIALS))))
            .where(uniform(key, "k", F.lit(day_index), salt=salt) < rate / TRIALS))


def daily_authorizations(cards: DataFrame, merchants: DataFrame, months: DataFrame, cfg: ProjectConfig,
                         day: date) -> DataFrame:
    s, d, di = str(cfg.seed), _d(day), (day - cfg.start_date).days
    nxt = F.date_add(d, 1)
    active = cards.where((F.col("issued_date") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d)))
    rate = F.lit(cfg.profile.card_purchases_month / 30.4) * F.when(F.col("card_type") == "CREDITO", 1.15).otherwise(1.0)
    normal = (
        _bernoulli(active, "card_key", di, rate, f"{s}-dna")
        .withColumn("i", F.lit(DAILY_I0 + di * 100) + F.col("k"))
        .withColumn("auth_ts", events._random_ts(["card_key", "i"], f"{s}-ats", d, nxt))
        .withColumn("merchant_key", zipf_index(uniform("card_key", "i", salt=f"{s}-m"), cfg.n_merchants, 1.7))
        .withColumn("is_fraud", F.lit(False))
    )
    risky = events._risky_keys(merchants)
    burst = (
        active.where(uniform("card_key", F.lit(di), salt=f"{s}-dfr") < DAILY_COMPROMISE)
        .withColumn("k", F.explode(F.sequence(F.lit(1), int_between(uniform("card_key", F.lit(di), salt=f"{s}-dnb"), 3, 12))))
        .withColumn("i", F.lit(DAILY_I0 + di * 100 + 50) + F.col("k"))
        .withColumn("auth_ts", events._random_ts(["card_key", "i"], f"{s}-bts", d, nxt))
        .withColumn("merchant_key", F.element_at(F.array(*[F.lit(k) for k in risky]),
                                                 int_between(uniform("card_key", "i", salt=f"{s}-bm"), 1, len(risky))))
        .withColumn("is_fraud", F.lit(True))
    )
    auth = normal.select(events.AUTH_BASE_COLS).unionByName(burst.select(events.AUTH_BASE_COLS))
    return events.enrich_authorizations(auth, merchants, months, cfg)


def daily_settlements(auth: DataFrame, months: DataFrame, cfg: ProjectConfig, day: date) -> DataFrame:
    """Las liquidaciones de hoy: compras aprobadas de 1 a 3 días antes (las mismas reglas que la historia)."""
    recent = auth.where(F.to_date("auth_ts").between(_d(day - timedelta(days=3)), _d(day)))
    return events.settlements_df(recent, months, replace(cfg, end_date=day)).where(F.col("settlement_date") == _d(day))


def _accounts_on(customers: DataFrame, accounts: DataFrame, day: date) -> DataFrame:
    d = _d(day)
    return (
        accounts.join(customers.select("customer_key", "segment", "monthly_income_2023", "has_salary", "is_legacy_duplicate"),
                      "customer_key")
        .where((F.col("opened_date") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d)))
        .withColumn("act_start", d).withColumn("act_end", F.date_add(d, 1))
        .withColumn("income_factor", F.pow(F.col("monthly_income_2023") / ref.SALARY_MEDIAN_ARS_2023, 0.8))
    )


def last_balances(transactions: DataFrame) -> DataFrame:
    """Último saldo de cada cuenta (por instante y id del movimiento)."""
    return transactions.groupBy("account_key").agg(
        F.max_by("balance_after", F.struct("ts_utc", "txn_id")).alias("prev_balance"))


def daily_transactions(customers: DataFrame, accounts: DataFrame, auth_day: DataFrame, auth_all: DataFrame,
                       months: DataFrame, prev: DataFrame, cfg: ProjectConfig, day: date) -> DataFrame:
    """Los movimientos del día con su saldo posterior. `prev`: último saldo de cada cuenta antes de hoy."""
    s, d, di, p = str(cfg.seed), _d(day), (day - cfg.start_date).days, cfg.profile
    acc = _accounts_on(customers, accounts, day)
    month = months.where(F.col("month") == day.strftime("%Y-%m")).select("price_index")
    cols = events.TXN_BASE_COLS

    income = events.income_rows(acc.withColumn("m", F.trunc(d, "month")), months, cfg)
    rate = F.lit(p.txn_per_account_month / 30.4) * F.when(F.col("account_type") == "CA_USD", 0.25) \
        .when(F.col("account_type") == "CC_ARS", 2.0).otherwise(1.0) * F.when(F.col("is_legacy_duplicate"), 0.2).otherwise(1.0)
    own = events.own_movements(
        _bernoulli(acc, "account_key", di, rate, f"{s}-dnt")
        .withColumn("i", F.lit(DAILY_I0 + di * 10) + F.col("k"))
        .withColumn("ts_utc", events._random_ts(["account_key", "i"], f"{s}-ts", d, F.date_add(d, 1))),
        months, cfg)
    parts = [income.select(*cols), own.select(*cols), events.debit_purchases(auth_day).select(*cols)]
    if day.day == 10:   # el resumen de la tarjeta de crédito del mes anterior
        m0 = date(day.year, day.month, 1)
        m_prev = (m0 - timedelta(days=1)).replace(day=1)
        consumed = auth_all.where((F.col("auth_ts") >= F.lit(m_prev.isoformat()).cast("timestamp"))
                                  & (F.col("auth_ts") < F.lit(m0.isoformat()).cast("timestamp")))
        parts.append(events.credit_card_payments(consumed).where(F.col("day") == d).select(*cols))
    txn = parts[0]
    for part in parts[1:]:
        txn = txn.unionByName(part)
    txn = txn.withColumn("signed_amount", F.col("amount") * events.txn_sign())

    # cobertura: la historia arranca cada cuenta con el saldo justo para que la caja de ahorro nunca quede negativa;
    # en un día nuevo, si los débitos superan el saldo, entra una transferencia al empezar el día (con un colchón)
    debits = txn.where("signed_amount < 0").groupBy("account_key").agg(F.sum(-F.col("signed_amount")).alias("debits"))
    cover = (
        acc.where(F.col("account_type").startswith("CA_")).join(debits, "account_key").join(prev, "account_key", "left")
        .crossJoin(month)
        .withColumn("deficit", F.col("debits") - F.coalesce("prev_balance", F.lit(0.0)))
        .where("deficit > 0")
        .withColumn("amount", F.round(F.col("deficit") + F.when(F.col("currency") == "USD", 500.0)
                                      .otherwise(F.col("monthly_income_2023") * F.col("price_index") * 0.3), 2))
        .withColumn("ts_utc", F.lit(f"{local_day_bounds(day)[0]:%Y-%m-%d %H:%M:%S}").cast("timestamp"))
        .withColumn("txn_type", F.lit("TRANSFERENCIA_RECIBIDA")).withColumn("channel", F.lit("TRANSFERENCIA"))
        .withColumn("reference", F.lit("COBERTURA"))
        .select(*cols).withColumn("signed_amount", F.col("amount"))
    )
    txn = txn.unionByName(cover).withColumn("txn_id", events.txn_id_col(cfg))

    if (day + timedelta(days=1)).day == 1:
        # fin de mes: lo que excede 2,5 ingresos mensuales va a plazo fijo (como en la historia, pero sobre el saldo real)
        end = (txn.groupBy("account_key").agg(F.sum("signed_amount").alias("day_net"))
               .join(acc.where("currency = 'ARS'").select("account_key", "monthly_income_2023"), "account_key", "right")
               .join(prev, "account_key", "left").crossJoin(month)
               .withColumn("excess", F.coalesce("prev_balance", F.lit(0.0)) + F.coalesce("day_net", F.lit(0.0))
                           - F.lit(ref.SWEEP_INCOME_MONTHS) * F.col("monthly_income_2023") * F.col("price_index")))
        sweeps = (
            end.where("excess > 0")
            .withColumn("amount", F.round("excess", 2)).withColumn("signed_amount", -F.col("amount"))
            .withColumn("ts_utc", F.timestamp_seconds(F.unix_timestamp(F.to_timestamp(d))
                                                      + (events.TZ_OFFSET_HOURS + 20) * 3600 + 1800))
            .withColumn("txn_type", F.lit("CONSTITUCION_PLAZO_FIJO")).withColumn("channel", F.lit("HOMEBANKING"))
            .withColumn("reference", F.lit(f"PF-{day:%Y%m}"))
            .withColumn("txn_id", F.concat(F.lit("TP"), F.lpad(F.col("account_key").cast("string"), 12, "0"),
                                           F.lit(f"{day:%Y%m}")))
            .select(*cols, "signed_amount", "txn_id")
        )
        txn = txn.unionByName(sweeps)

    w = Window.partitionBy("account_key").orderBy("ts_utc", "txn_id").rowsBetween(Window.unboundedPreceding, 0)
    return (
        txn.join(prev, "account_key", "left")
        .withColumn("balance_after", F.round(F.coalesce("prev_balance", F.lit(0.0)) + F.sum("signed_amount").over(w), 2))
        .join(accounts.select("account_key", "account_id", "currency", "customer_key"), "account_key")
        .withColumn("posting_date", events.posting_date(F.col("ts_utc")))
        .select("txn_id", "account_key", "account_id", "customer_key", "ts_utc", "posting_date", "txn_type", "channel",
                "amount", "signed_amount", "currency", "balance_after", "reference")
    )


def daily_balances(transactions: DataFrame, accounts: DataFrame, day: date) -> DataFrame:
    """Saldo de cierre que informa el core para `day`: cuentas con movimiento contable ese día + foto de fin de mes."""
    d = _d(day)
    w = Window.partitionBy("account_key").orderBy(F.desc("ts_utc"), F.desc("txn_id"))
    out = (transactions.where(F.col("posting_date") == d).withColumn("rn", F.row_number().over(w)).where("rn = 1")
           .select("account_key", F.col("balance_after").alias("closing_balance"), F.lit(False).alias("snapshot")))
    if (day + timedelta(days=1)).day == 1:
        alive = accounts.where((F.col("opened_date") <= d) & (F.col("closed_date").isNull() | (F.col("closed_date") > d)))
        last = (transactions.where(F.col("posting_date") <= d).groupBy("account_key")
                .agg(F.max_by("balance_after", F.struct("ts_utc", "txn_id")).alias("closing_balance")))
        snap = (alive.select("account_key").join(last, "account_key").join(out.select("account_key"), "account_key", "left_anti")
                .withColumn("snapshot", F.lit(True)))
        out = out.unionByName(snap)
    return (out.join(accounts.select("account_key", "account_id", "currency"), "account_key")
            .select("account_key", "account_id", d.alias("balance_date"), "closing_balance", "currency", "snapshot"))


# ----------------------------------------------------------------------------------------------- un día
def generate_day(spark: SparkSession, cfg: ProjectConfig, day: date) -> dict[str, int]:
    """Agrega `day` a `sim.*` (idempotente) y devuelve cuántas filas agregó a cada tabla."""
    t = lambda name: spark.table(cfg.table("sim", name))  # noqa: E731
    cfg_d = replace(cfg, end_date=day)
    ensure_months(spark, cfg, day)
    start, end = local_day_bounds(day)
    # idempotencia: lo que haya quedado de este día en una corrida anterior se borra antes de volver a generarlo
    spark.sql(f"DELETE FROM {cfg.table('sim', 'authorizations')} WHERE auth_ts >= {_ts(start)} AND auth_ts < {_ts(end)}")
    spark.sql(f"DELETE FROM {cfg.table('sim', 'transactions')} WHERE ts_utc >= {_ts(start)} AND ts_utc < {_ts(end)}")
    spark.sql(f"DELETE FROM {cfg.table('sim', 'settlements')} WHERE settlement_date = DATE'{day}'")
    spark.sql(f"DELETE FROM {cfg.table('sim', 'balances')} WHERE balance_date = DATE'{day}'")

    counts = {}
    auth = daily_authorizations(t("cards"), t("merchants"), t("months"), cfg_d, day)
    auth.write.mode("append").saveAsTable(cfg.table("sim", "authorizations"))
    auth_day = t("authorizations").where(f"auth_ts >= {_ts(start)} AND auth_ts < {_ts(end)}")
    counts["authorizations"] = auth_day.count()

    sett = daily_settlements(t("authorizations"), t("months"), cfg_d, day)
    sett.write.mode("append").saveAsTable(cfg.table("sim", "settlements"))
    counts["settlements"] = t("settlements").where(F.col("settlement_date") == _d(day)).count()

    # se lee y se agrega a la misma tabla: Delta lee la versión anterior al commit (aislamiento de snapshot)
    prev = last_balances(t("transactions").where(f"ts_utc < {_ts(start)}"))
    txn = daily_transactions(t("customers"), t("accounts"), auth_day, t("authorizations"), t("months"), prev, cfg_d, day)
    txn.write.mode("append").saveAsTable(cfg.table("sim", "transactions"))
    counts["transactions"] = t("transactions").where(f"ts_utc >= {_ts(start)} AND ts_utc < {_ts(end)}").count()

    daily_balances(t("transactions"), t("accounts"), day).write.mode("append").saveAsTable(cfg.table("sim", "balances"))
    counts["balances"] = t("balances").where(F.col("balance_date") == _d(day)).count()
    log.info("día %s: %s", day, counts)
    return counts


def run_new_days(spark: SparkSession, cfg: ProjectConfig, sas_token: str | None = None,
                 until: date | None = None) -> list[dict]:
    """Genera, exporta y (con `sas_token`) sube a Azure cada día que falta hasta `until` (por defecto, ayer).

    Un día se marca como generado recién después de subirlo: si la subida falla, la próxima corrida lo rehace entero.
    """
    from andes_bank.generator.upload import upload_all

    out = []
    for day in days_to_generate(spark, cfg, until):
        counts = generate_day(spark, cfg, day)
        exports.export_all(spark, replace(cfg, end_date=day), day=day)
        if sas_token:
            counts["uploaded"] = upload_all(replace(cfg, end_date=day), sas_token, start=day, end=day,
                                            parts=("core", "api_data"))
        record_generated_day(spark, cfg, day, counts)
        out.append({"day": day.isoformat(), **counts})
    return out
