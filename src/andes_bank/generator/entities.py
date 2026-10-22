"""Las entidades del banco (la "verdad" de los sistemas origen): sucursales, clientes, cuentas, tarjetas y comercios.

Todo sale de `spark.range` + hashes determinísticos (`rand.uniform`): la misma semilla da siempre el mismo banco, sin
importar cuántos executors haya. Estas tablas viven en el esquema `sim`; el lakehouse NO las lee (llega a ellas
sólo a través de los exportes sucios del core, la API del procesador y el CRM).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.generator import reference as ref
from andes_bank.generator.rand import int_between, pick, uniform, weighted_pick
from andes_bank.utils.ids import cbu_col, cuit_col, cuit_company_col

AREA_CODES = {"Mendoza": "261", "San Juan": "264", "San Luis": "266", "Salta": "387", "Jujuy": "388",
              "Tucumán": "381", "Córdoba": "351", "CABA": "11", "Buenos Aires": "221"}


def _d(value: date):
    return F.lit(value.isoformat()).cast("date")


def branches_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(ref.branches())


def monthly_index_df(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Índice de precios (base ene-2023 = 1) y dólar oficial de fin de mes, por mes."""
    return spark.createDataFrame(ref.monthly_index_rows(cfg.start_date, cfg.end_date),
                                 "month string, price_index double, usd_official double")


def _branch_for_region(u, region_col) -> F.Column:
    """Sucursal al azar dentro de la región (ponderada por cantidad de sucursales de cada ciudad)."""
    expr = None
    for region in ("CUYO", "NOA", "CENTRO"):
        ids = [b["branch_id"] for b in ref.branches() if b["region"] == region]
        branch = pick(u, ids)
        expr = F.when(region_col == region, branch) if expr is None else expr.when(region_col == region, branch)
    return expr.otherwise(F.lit(ref.DIGITAL_BRANCH_ID))


def customers_df(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Clientes. Columnas de "verdad" (`person_id`, `is_legacy_duplicate`) que el core no exporta."""
    s = str(cfg.seed)
    n = cfg.n_customers
    base = spark.range(1, n + 1).withColumnRenamed("id", "customer_key")
    u = lambda salt: uniform("customer_key", salt=f"{s}-cust-{salt}")  # noqa: E731

    regions = list(ref.REGION_WEIGHTS)
    df = (
        base.withColumn("region", weighted_pick(u("region"), regions, list(ref.REGION_WEIGHTS.values())))
        .withColumn("branch_id", _branch_for_region(u("branch"), F.col("region")))
        .withColumn("segment", weighted_pick(u("segment"), ref.SEGMENTS, ref.SEGMENT_WEIGHTS))
        .withColumn("person_type", F.when(F.col("segment") == "PYME", "JURIDICA").otherwise("HUMANA"))
        .withColumn("female", u("sex") < 0.52)
        .withColumn("age_2023", int_between(u("age"), 18, 85))
        .withColumn("birth_date", F.date_sub(_d(date(2023, 1, 1)),
                                             (F.col("age_2023") * 365.25 + int_between(u("bday"), 0, 364)).cast("int")))
        # DNI correlacionado con la edad (los más jóvenes tienen números más altos), como en la realidad
        .withColumn("dni", (F.lit(47_000_000) - F.col("age_2023") * 520_000 + int_between(u("dni"), 0, 900_000)).cast("long"))
        .withColumn("first_name", F.when(F.col("female"), pick(u("fn"), ref.FIRST_NAMES_F)).otherwise(pick(u("fn"), ref.FIRST_NAMES_M)))
        .withColumn("last_name", pick(u("ln"), ref.LAST_NAMES))
        .withColumn("company_name", F.concat_ws(" ", pick(u("cw"), ref.COMPANY_WORDS), pick(u("ln2"), ref.LAST_NAMES),
                                                pick(u("cs"), ref.COMPANY_SUFFIX)))
        .withColumn("cuit", F.when(F.col("person_type") == "JURIDICA",
                                   cuit_company_col(F.col("customer_key") * 37 + 10_000_000))
                    .otherwise(cuit_col(F.col("dni"), F.col("female"))))
        .withColumn("full_name", F.when(F.col("person_type") == "JURIDICA", F.col("company_name"))
                    .otherwise(F.concat_ws(" ", "first_name", "last_name")))
    )
    # antigüedad: Cuyo desde 1998, NOA desde 2008 (compra), Centro desde 2016, Digital desde 2020.
    # ~8 % son altas dentro del período (generan CDC de inserción).
    first_year = F.when(F.col("region") == "CUYO", 1998).when(F.col("region") == "NOA", 2008) \
        .when(F.col("region") == "CENTRO", 2016).otherwise(2020)
    window_days = cfg.n_days
    df = (
        df.withColumn("new_in_window", u("new") < 0.08)
        .withColumn("customer_since", F.when(
            F.col("new_in_window"), F.date_add(_d(cfg.start_date), int_between(u("since"), 0, window_days - 2)))
            .otherwise(F.make_date(first_year + F.floor(u("since") * (2023 - first_year)).cast("int"),
                                   int_between(u("sm"), 1, 12), int_between(u("sd"), 1, 28))))
        .withColumn("closed_date", F.when(u("churn") < 0.05,
                                          F.date_add(F.greatest(F.col("customer_since"), _d(cfg.start_date)),
                                                     int_between(u("closed"), 30, 900)))
                    .otherwise(F.lit(None).cast("date")))
        .withColumn("closed_date", F.when(F.col("closed_date") < _d(cfg.end_date), F.col("closed_date")))
        # cambio de segmento y de email dentro del período (CDC de actualización)
        .withColumn("segment_change_date", F.when(u("segch") < 0.10,
                                                  F.date_add(_d(cfg.start_date), int_between(u("segd"), 30, window_days - 30))))
        .withColumn("new_segment", F.when(F.col("segment") == "MASIVO", "PLUS").when(F.col("segment") == "PLUS", "PREMIUM")
                    .otherwise(F.col("segment")))
        .withColumn("email_change_date", F.when(u("emch") < 0.08,
                                                F.date_add(_d(cfg.start_date), int_between(u("emd"), 30, window_days - 30))))
    )
    province = F.element_at(F.create_map(*[x for b in ref.branches() for x in (F.lit(b["branch_id"]), F.lit(b["province"]))]),
                            F.col("branch_id"))
    city = F.element_at(F.create_map(*[x for b in ref.branches() for x in (F.lit(b["branch_id"]), F.lit(b["city"]))]),
                        F.col("branch_id"))
    area = F.element_at(F.create_map(*[x for k, v in AREA_CODES.items() for x in (F.lit(k), F.lit(v))]), province)
    email_user = F.lower(F.translate(F.concat_ws(".", "first_name", "last_name"), "áéíóúÁÉÍÓÚñÑ ", "aeiouAEIOUnN"))
    df = (
        df.withColumn("province", province).withColumn("city", city)
        .withColumn("phone", F.concat(F.lit("+54 9 "), area, F.lit(" "), int_between(u("ph"), 4_000_000, 6_999_999).cast("string")))
        .withColumn("email", F.when(F.col("person_type") == "JURIDICA",
                                    F.concat(F.lit("administracion@"), F.lower(F.regexp_replace("company_name", r"[^A-Za-z]", "")), F.lit(".com.ar")))
                    .otherwise(F.concat(email_user, (F.col("customer_key") % 97).cast("string"), F.lit("@"),
                                        pick(u("dom"), ref.EMAIL_DOMAINS))))
        .withColumn("new_email", F.regexp_replace("email", "@.*$", "@gmail.com"))
        .withColumn("monthly_income_2023", F.when(F.col("segment") == "PREMIUM", 4.0).when(F.col("segment") == "PLUS", 1.8)
                    .when(F.col("segment") == "PYME", 6.0).otherwise(1.0) * F.lit(ref.SALARY_MEDIAN_ARS_2023)
                    * F.exp(F.lit(0.35) * (u("inc") - 0.5) * 2))
        .withColumn("has_salary", (F.col("person_type") == "HUMANA") & (u("salary") < cfg.profile.salary_share))
        .withColumn("person_id", F.col("customer_key"))
        .withColumn("is_legacy_duplicate", F.lit(False))
    )
    prefix = F.when(F.col("region") == "CUYO", "CUY").when(F.col("region") == "NOA", "NOA").otherwise("AB")
    df = df.withColumn("customer_id", F.concat(prefix, F.lit("-"), F.lpad(F.col("customer_key").cast("string"), 7, "0")))

    # Fusión 2008: una parte de los clientes del NOA ya eran clientes del banco de Cuyo → la misma persona con dos
    # números de cliente. El core no lo sabe; silver tiene que descubrirlo (DAT-08).
    dups = (
        df.where((F.col("region") == "NOA") & (F.col("person_type") == "HUMANA")
                 & (uniform("customer_key", salt=f"{s}-dup") < cfg.profile.legacy_duplicate_share * 4))
        .withColumn("customer_key", F.col("customer_key") + F.lit(n))
        .withColumn("region", F.lit("CUYO"))
        .withColumn("branch_id", _branch_for_region(uniform("customer_key", salt=f"{s}-dupbr"), F.lit("CUYO")))
        .withColumn("customer_id", F.concat(F.lit("CUY-"), F.lpad(F.col("customer_key").cast("string"), 7, "0")))
        # el sistema viejo de Cuyo cargaba el nombre en mayúsculas y sin acentos
        .withColumn("full_name", F.upper(F.translate("full_name", "áéíóúÁÉÍÓÚ", "aeiouAEIOU")))
        .withColumn("customer_since", F.make_date(F.lit(2001) + int_between(uniform("customer_key", salt=f"{s}-dy"), 0, 6),
                                                  F.lit(6), F.lit(1)))
        .withColumn("province", F.lit("Mendoza")).withColumn("city", F.lit("Mendoza"))
        .withColumn("is_legacy_duplicate", F.lit(True))
        .withColumn("has_salary", F.lit(False))
        .withColumn("new_in_window", F.lit(False))
    )
    cols = ["customer_key", "customer_id", "person_id", "is_legacy_duplicate", "person_type", "full_name", "first_name",
            "last_name", "female", "birth_date", "dni", "cuit", "email", "phone", "region", "province", "city",
            "branch_id", "segment", "customer_since", "new_in_window", "closed_date", "segment_change_date",
            "new_segment", "email_change_date", "new_email", "monthly_income_2023", "has_salary"]
    return df.select(cols).unionByName(dups.select(cols))


def accounts_df(customers: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Cuentas: caja de ahorro en pesos (todos), en dólares (40 %) y cuenta corriente (15 %, más en pymes/premium)."""
    s = str(cfg.seed)
    p = cfg.profile
    u = lambda salt: uniform("customer_key", salt=f"{s}-acct-{salt}")  # noqa: E731
    cc_prob = F.when(F.col("segment").isin("PYME", "PREMIUM"), 0.7).otherwise(p.checking_share * 0.6)
    kinds = F.array(
        F.struct(F.lit(1).alias("n"), F.lit("CA_ARS").alias("account_type"), F.lit("ARS").alias("currency"), F.lit(True).alias("has")),
        F.struct(F.lit(2).alias("n"), F.lit("CA_USD").alias("account_type"), F.lit("USD").alias("currency"),
                 (u("usd") < p.usd_account_share).alias("has")),
        F.struct(F.lit(3).alias("n"), F.lit("CC_ARS").alias("account_type"), F.lit("ARS").alias("currency"),
                 (u("cc") < cc_prob).alias("has")),
    )
    df = (
        customers.select("customer_key", "customer_id", "branch_id", "segment", "customer_since", "closed_date", kinds.alias("k"))
        .withColumn("k", F.explode("k")).where("k.has")
        .select("*", "k.*").drop("k", "has")
        .withColumn("account_key", F.col("customer_key") * 10 + F.col("n"))
        .withColumn("account_id", F.concat(F.lit("AC"), F.lpad(F.col("account_key").cast("string"), 11, "0")))
        .withColumn("cbu", cbu_col(F.col("branch_id"), F.col("account_key")))
        # la cuenta en USD o la corriente se abren después del alta del cliente
        .withColumn("opened_date", F.when(F.col("n") == 1, F.col("customer_since"))
                    .otherwise(F.date_add("customer_since", int_between(uniform("account_key", salt=f"{s}-open"), 0, 700))))
        .withColumn("opened_date", F.when(F.col("opened_date") >= F.coalesce("closed_date", _d(cfg.end_date)), F.col("customer_since"))
                    .otherwise(F.col("opened_date")))
        .withColumn("overdraft_limit_2023", F.when(F.col("account_type") == "CC_ARS",
                                                   F.when(F.col("segment") == "PYME", 2_000_000).otherwise(400_000)).otherwise(0))
    )
    return df.select("account_key", "account_id", "customer_key", "customer_id", "account_type", "currency", "cbu",
                     "branch_id", "opened_date", F.col("closed_date"), "overdraft_limit_2023")


def cards_df(customers: DataFrame, accounts: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Tarjetas: débito (asociada a la caja de ahorro en pesos) y crédito (Visa / Mastercard)."""
    s = str(cfg.seed)
    p = cfg.profile
    ca = accounts.where("account_type = 'CA_ARS'").select("customer_key", F.col("account_key").alias("ca_ars_account_key"))
    base = customers.select("customer_key", "segment", "customer_since", "closed_date", "person_type").join(ca, "customer_key")
    u = lambda salt: uniform("customer_key", salt=f"{s}-card-{salt}")  # noqa: E731
    credit_prob = F.when(F.col("segment") == "PREMIUM", 0.9).when(F.col("segment") == "PLUS", 0.6) \
        .when(F.col("segment") == "PYME", 0.5).otherwise(p.credit_card_share * 0.7)
    kinds = F.array(
        F.struct(F.lit(1).alias("n"), F.lit("DEBITO").alias("card_type"), (u("deb") < p.debit_card_share).alias("has")),
        F.struct(F.lit(2).alias("n"), F.lit("CREDITO").alias("card_type"), (u("cred") < credit_prob).alias("has")),
    )
    limit_base = F.when(F.col("segment") == "PREMIUM", 3_000_000).when(F.col("segment") == "PLUS", 1_200_000) \
        .when(F.col("segment") == "PYME", 2_500_000).otherwise(450_000)
    df = (
        base.withColumn("k", F.explode(kinds)).where("k.has").select("*", "k.*").drop("k", "has")
        .withColumn("card_key", F.col("customer_key") * 10 + F.col("n"))
        .withColumn("brand", F.when(F.col("card_type") == "DEBITO", "VISA")
                    .otherwise(F.when(uniform("card_key", salt=f"{s}-brand") < 0.55, "VISA").otherwise("MASTERCARD")))
        .withColumn("card_token", F.substring(F.sha2(F.concat(F.lit(s), F.lit("tok"), F.col("card_key").cast("string")), 256), 1, 24))
        .withColumn("pan_last4", F.lpad((F.col("card_key") * 7919 % 10000).cast("string"), 4, "0"))
        .withColumn("issued_date", F.date_add("customer_since", int_between(uniform("card_key", salt=f"{s}-iss"), 0, 120)))
        .withColumn("credit_limit_2023", F.when(F.col("card_type") == "CREDITO", limit_base).otherwise(F.lit(0)))
        .withColumn("compromised", uniform("card_key", salt=f"{s}-fraud") < 0.004)
        .withColumn("compromise_date", F.when(F.col("compromised"),
                                              F.date_add(_d(cfg.start_date), int_between(uniform("card_key", salt=f"{s}-fd"), 10, cfg.n_days - 3))))
    )
    return df.select("card_key", "card_token", "customer_key", "ca_ars_account_key", "card_type", "brand", "pan_last4",
                     "issued_date", "closed_date", "credit_limit_2023", "compromised", "compromise_date")


def merchants_df(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Comercios: rubro (MCC), ciudad, país y un nombre de fantasía. ~4 % del exterior (compras en USD)."""
    s = str(cfg.seed)
    mccs = list(ref.MCC)
    weights = [v[3] for v in ref.MCC.values()]
    cities = sorted({b["city"] for b in ref.branches()})
    u = lambda salt: uniform("merchant_key", salt=f"{s}-merch-{salt}")  # noqa: E731
    foreign = u("foreign") < ref.FOREIGN_MERCHANT_SHARE
    mcc_name = F.element_at(F.create_map(*[x for k, v in ref.MCC.items() for x in (F.lit(k), F.lit(v[0]))]), F.col("mcc"))
    return (
        spark.range(1, cfg.n_merchants + 1).withColumnRenamed("id", "merchant_key")
        .withColumn("mcc", weighted_pick(u("mcc"), mccs, weights))
        .withColumn("mcc_description", mcc_name)
        .withColumn("merchant_id", F.concat(F.lit("M"), F.lpad(F.col("merchant_key").cast("string"), 8, "0")))
        .withColumn("merchant_name", F.upper(F.concat_ws(" ", pick(u("w"), ["EL", "LA", "DON", "NUEVA", "GRAN", "MI", "SAN"]),
                                                          pick(u("n"), ref.LAST_NAMES), F.col("mcc_description"))))
        .withColumn("country", F.when(foreign, pick(u("ctry"), ["US", "CL", "BR", "ES", "UY"])).otherwise("AR"))
        .withColumn("city", F.when(foreign, F.lit(None).cast("string")).otherwise(pick(u("city"), cities)))
        .withColumn("currency", F.when(foreign, "USD").otherwise("ARS"))
        .withColumn("ecommerce", (F.col("mcc").isin("5815", "7995", "4511")) | (u("ecom") < 0.15))
    )
