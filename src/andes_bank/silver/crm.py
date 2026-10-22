"""SILVER del CRM (Salesforce): parseo, match con el core, duplicados y PII.

Match de una cuenta de Salesforce con el cliente del core (DAT-09), en orden:
  1. `ID_CORE`: `Core_Customer_Id__c` existe en el core y su CUIT no contradice el de la cuenta.
  2. `CUIT`: el CUIT de la cuenta (sin guiones) es el de un cliente del core. Cubre el ID vacío, el número del banco
     viejo (`MZA…`) y el ID con dígitos cambiados.
  3. `SIN_MATCH`: prospectos, o cuentas sin ID ni CUIT útil.
El cliente encontrado se lleva a su **persona** (`customer_xref`): una persona con dos números de cliente es una sola.

Duplicadas (DAT-16): mismo CUIT en más de una cuenta → se queda como principal la que tiene ID del core (y si no,
la más vieja); el resto queda con `es_duplicada = true` y apunta a la principal.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig
from andes_bank.ingestion.bronze_salesforce import AUDIT, OBJECTS
from andes_bank.silver.transforms import has_pii, normalize_name, only_digits, redact_pii

NUMERIC = {"Amount"}
BOOLEAN = {"IsDeleted", "IsConverted", "IsClosed", "IsWon"}


def _schema(obj: str) -> T.StructType:
    def typ(f):
        if f in BOOLEAN:
            return T.BooleanType()
        if f in NUMERIC:
            return T.DoubleType()
        return T.StringType()
    return T.StructType([T.StructField(f, typ(f)) for f in AUDIT + OBJECTS[obj][1]])


def _ts(col: Column) -> Column:
    return F.to_timestamp(F.substring(col, 1, 19), "yyyy-MM-dd'T'HH:mm:ss")


def parsed(spark: SparkSession, cfg: ProjectConfig, obj: str) -> DataFrame | None:
    """Bronze → columnas tipadas, sin los borrados."""
    table = cfg.table("bronze", OBJECTS[obj][0])
    if not spark.catalog.tableExists(table):
        return None
    df = spark.table(table).select(F.from_json("raw", _schema(obj)).alias("r")).select("r.*").where("NOT IsDeleted")
    return df.withColumn("CreatedDate", _ts(F.col("CreatedDate"))).withColumn("SystemModstamp", _ts(F.col("SystemModstamp")))


def core_lookup(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Clientes vigentes del core con su persona: (cliente_id, cuit, persona_id)."""
    h = spark.table(cfg.table("silver", "customers_history")).where("is_current").select("cliente_id", "cuit")
    xref = spark.table(cfg.table("silver", "customer_xref")).select("cliente_id", "persona_id")
    return h.join(xref, "cliente_id", "left").withColumn("persona_id", F.coalesce("persona_id", "cliente_id"))


def crm_accounts(accounts: DataFrame, core: DataFrame) -> DataFrame:
    a = (accounts
         .withColumn("cuit", F.nullif(only_digits(F.col("CUIT__c")), F.lit("")))
         .withColumn("core_id_cargado", F.nullif(F.upper(F.trim("Core_Customer_Id__c")), F.lit(""))))
    by_id = core.select(F.col("cliente_id").alias("core_id_cargado"), F.col("cuit").alias("_cuit_del_id"),
                        F.col("persona_id").alias("_persona_por_id"))
    # un CUIT → una persona (los duplicados de la fusión comparten CUIT y persona)
    by_cuit = core.where("cuit IS NOT NULL").groupBy("cuit").agg(F.min("persona_id").alias("_persona_por_cuit"))
    m = a.join(by_id, "core_id_cargado", "left").join(by_cuit, "cuit", "left")
    id_ok = F.col("_persona_por_id").isNotNull() & (F.col("cuit").isNull() | (F.col("cuit") == F.col("_cuit_del_id")))
    m = (m.withColumn("metodo_match", F.when(id_ok, "ID_CORE").when(F.col("_persona_por_cuit").isNotNull(), "CUIT")
                      .otherwise("SIN_MATCH"))
          .withColumn("persona_id", F.when(id_ok, F.col("_persona_por_id")).otherwise(F.col("_persona_por_cuit")))
          .withColumn("problema_id_core", F.when(F.col("core_id_cargado").isNull(), "VACIO")
                      .when(F.col("core_id_cargado").startswith("MZA"), "BANCO_VIEJO")
                      .when(F.col("_persona_por_id").isNull(), "INEXISTENTE")
                      .when(~id_ok, "CONTRADICE_CUIT")))
    w = Window.partitionBy("cuit").orderBy(F.col("core_id_cargado").isNull(), "CreatedDate", "Id")
    m = (m.withColumn("_rn", F.when(F.col("cuit").isNotNull(), F.row_number().over(w)).otherwise(1))
          .withColumn("cuenta_principal_id", F.when(F.col("cuit").isNotNull(), F.first("Id").over(w)).otherwise(F.col("Id")))
          .withColumn("es_duplicada", F.col("_rn") > 1))
    return m.select(
        F.col("Id").alias("cuenta_crm_id"), F.trim("Name").alias("nombre"), normalize_name(F.col("Name")).alias("nombre_normalizado"),
        F.col("Type").alias("tipo"), F.col("Industry").alias("industria"), F.col("BillingState").alias("provincia"),
        "cuit", "core_id_cargado", "persona_id", "metodo_match", "problema_id_core", "es_duplicada", "cuenta_principal_id",
        F.col("Segmento__c").alias("segmento_crm"), F.col("Andes_Key__c").alias("clave_carga"),
        F.col("CreatedDate").alias("creada_utc"), F.col("SystemModstamp").alias("modificada_utc"))


def crm_contacts(contacts: DataFrame) -> DataFrame:
    return contacts.select(
        F.col("Id").alias("contacto_id"), F.col("AccountId").alias("cuenta_crm_id"),
        F.concat_ws(" ", "FirstName", "LastName").alias("nombre"), F.lower("Email").alias("email"),
        F.col("Phone").alias("telefono"), F.col("Title").alias("cargo"), F.col("CreatedDate").alias("creado_utc"))


def crm_leads(leads: DataFrame, core: DataFrame) -> DataFrame:
    cuits = core.where("cuit IS NOT NULL").groupBy("cuit").agg(F.min("persona_id").alias("persona_id"))
    return (leads.withColumn("cuit", F.nullif(only_digits(F.col("CUIT__c")), F.lit("")))
            .join(cuits, "cuit", "left")
            .select(F.col("Id").alias("lead_id"), F.concat_ws(" ", "FirstName", "LastName").alias("nombre"),
                    F.col("Company").alias("empresa"), F.lower("Email").alias("email"), F.col("LeadSource").alias("origen"),
                    F.col("Status").alias("estado"), "cuit", "persona_id",
                    F.col("persona_id").isNotNull().alias("ya_es_cliente"),              # DAT-18
                    F.col("CreatedDate").alias("creado_utc")))


def crm_opportunities(opps: DataFrame) -> DataFrame:
    return opps.select(
        F.col("Id").alias("oportunidad_id"), F.col("AccountId").alias("cuenta_crm_id"), F.col("Name").alias("nombre"),
        F.col("Producto__c").alias("producto"), F.col("StageName").alias("etapa"), F.col("IsClosed").alias("cerrada"),
        F.col("IsWon").alias("ganada"), F.to_date("CloseDate").alias("fecha_cierre"),
        F.col("Amount").cast("decimal(18,2)").alias("monto_ars"), F.col("CreatedDate").alias("creada_utc"))


def crm_cases(cases: DataFrame) -> DataFrame:
    return cases.select(
        F.col("Id").alias("caso_id"), F.col("CaseNumber").alias("numero"), F.col("AccountId").alias("cuenta_crm_id"),
        F.col("Subject").alias("asunto"), redact_pii(F.col("Description")).alias("descripcion"),
        has_pii(F.col("Description")).alias("tenia_pii"), F.col("Status").alias("estado"),
        F.col("Priority").alias("prioridad"), F.col("Origin").alias("canal"), F.col("IsClosed").alias("cerrado"),
        F.col("CreatedDate").alias("creado_utc"), _ts(F.col("ClosedDate")).alias("cerrado_utc"))


def load_crm(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    """Reconstruye silver.crm_* (el CRM es chico: overwrite completo en cada corrida)."""
    src = {obj: parsed(spark, cfg, obj) for obj in OBJECTS}
    if src["Account"] is None:
        return {}
    core = core_lookup(spark, cfg)
    out = {
        "crm_accounts": crm_accounts(src["Account"], core),
        "crm_contacts": crm_contacts(src["Contact"]) if src["Contact"] is not None else None,
        "crm_leads": crm_leads(src["Lead"], core) if src["Lead"] is not None else None,
        "crm_opportunities": crm_opportunities(src["Opportunity"]) if src["Opportunity"] is not None else None,
        "crm_cases": crm_cases(src["Case"]) if src["Case"] is not None else None,
    }
    counts = {}
    for name, df in out.items():
        if df is None:
            continue
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("silver", name))
        counts[name] = spark.table(cfg.table("silver", name)).count()
    return counts
