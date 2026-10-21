"""MARTS: tablas anchas pensadas para las preguntas de cada área (y para que Genie no tenga que hacer 5 joins).

  mart_cliente_360         Comercial     una fila por cliente vigente: productos, saldos, consumo, actividad
  mart_fraude_diario       Fraude        día × rubro × exterior: autorizaciones, rechazos por sospecha, montos
  mart_tesoreria_mensual   Tesorería     mes × moneda × segmento × región: depósitos a fin de mes
  mart_conciliacion_tarjetas  Operaciones  día: compras con débito según el procesador vs. según el core
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig


def _g(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame:
    return spark.table(cfg.table("gold", name))


def mart_cliente_360(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    end = F.lit(cfg.end_date.isoformat()).cast("date")
    cli = _g(spark, cfg, "dim_cliente").where("is_current")
    cuentas = (_g(spark, cfg, "dim_cuenta").where("estado = 'ACTIVO'").groupBy("cliente_id").agg(
        F.count("*").alias("cuentas_activas"),
        F.max(F.when(F.col("tipo_cuenta") == "CA_USD", 1).otherwise(0)).cast("boolean").alias("tiene_caja_usd"),
        F.max(F.when(F.col("tipo_cuenta") == "CC_ARS", 1).otherwise(0)).cast("boolean").alias("tiene_cuenta_corriente")))
    tarjetas = (_g(spark, cfg, "dim_tarjeta").where("estado = 'ACTIVO'").groupBy("cliente_id").agg(
        F.max(F.when(F.col("tipo_tarjeta") == "CREDITO", 1).otherwise(0)).cast("boolean").alias("tiene_tarjeta_credito"),
        F.max(F.when(F.col("tipo_tarjeta") == "DEBITO", 1).otherwise(0)).cast("boolean").alias("tiene_tarjeta_debito")))
    w = Window.partitionBy("cuenta_id").orderBy(F.desc("fecha"))
    saldos = (_g(spark, cfg, "fact_saldo_diario").withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
              .groupBy("cliente_id").agg(F.sum("saldo_ars").alias("saldo_total_ars"), F.sum("saldo_usd").alias("saldo_total_usd")))
    mov = (_g(spark, cfg, "fact_movimiento").where(F.col("fecha_contable") > F.date_sub(end, 90)).groupBy("cliente_id").agg(
        F.count("*").alias("movimientos_90d"),
        F.round(F.avg(F.col("canal_digital").cast("int")) * 100, 1).alias("porcentaje_digital_90d"),
        F.max(F.when(F.col("concepto") == "ACREDITACION_SUELDO", 1).otherwise(0)).cast("boolean").alias("cobra_sueldo"),
        F.max("fecha_contable").alias("ultimo_movimiento")))
    consumo = (_g(spark, cfg, "fact_autorizacion").where(F.col("aprobada") & (F.col("fecha") > F.date_sub(end, 90)))
               .groupBy("cliente_id").agg(F.sum("importe_ars").alias("consumo_tarjetas_90d_ars"),
                                          F.count("*").alias("compras_90d")))
    suc = _g(spark, cfg, "dim_sucursal").select("sucursal_id", "sucursal")
    out = (cli.join(suc, "sucursal_id", "left").join(cuentas, "cliente_id", "left").join(tarjetas, "cliente_id", "left")
           .join(saldos, "cliente_id", "left").join(mov, "cliente_id", "left").join(consumo, "cliente_id", "left"))
    bools = ["tiene_caja_usd", "tiene_cuenta_corriente", "tiene_tarjeta_credito", "tiene_tarjeta_debito", "cobra_sueldo"]
    out = out.fillna(False, subset=bools).fillna(0, subset=["cuentas_activas", "movimientos_90d", "compras_90d"])
    return out.withColumn("cantidad_productos", F.col("cuentas_activas") + F.col("tiene_tarjeta_credito").cast("int")
                          + F.col("tiene_tarjeta_debito").cast("int")).select(
        "cliente_id", "persona_id", "nombre_completo", "tipo_persona", "segmento", "region", "provincia", "sucursal_id",
        "sucursal", "banco_origen", "rango_edad", "fecha_alta", "estado", "cuentas_activas", "tiene_caja_usd",
        "tiene_cuenta_corriente", "tiene_tarjeta_credito", "tiene_tarjeta_debito", "cantidad_productos", "saldo_total_ars",
        "saldo_total_usd", "movimientos_90d", "porcentaje_digital_90d", "cobra_sueldo", "ultimo_movimiento",
        "consumo_tarjetas_90d_ars", "compras_90d")


def mart_fraude_diario(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    a = _g(spark, cfg, "fact_autorizacion").join(_g(spark, cfg, "dim_comercio").select("comercio_id", "rubro", "es_exterior",
                                                                                      "rubro_riesgoso"), "comercio_id", "left")
    return a.groupBy("fecha", "mcc", "rubro", "es_exterior", "rubro_riesgoso").agg(
        F.count("*").alias("autorizaciones"), F.sum(F.col("aprobada").cast("int")).alias("aprobadas"),
        F.sum(F.col("rechazo_por_sospecha").cast("int")).alias("rechazos_por_sospecha"),
        F.sum((~F.col("aprobada")).cast("int")).alias("rechazos_total"),
        F.sum("importe_ars").cast("decimal(20,2)").alias("monto_ars"),
        F.sum(F.when(F.col("aprobada"), F.col("importe_ars"))).cast("decimal(20,2)").alias("monto_aprobado_ars"),
        F.countDistinct("token_tarjeta").alias("tarjetas_distintas"),
        F.sum(F.col("llego_tarde").cast("int")).alias("informadas_tarde"))


def mart_tesoreria_mensual(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    s = _g(spark, cfg, "fact_saldo_diario").where("es_fin_de_mes")
    cli = _g(spark, cfg, "dim_cliente").where("is_current").select("cliente_id", "segmento", "region")
    return (s.join(cli, "cliente_id", "left").withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
            .groupBy("anio_mes", "fecha", "moneda", "tipo_cuenta", "segmento", "region").agg(
                F.count("*").alias("cuentas"), F.sum("saldo").cast("decimal(22,2)").alias("saldo_moneda_original"),
                F.sum("saldo_ars").cast("decimal(22,2)").alias("saldo_ars"),
                F.sum("saldo_usd").cast("decimal(22,2)").alias("saldo_usd")))


def mart_conciliacion_tarjetas(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Compras con débito aprobadas según el procesador vs. débitos COMPRA_DEBITO según el core (por auth_id)."""
    proc = (_g(spark, cfg, "fact_autorizacion").where("aprobada AND tipo_tarjeta = 'DEBITO'")
            .select("auth_id", "fecha", F.col("importe_ars").alias("importe_procesador")))
    core = (_g(spark, cfg, "fact_movimiento").where("concepto = 'COMPRA_DEBITO'")
            .select(F.col("referencia").alias("auth_id"), F.col("fecha_contable"), (-F.col("importe")).alias("importe_core")))
    j = proc.join(core, "auth_id", "full_outer")
    return (j.withColumn("fecha", F.coalesce("fecha", "fecha_contable"))
            .groupBy("fecha").agg(
                F.count("importe_procesador").alias("compras_procesador"), F.count("importe_core").alias("compras_core"),
                F.sum(F.when(F.col("importe_core").isNull(), 1).otherwise(0)).alias("solo_en_procesador"),
                F.sum(F.when(F.col("importe_procesador").isNull(), 1).otherwise(0)).alias("solo_en_core"),
                F.sum("importe_procesador").cast("decimal(20,2)").alias("monto_procesador"),
                F.sum("importe_core").cast("decimal(20,2)").alias("monto_core"))
            .withColumn("diferencia_monto", (F.col("monto_procesador") - F.col("monto_core")).cast("decimal(20,2)")))


MARTS = {"mart_cliente_360": mart_cliente_360, "mart_fraude_diario": mart_fraude_diario,
         "mart_tesoreria_mensual": mart_tesoreria_mensual, "mart_conciliacion_tarjetas": mart_conciliacion_tarjetas}


def build_marts(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    out = {}
    for name, fn in MARTS.items():
        table = cfg.table("gold", name)
        fn(spark, cfg).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        out[name] = spark.table(table).count()
    return out
