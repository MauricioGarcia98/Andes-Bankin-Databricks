"""BRONZE de archivos: lo que aterrizó en el Volume, tal cual llegó, con Auto Loader.

Principios (los mismos de Andes Viento Austral):
  * "Lo que llegó, tal cual llegó": todo como texto; no se limpia ni se deduplica.
  * Linaje: archivo origen, fecha de entrega (de la carpeta), momento de ingesta.
  * Incremental: cada corrida procesa sólo archivos nuevos (checkpoint + trigger availableNow).

Novedad de este proyecto: el core cambió de sistema en mar-2024, así que cada fuente del core tiene DOS contratos
(dos tablas bronze): `core_<fuente>_legacy` (latin1, ';', columnas en mayúscula) y `core_<fuente>` (UTF-8, gzip,
snake_case). Silver los unifica. Mezclarlos en una sola tabla bronze escondería el problema en vez de mostrarlo.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig, is_databricks

_SCHEMA_EVOLUTION_RESTARTS = 3


@dataclass(frozen=True)
class FileSource:
    table: str            # nombre de la tabla bronze
    system: str           # carpeta del sistema en el Volume (= contenedor de Azure)
    folder: str           # carpeta de la fuente
    fmt: str              # csv | json
    glob: str             # qué archivos le tocan a este contrato
    columns: tuple[str, ...] = ()   # contrato CSV (todo STRING)
    sep: str = ","
    encoding: str = "UTF-8"
    late_columns: tuple[str, ...] = ()   # columnas que aparecen después (schema drift)


LEGACY = {"sep": ";", "encoding": "ISO-8859-1", "glob": "*.csv"}
NEW = {"sep": ",", "encoding": "UTF-8", "glob": "*.csv.gz"}

SOURCES = {
    s.table: s for s in [
        FileSource("core_clientes_legacy", "core", "clientes", "csv", columns=(
            "OPERACION", "NRO_CLIENTE", "APELLIDO_NOMBRE", "TIPO_PERSONA", "NRO_DOC", "CUIT_CUIL", "FEC_NAC", "EMAIL",
            "TELEFONO", "LOCALIDAD", "PROVINCIA", "SUCURSAL", "SEGMENTO", "FEC_ALTA", "FEC_NOVEDAD", "OBSERVACIONES"), **LEGACY),
        FileSource("core_clientes", "core", "clientes", "csv", columns=(
            "op", "cliente_id", "nombre_completo", "tipo_persona", "dni", "cuit", "fecha_nacimiento", "email", "telefono",
            "localidad", "provincia", "sucursal_id", "segmento", "fecha_alta", "fecha_novedad", "observaciones"), **NEW),
        FileSource("core_cuentas_legacy", "core", "cuentas", "csv", columns=(
            "OPERACION", "NRO_CUENTA", "NRO_CLIENTE", "TIPO_CUENTA", "COD_MONEDA", "CBU", "SUCURSAL", "FEC_APERTURA",
            "FEC_CIERRE", "LIMITE_DESCUBIERTO"), **LEGACY),
        FileSource("core_cuentas", "core", "cuentas", "csv", columns=(
            "op", "cuenta_id", "cliente_id", "tipo_cuenta", "moneda", "cbu", "sucursal_id", "fecha_apertura",
            "fecha_cierre", "limite_descubierto"), **NEW),
        FileSource("core_tarjetas_legacy", "core", "tarjetas", "csv", columns=(
            "OPERACION", "TOKEN_TARJETA", "NRO_CLIENTE", "NRO_CUENTA", "TIPO", "MARCA", "ULTIMOS4", "FEC_EMISION",
            "LIMITE"), **LEGACY),
        FileSource("core_tarjetas", "core", "tarjetas", "csv", columns=(
            "op", "token_tarjeta", "cliente_id", "cuenta_id", "tipo", "marca", "ultimos4", "fecha_emision", "limite"), **NEW),
        FileSource("core_movimientos_legacy", "core", "movimientos", "csv", columns=(
            "NRO_MOVIMIENTO", "NRO_CUENTA", "FECHA_HORA", "FECHA_CONTABLE", "CONCEPTO", "DEB_CRED", "IMPORTE", "COD_MONEDA",
            "SALDO", "CANAL", "REFERENCIA", "ARCHIVO"), **LEGACY),
        FileSource("core_movimientos", "core", "movimientos", "csv", columns=(
            "movimiento_id", "cuenta_id", "fecha_hora", "fecha_contable", "concepto", "importe", "moneda", "saldo", "canal",
            "referencia", "archivo"), late_columns=("canal_digital",), **NEW),
        FileSource("core_saldos_legacy", "core", "saldos", "csv", columns=(
            "NRO_CUENTA", "FECHA_SALDO", "SALDO_CIERRE", "COD_MONEDA"), **LEGACY),
        FileSource("core_saldos", "core", "saldos", "csv", columns=(
            "cuenta_id", "fecha_saldo", "saldo_cierre", "moneda"), **NEW),
        FileSource("cards_v1_autorizaciones", "cards-legacy", "autorizaciones", "json", glob="*.json"),
        FileSource("cards_v1_liquidaciones", "cards-legacy", "liquidaciones", "json", glob="*.json"),
    ]
}

# JSON v1: los montos en centavos son enteros grandes; la inferencia podría elegir INT en un archivo chico.
JSON_SCHEMA_HINTS = {
    "cards_v1_autorizaciones": "monto_centavos BIGINT, monto_pesos_centavos BIGINT, fecha_hora_ms BIGINT",
    "cards_v1_liquidaciones": "monto_centavos BIGINT, monto_pesos_centavos BIGINT",
}


def _local_schema(src: FileSource) -> T.StructType:
    return T.StructType([T.StructField(c, T.StringType()) for c in src.columns + src.late_columns])


def read_stream(spark: SparkSession, cfg: ProjectConfig, src: FileSource) -> DataFrame:
    path = cfg.landing_path(src.system, src.folder)
    if is_databricks():  # pragma: no cover - Auto Loader sólo existe en Databricks
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", src.fmt)
            .option("cloudFiles.schemaLocation", cfg.schema_location(src.table))
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.maxFilesPerTrigger", 200)
            .option("rescuedDataColumn", "_rescued_data")
            .option("pathGlobFilter", src.glob)
        )
        if src.fmt == "csv":
            # todo como texto: los tipos se aplican en silver (el contrato va como schemaHints, no con .schema())
            hints = ", ".join(f"`{c}` STRING" for c in src.columns)
            reader = reader.option("cloudFiles.inferColumnTypes", "false").option("cloudFiles.schemaHints", hints)
        else:
            reader = reader.option("cloudFiles.inferColumnTypes", "true")
            if src.table in JSON_SCHEMA_HINTS:
                reader = reader.option("cloudFiles.schemaHints", JSON_SCHEMA_HINTS[src.table])
    else:
        reader = spark.readStream.format(src.fmt).option("pathGlobFilter", src.glob).option("recursiveFileLookup", "true")
        if src.fmt == "csv":
            reader = reader.schema(_local_schema(src))
        else:
            reader = reader.schema(spark.read.option("recursiveFileLookup", "true").json(path).schema)
    if src.fmt == "csv":
        reader = (reader.option("header", "true").option("mode", "PERMISSIVE").option("sep", src.sep)
                  .option("encoding", src.encoding).option("multiLine", "false"))
    return reader.load(path)


def add_lineage(df: DataFrame) -> DataFrame:
    """¿De qué archivo vino, qué día se entregó (carpeta yyyy/mm/dd) y cuándo lo cargamos?"""
    path = F.col("_metadata.file_path")
    return (
        df.withColumn("_source_file", path)
        .withColumn("_delivery_date", F.to_date(F.regexp_extract(path, r"/(\d{4}/\d{2}/\d{2})/[^/]+$", 1), "yyyy/MM/dd"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest(spark: SparkSession, cfg: ProjectConfig, table: str) -> int:
    """Ingesta incremental de una fuente de archivos a bronze. Devuelve las filas nuevas."""
    src = SOURCES[table]
    target = cfg.table("bronze", table)
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
    for attempt in range(_SCHEMA_EVOLUTION_RESTARTS + 1):
        query = (
            add_lineage(read_stream(spark, cfg, src))
            .writeStream.format("delta")
            .option("checkpointLocation", cfg.checkpoint_path(f"bronze_{table}"))
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target)
        )
        try:
            query.awaitTermination()
            break
        except Exception as exc:
            # Auto Loader (addNewColumns) guarda el esquema ampliado y detiene el stream al ver una columna nueva
            # (canal_digital desde 2025): reiniciarlo retoma con el esquema nuevo.
            if "UNKNOWN_FIELD_EXCEPTION" not in str(exc) or attempt == _SCHEMA_EVOLUTION_RESTARTS:
                raise
            print(f"{table}: columnas nuevas detectadas, reiniciando el stream ({attempt + 1})")
    return spark.table(target).count() - before


def ingest_all(spark: SparkSession, cfg: ProjectConfig, tables: list[str] | None = None) -> dict[str, int]:
    out = {}
    for table in tables or list(SOURCES):
        try:
            out[table] = ingest(spark, cfg, table)
        except Exception as exc:
            if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc) or "Unable to infer schema" in str(exc):
                out[table] = 0   # la fuente todavía no mandó ningún archivo
            else:
                raise
    return out
