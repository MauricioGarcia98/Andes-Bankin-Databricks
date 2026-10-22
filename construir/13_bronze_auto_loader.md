# 13 · Etapa 09 (II) · Bronze de archivos con Auto Loader (dos contratos)

## 🎯 Objetivo

Cargar en bronze **lo que llegó, tal cual llegó**, con linaje, de forma incremental: cada corrida procesa sólo los
archivos nuevos del Volume.

**Archivos:** `src/andes_bank/ingestion/bronze_files.py`, `notebooks/11_bronze_files.py`, `tests/test_ingestion.py`.

![Cargar sólo lo nuevo](../docs/diagramas/11_incremental.svg)

## 📚 Conceptos

- **Auto Loader (`cloudFiles`):** un stream que descubre archivos nuevos en una carpeta y guarda en un *checkpoint*
  cuáles ya leyó. Con `trigger(availableNow=True)` lee todo lo pendiente y **termina**: un batch incremental.
- **Dos contratos, dos tablas:** el core cambió de formato en mar-2024, así que cada fuente tiene
  `core_<fuente>_legacy` (latin1, `;`, mayúsculas) y `core_<fuente>` (UTF-8, gzip, snake_case). Silver las une.
- **Todo como texto:** bronze no castea. Si castea y un día llega `"1.234,56"` en una columna `double`, la fila se
  pierde en silencio. Los tipos se aplican en silver, donde se puede mandar a cuarentena.
- **Linaje:** `_source_file`, `_delivery_date` (de la carpeta `yyyy/mm/dd`) e `_ingested_at` en cada fila.

## 🧱 Paso a paso

### 1. Una fuente = un contrato (`SOURCES`)

```python
SOURCES = {s.table: s for s in [
    FileSource("core_movimientos_legacy", "core", "movimientos", "csv", columns=(...), **LEGACY),   # latin1, ';'
    FileSource("core_movimientos", "core", "movimientos", "csv", columns=(...)),
    FileSource("cards_v1_autorizaciones", "cards-legacy", "autorizaciones", "json", glob="*.json"),
    ...]}
```

### 2. El lector (`read_stream`)

```python
reader = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", src.fmt)
          .option("cloudFiles.schemaLocation", cfg.schema_location(src.table))
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # una columna nueva no rompe nada
          .option("rescuedDataColumn", "_rescued_data")                 # lo que no encaja, no se pierde
          .option("pathGlobFilter", src.glob))
if src.fmt == "csv":
    hints = ", ".join(f"`{c}` STRING" for c in src.columns)            # el contrato, todo texto
    reader = reader.option("cloudFiles.inferColumnTypes", "false").option("cloudFiles.schemaHints", hints)
```

### 3. Linaje y escritura

```python
def add_lineage(df):
    path = F.col("_metadata.file_path")
    return (df.withColumn("_source_file", path)
              .withColumn("_delivery_date", F.to_date(F.regexp_extract(path, r"/(\d{4}/\d{2}/\d{2})/[^/]+$", 1), "yyyy/MM/dd"))
              .withColumn("_ingested_at", F.current_timestamp()))

query = (add_lineage(read_stream(spark, cfg, src)).writeStream.format("delta")
         .option("checkpointLocation", cfg.checkpoint_path(f"bronze_{table}"))
         .trigger(availableNow=True).toTable(target))
```

### 4. Schema drift: la columna nueva de 2025

Desde ene-2025 los movimientos traen `canal_digital`. Auto Loader (`addNewColumns`) guarda el esquema ampliado y
**detiene** el stream con `UNKNOWN_FIELD_EXCEPTION`. Reiniciarlo retoma con el esquema nuevo:

```python
except Exception as exc:
    if "UNKNOWN_FIELD_EXCEPTION" not in str(exc) or attempt == _SCHEMA_EVOLUTION_RESTARTS:
        raise
    print(f"{table}: columnas nuevas detectadas, reiniciando el stream ({attempt + 1})")
```

## 🤔 Por qué así

- **¿Por qué no una sola tabla para los dos contratos?** Mezclarlos escondería el problema: las columnas no coinciden
  y una mitad quedaría nula. Dos tablas lo hacen **visible**; silver lo resuelve una vez.
- **¿Por qué no deduplicar en bronze?** Porque el duplicado también es un dato: silver decide cuál gana (DAT-01) y
  bronze guarda la evidencia.

## 🧪 Probalo

```bash
pytest -q tests/test_ingestion.py -k bronze_de_archivos
```

```sql
SELECT _delivery_date, count(*) FROM andes_bank.bronze.core_movimientos GROUP BY ALL ORDER BY 1 DESC LIMIT 5;
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| El stream frena con `UNKNOWN_FIELD_EXCEPTION` | columna nueva (schema drift) | reiniciar: el esquema nuevo ya quedó guardado |
| Acentos rotos (`Ã©`) en las tablas legacy | se leyó latin1 como UTF-8 | `encoding="latin1"` en el contrato legacy |
| Re-correr no trae nada nuevo aunque agregaste archivos | el checkpoint ya los "vio" con otro nombre | revisá `pathGlobFilter`; borrá el checkpoint sólo si querés re-ingerir todo |

## ✅ Punto de control

`git diff etapa-09 -- src/andes_bank/ingestion/bronze_files.py`

## 🏋️ Ejercicio

Agregá una regla de calidad que cuente filas con `_rescued_data` no nulo en `bronze.core_movimientos`. ¿Qué te
diría un número distinto de cero?
