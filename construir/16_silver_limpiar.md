# 16 · Etapa 12 · Silver I: limpiar (números argentinos, monedas, husos, PII)

## 🎯 Objetivo

Que cada problema del catálogo tenga **una función** que lo arregla, escrita como expresión de Spark (sin UDFs):
números con coma, fechas en tres formatos, hora local sin offset, monedas mal informadas, PII escrita a mano. Y que
lo que no se pueda arreglar vaya a **cuarentena** con su motivo, nunca a la basura en silencio.

**Archivos:** `src/andes_bank/silver/transforms.py`, `src/andes_bank/silver/core.py` (movimientos y saldos),
`src/andes_bank/silver/load.py`.

![Silver: un arreglo por problema del catálogo](../docs/diagramas/03_silver.svg)

## 📚 Conceptos

- **Una función, un problema:** `ar_number` es DAT-02, `parse_local_ts` es DAT-06, `redact_pii` es DAT-11. El ID va
  en el docstring: desde el catálogo llegás al código y al revés.
- **Sin UDFs:** una expresión de Spark corre en la JVM, vectorizada. Con 17 o 170 millones de filas tarda lo mismo
  por fila; una UDF de Python no.
- **Cuarentena:** `silver.quarantine` guarda la entidad, la clave, el **motivo** y la fila entera como JSON. Se puede
  reprocesar, contar y preguntarle a Genie.
- **Hora local vs. UTC:** se guarda `*_utc` para calcular y `*_local` (America/Argentina/Buenos_Aires, sin horario de
  verano) para el negocio.

## 🧱 Paso a paso

### 1. Números argentinos (DAT-02)

```python
def ar_number(col: Column) -> Column:
    """'1.234,56' → 1234.56 (DAT-02). También acepta '1234.56' (formato nuevo) sin romperlo.

    Regla: si tiene coma, el punto es separador de miles y la coma es decimal. Si no tiene coma, se usa tal cual.
    """
    s = F.trim(col.cast("string"))
    return F.when(s.contains(","), F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")) \
        .otherwise(s).cast("decimal(18,2)")
```

### 2. Fechas y horas (DAT-06)

`parse_date` prueba `dd/MM/yyyy`, `yyyy-MM-dd` y `yyyyMMdd` con `coalesce`; `parse_local_ts` interpreta la hora del
core como local y `local_to_utc` la pasa a UTC.

### 3. Movimientos: dos contratos → una tabla

```python
def transactions_from_bronze(spark, cfg):
    # core viejo: NRO_MOVIMIENTO, IMPORTE '1.234,56', COD_MONEDA 1/2, DEB_CRED C/D
    # core nuevo: movimiento_id, importe con signo, moneda ARS/USD
    ...                                           # las mismas columnas y tipos → unionByName
def dedupe_first_arrival(df, key):
    """Gana la primera copia que llegó (DAT-01): menor fecha de entrega, después menor momento de ingesta."""
    w = Window.partitionBy(key).orderBy("_delivery_date", "_ingested_at", "_source_file")
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
```

`fix_currency` (DAT-05) corrige la moneda a la de la cuenta y deja `moneda_corregida = true`.

### 4. Cuarentena (`load._quarantine`)

```python
q = (df.withColumn("motivo", reason).where(F.col("motivo").isNotNull())
     .select(F.lit(entity).alias("entidad"), F.col(key).cast("string").alias("clave"), "motivo",
             F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("fila"),
             F.current_timestamp().alias("_quarantined_at")))
```

`ensure_quarantine` crea la tabla vacía aunque no haya nada: las reglas y Genie la consultan igual.

### 5. Hechos incrementales

Movimientos y saldos son grandes: se procesa sólo lo nuevo de bronze (watermark por `_ingested_at`) y se escribe con
**MERGE insert-only** (`utils.delta.merge_insert_only`): la primera copia que llegó gana, re-correr no duplica.

## 🤔 Por qué así

- **¿Por qué el ID del catálogo en cada función?** Porque el día que un dato sale mal, la pregunta es "¿qué problema
  del catálogo es y dónde se arregla?". La respuesta tiene que estar a un `grep` de distancia.
- **¿Por qué no borrar las filas inválidas?** Una fila borrada no se puede contar ni explicar. En cuarentena es
  evidencia, y el quality gate la mide.

## 🧪 Probalo

```bash
pytest -q tests/test_e2e.py -k "movimientos"      # silver recupera la verdad: sin duplicados, importes y horas exactos
```

```sql
SELECT entidad, motivo, count(*) FROM andes_bank.silver.quarantine GROUP BY ALL;
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Importes 100 veces más grandes | se sacó la coma decimal como si fuera de miles | `ar_number` sólo trata `.` como miles si hay `,` |
| Movimientos de las 23 h en el día siguiente | se trató la hora local como UTC | `parse_local_ts` + `local_to_utc` (DAT-06) |
| Duplicados que "vuelven" en cada corrida | MERGE con UPDATE en vez de insert-only | `merge_insert_only`: gana la primera llegada |

## ✅ Punto de control

`git diff etapa-12 -- src/andes_bank/silver/transforms.py`

## 🏋️ Ejercicio

Agregá a `transforms.py` una función que normalice teléfonos argentinos (`0261 15-555-1234` → `+5492615551234`) y una
regla de calidad que cuente los que no se pudieron normalizar.
