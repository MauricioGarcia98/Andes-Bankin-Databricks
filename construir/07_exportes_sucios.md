# 07 · Etapa 04 · Exportes sucios del core y del procesador

## 🎯 Objetivo

Convertir la verdad limpia (`sim.*`) en lo que **entrega** cada sistema origen, con la suciedad del
[catálogo de problemas](../docs/03_catalogo_de_problemas.md):

- el **core** manda CSV diarios, y cambia de formato el 11-mar-2024 (migración);
- el **procesador** manda un JSON diario por la integración vieja (v1) y sirve registros canónicos por la API v2.

**Archivos:** `src/andes_bank/generator/exports.py`, `tests/test_exports.py`.

![Cómo se genera el banco](../docs/diagramas/09_generador.svg)

## 📚 Conceptos

- **Dos contratos del core:**

  | | antes de mar-2024 (core viejo) | después (core nuevo) |
  |---|---|---|
  | encoding | latin1 | UTF-8 |
  | separador | `;` | `,` |
  | fechas | `dd/MM/yyyy` | ISO `yyyy-MM-dd` |
  | números | `1.234,56` | `1234.56` |
  | columnas | `NRO_CUENTA` | `cuenta_id` |
  | compresión | ninguna | gzip |

- **CDC (*change data capture*):** clientes y cuentas no se mandan enteros cada día, sino como eventos
  `I` (alta), `U` (cambio) y `D` (baja), con la foto completa del registro en cada evento. Silver arma la historia
  (SCD2) con eso (capítulo 17).
- **Fecha de entrega vs. fecha del archivo:** el archivo del 12-ago puede llegar el 15. Por eso se particiona por
  **las dos** (`delivery_date=.../file_date=...`).

## 🧱 Paso a paso

### 1. Los movimientos, ensuciados (`transaction_export`)

Cada problema es una línea, con su ID del catálogo al lado:

```python
base = (txn.where(u("miss") >= MISSING_ROW_SHARE)                                 # DAT-10: faltantes
        .withColumn("currency_out", F.when((F.col("currency") == "USD") & (u("cur") < WRONG_CURRENCY_SHARE), "ARS")
                    .otherwise(F.col("currency"))))                               # DAT-05: moneda mal
dups = (base.where(u("dup") < DUPLICATE_ROW_SHARE)                               # DAT-01: re-enviados
        .withColumn("delivery_date", F.date_add("delivery_date", 1)))             # en el archivo del día siguiente
df = (df.withColumn("delivery_date", F.when(F.col("delivery_date") == _d(late_from), _d(late_to))  # ING-04: tarde
                    .otherwise(F.col("delivery_date"))))
```

Los porcentajes están arriba del archivo (`DUPLICATE_ROW_SHARE = 0.003`, etc.) y las fechas de los incidentes también
(`LATE_FILE`, `EMPTY_FILE`, `CORRUPT_FILE`, `DUPLICATE_FILE`). Todo determinístico: con la misma semilla, los mismos
movimientos duplicados.

### 2. Los dos formatos

```python
legacy = [F.col("txn_id").alias("NRO_MOVIMIENTO"),
          F.date_format(_local(F.col("ts_utc")), "dd/MM/yyyy HH:mm:ss").alias("FECHA_HORA"),
          _ar_number(F.col("amount")).alias("IMPORTE"),                  # '1.234,56'
          F.when(F.col("currency_out") == "USD", "2").otherwise("1").alias("COD_MONEDA")]
new = [F.col("txn_id").alias("movimiento_id"),
       F.date_format(_local(F.col("ts_utc")), "yyyy-MM-dd HH:mm:ss").alias("fecha_hora"),
       F.col("signed_amount").cast("decimal(18,2)").alias("importe")]
```

`_write_csv` escribe el viejo en latin1 con `;` y sin comprimir, y el nuevo en UTF-8 con `,` y gzip. Desde ene-2025
el nuevo trae una columna más (`canal_digital`): *schema drift*.

### 3. El procesador

- **v1** (`write_cards_legacy`): un JSON por día, montos en **centavos**, fechas en **epoch ms**, estado `A`/`R`.
- **v2** (`write_api_data`): los registros canónicos, un JSONL por día. `upload.py` los parte en páginas para la API.

## 🤔 Por qué así

- **¿Por qué no ensuciar con Python, fila por fila?** Son millones de filas; con columnas de Spark y `uniform` es
  determinístico y rápido.
- **¿Por qué `decimal(18,2)` para los montos?** Porque Spark escribe los `double` en notación científica
  (`1.869293698E7`) en un CSV (R02). Un banco nunca manda eso.

## 🧪 Probalo

```bash
pytest -q tests/test_exports.py
ls <volume>/_staging/core/movimientos/   # delivery_date=2025-08-15/file_date=2025-08-12/ ...
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `saldo` como `1.869293698E7` en el CSV | Spark escribe los `double` en notación científica | `decimal(18,2)` (R02) |
| El archivo del 12-ago se mezcla con el del 15 | se particionaba sólo por fecha de entrega | particionar por entrega **y** fecha del archivo (R03) |
| Acentos rotos en el core viejo | se escribió en UTF-8 | el viejo va en latin1: lo tiene que leer así el contrato legacy |

## ✅ Punto de control

`git diff etapa-04 -- src/andes_bank/generator/exports.py`

## 🏋️ Ejercicio

Agregá un incidente nuevo: un día en que el core manda los movimientos con las columnas en **otro orden**. ¿Qué
capa del lakehouse lo detecta? (Pista: el contrato de bronze del capítulo 13.)
