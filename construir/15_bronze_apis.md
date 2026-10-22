# 15 · Etapa 11 · Bronze de tarjetas, FX y CRM

## 🎯 Objetivo

Las tres ingestas por API, **incrementales e idempotentes**:

- **tarjetas (API v2):** día por día desde el watermark, conciliando cada día; un día incompleto no avanza el
  watermark;
- **FX:** la API real del BCRA y el espejo, reemplazando el rango pedido;
- **CRM:** incremental por `SystemModstamp`, con borrados, MERGE por Id.

**Archivos:** `src/andes_bank/ingestion/{bronze_api,bronze_crm}.py`, `notebooks/1[2-4]_*.py`.

![Ingesta: de las fuentes a bronze](../docs/diagramas/02_ingesta.svg)

## 📚 Conceptos

- **Bronze de API = JSON crudo como texto:** cada registro se guarda tal cual vino (`raw`), más su id y metadatos. Si
  mañana la API agrega `wallet_type` (mar-2026), bronze no se rompe; silver lo ve.
- **`replace_where`:** reemplaza **sólo** las filas de una condición (los días que se volvieron a pedir). Re-ingerir
  un día lo reemplaza, no lo duplica.
- **Watermark:** "hasta dónde llegué". Para tarjetas es el último día **completo**; para el CRM, el mayor
  `SystemModstamp` visto.

![Cargar sólo lo nuevo](../docs/diagramas/11_incremental.svg)

## 🧱 Paso a paso

### 1. Tarjetas: por lotes de días, y los incompletos al final (`ingest_cards_api`)

```python
def fetch_and_write(batch):
    with ThreadPoolExecutor(max_workers=parallel_days) as pool:
        results = list(pool.map(lambda d: client.fetch_day(resource, d.isoformat(), id_field, date_param), batch))
    ...                                              # ops.api_reconciliation: esperado vs. recibido por día
    summary["rows"] += write_day_batch(spark, cfg, resource, results, client.run_id)
    return [date.fromisoformat(r.day) for r in results if not r.complete]

incomplete = []
for i in range(0, len(days), batch_days):
    incomplete += fetch_and_write(days[i: i + batch_days])
while incomplete and summary["retry_rounds"] < retry_rounds:      # hasta 3 rondas, con 4 minutos de espera
    summary["retry_rounds"] += 1
    sleep(retry_wait_s)
    incomplete = fetch_and_write(incomplete)
```

El watermark avanza sólo hasta el último día completo **sin huecos antes**. Lo que siga incompleto, la próxima
corrida lo vuelve a pedir.

![R14: el bug que encontró el quality gate](../docs/diagramas/12_dia_incompleto_r14.svg)

### 2. FX (`ingest_fx`)

Paginado de a 1.000 con `offset`. `bcra_page_getter` llama a la API real (sin token, con su propio backoff);
`mirror_page_getter` usa el `ApiClient` contra el espejo. Escribe en `bronze.fx_bcra` y `bronze.fx_mirror`.

### 3. CRM (`bronze_crm`)

```python
since = crm_watermark(spark, cfg, obj)
records = [{f: r.get(f) for f in fields} for r in fetch(obj, since)]          # >= since: con solapamiento
spark.sql(f"""MERGE INTO {table} t USING _crm_batch s ON t.record_id = s.record_id
              WHEN MATCHED AND s.system_modstamp >= t.system_modstamp THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *""")
```

`crm_api_fetcher` sigue el cursor hasta el final y concilia contra el `total` de la primera página: si quedó corto,
vuelve a recorrer.

## 🤔 Por qué así

- **¿Por qué `>=` y no `>` en el watermark del CRM?** Dos registros pueden tener el mismo `SystemModstamp`; con `>`
  te perdés el segundo. El solapamiento lo absorbe el MERGE.
- **¿Por qué no frenar en el primer día incompleto?** Era así, y en la primera corrida real dejó la ingesta cortada en
  dos días. Ver R14 en el diagrama.

## 🧪 Probalo

```bash
pytest -q tests/test_ingestion.py -k "api or incompleto"
pytest -q tests/test_e2e.py                 # incluye el CRM con un borrado en una segunda pasada
```

```sql
SELECT resource, max_by(complete, checked_at) AS ok, count(*) FROM andes_bank.ops.api_reconciliation GROUP BY ALL;
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Faltan miles de autorizaciones en el test local | `replaceWhere` ignorado en tablas Parquet | Spark local con Delta por defecto (R09) |
| El gate frena con `dias_api_completos`; faltan exactamente 1.000 filas | página salteada estable + reintentos inmediatos | rondas al final y watermark sin huecos (R14) |
| Un registro borrado en el CRM sigue en silver | la API no devolvía los borrados | `IsDeleted` incluido y filtrado recién en silver |

## ✅ Punto de control

`git diff etapa-11 -- src/andes_bank/ingestion/bronze_api.py src/andes_bank/ingestion/bronze_crm.py`

## 🏋️ Ejercicio

Agregá la ventana deslizante de API-08 (datos tardíos): además de lo nuevo, re-pedir siempre los últimos 3 días. ¿Por
qué no duplica nada?
