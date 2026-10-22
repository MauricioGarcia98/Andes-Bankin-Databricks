# 12 · El recorrido de un dato

> Para ver cómo encajan todas las piezas, seguimos dos datos desde que "pasan" en el banco hasta que Genie los cuenta:
> **una compra con tarjeta de débito** y **el movimiento que esa compra genera en la cuenta**. Cada paso dice qué
> archivo del repo lo hace.

![El panorama](../diagramas/00_panorama.svg)

## Paso 0 · Pasa en el banco (generador → `sim`)

El 15 de julio de 2025 a las 13:40 (hora argentina), una clienta de Mendoza (`CUY-0001234`, segmento PLUS) paga
ARS 18.500 en un supermercado con su tarjeta de débito.

- `entities.customers_df` la creó (región CUYO, sucursal de Mendoza, CUIL válido, ingreso según segmento) y
  `accounts_df` le dio una caja de ahorro en pesos con CBU válido; `cards_df`, una tarjeta de débito con su
  `card_token`.
- `events.authorizations_df` generó la compra: `Poisson` decidió cuántas compras tiene su tarjeta, `_random_ts` el
  instante (en UTC: 16:40), `zipf_index` el comercio, `lognormal` el monto (mediana del rubro × índice de precios de
  julio de 2025). Respuesta: `00_APROBADA`. `processing_date` = 2025-07-15 (no llegó tarde).
- `events.transactions_df` generó el débito en la caja de ahorro: `COMPRA_DEBITO`, `channel = TARJETA_DEBITO`,
  `reference = auth_id`, con el **saldo posterior** calculado por la ventana acumulada.
- `events.settlements_df` liquidó la compra el 17 de julio.
- `truth.generate_truth` guardó todo en `sim.authorizations`, `sim.transactions`, `sim.settlements`. **Esta es la
  verdad.**

## Paso 1 · Cada sistema lo entrega a su manera (exportes → Azure)

- **La autorización** está después del lanzamiento de la API v2, así que `exports.write_api_data` la escribe en el
  JSONL del día. `upload.api_uploads` la mete en `api-data/authorizations/2025/07/15/page-00004.json` (páginas de 1.000)
  y escribe el `_manifest.json` del día con la cantidad y el monto total.
- **El movimiento** va al CSV del core: `exports.transaction_export` decide si es uno de los 0,02 % que nunca se mandan
  (no) o de los 0,3 % que se re-envían al día siguiente (supongamos que sí: aparece en el archivo del 15 **y** en el del
  16). Después de la migración y desde 2025, el formato es nuevo: UTF-8, gzip, importe con signo (`-18500.00`) y la
  columna `canal_digital = N`. `upload.core_uploads` lo sube como
  `core/movimientos/2025/07/15/MOVIMIENTOS_20250715.csv.gz`.

Todo esto lo corre el job `andes-bank-fuentes` (notebooks 01, 02, 03).

## Paso 2 · El lakehouse lo va a buscar (landing y bronze)

A las 7:00 arranca `andes-bank-lakehouse`.

**El movimiento:**

1. `landing_sync.run_sync` lista el contenedor `core` con la SAS de lectura, ve el blob nuevo (etag desconocido), lo
   baja, lo descomprime (`inspect`: no está corrupto, tiene filas) y lo copia a
   `/Volumes/andes_bank/landing/raw/core/movimientos/2025/07/15/`. Fila en `ops.landing_files` con estado `LANDED`.
2. `bronze_files.ingest("core_movimientos")`: Auto Loader ve el archivo nuevo (el `*.csv.gz` lo asigna al contrato
   nuevo), lo lee **todo como texto** y agrega `_source_file`, `_delivery_date = 2025-07-15` e `_ingested_at`. Al día
   siguiente entra la copia re-enviada, con `_delivery_date = 2025-07-16`. **Bronze tiene las dos copias**: lo que
   llegó, tal cual.

**La autorización:**

1. `bronze_api.days_to_fetch`: el watermark de `authorizations` está en el 14, así que toca el 15.
2. `ApiClient.fetch_day`: pide `daily-totals` (digamos 6.412 registros), recorre el cursor. En la página 3 recibe un
   **429**: espera el `Retry-After`. En la 5, el token vence: lo renueva y repite la misma página. La página 4 —la de
   nuestra compra— la API la **saltea** (caos estructural). Al final hay 5.412 de 6.412: día incompleto.
3. La corrida sigue con los demás días. Al final, `ingest_cards_api` espera 4 minutos y vuelve a pedir el 15: la
   ventana de caos cambió, la página 4 llega. 6.412 de 6.412.
4. `write_day_batch` reemplaza el día en `bronze.cards_api_authorizations` (`replace_where`): una fila con el JSON
   crudo. `ops.api_reconciliation` tiene dos filas para el día (incompleto en la primera ronda, completo en la
   segunda); `ops.api_calls`, cada pedido, incluidos el 429 y el 401. El watermark avanza.

## Paso 3 · Silver lo limpia

**El movimiento** (`silver.load.load_transactions`, incremental por `_ingested_at`):

1. `core.transactions_from_bronze`: `ar_number` lee `-18500.00` (sin coma: tal cual), `parse_local_ts` + `local_to_utc`
   convierten la hora local a UTC (16:40), `parse_date` la fecha contable, `canal_digital` = falso.
2. `core.dedupe_first_arrival`: las dos copias tienen el mismo `movimiento_id`; **gana la del 15** (primera entrega).
3. `core.fix_currency`: la cuenta es en pesos y el movimiento dice ARS: nada que corregir.
4. No tiene motivo de cuarentena → `merge_insert_only` lo inserta en `silver.transactions`. Si mañana llegara otra copia,
   el MERGE no la insertaría: ya existe.
5. `core.balance_breaks`: saldo anterior + (−18.500) = saldo informado: no hay quiebre.

**La autorización** (`load_card_events`): `cards.auth_from_v2` parsea el JSON con el esquema explícito y renombra a
castellano; `prefer_v2` no tiene competencia (después de junio de 2025 no hay v1); `merge_upsert_when` la inserta en
`silver.card_authorizations`.

**La clienta** (`load_masters`): `customers_history` la tiene con su historia (si cambió de segmento, dos versiones) y
`customer_xref` le asigna su `persona_id` (si tuviera un número del NOA viejo, los dos apuntarían a la misma persona).

## Paso 4 · Gold lo modela

- `gold.model.fact_autorizacion`: la autorización + su liquidación (liquidada, 2 días después, diferencia 0) + la
  clienta (por la tarjeta) + `hora_local = 13` + `importe_usd` = 18.500 / dólar oficial del 15-jul (de `dim_fecha`,
  que viene del BCRA real).
- `gold.model.fact_movimiento`: el movimiento + cuenta + cliente + persona + `importe_ars = -18.500` +
  `importe_usd`.
- `gold.marts.mart_conciliacion_tarjetas`: para el 15-jul, la compra aparece en el procesador **y** en el core (se unen
  por `auth_id` = `referencia`): no suma a `solo_en_procesador`.
- `gold.marts.mart_cliente_360`: suma la compra a `consumo_tarjetas_90d_ars` y a `compras_90d` de la clienta (si cae en
  los últimos 90 días del período).
- `gold.marts.mart_fraude_diario`: suma 1 a las autorizaciones del 15-jul en Supermercados, nacional, no riesgoso.

## Paso 5 · Gobierno, semántica y calidad

- `governance_semantic` vuelve a poner comentarios, claves y máscaras: un analista de Fraude que mira `dim_cliente` ve
  el CUIL de la clienta como `*****456` y, si su región asignada no es CUYO, **no ve la fila**.
- `semantic.v_autorizaciones` y `mv_tarjetas` exponen la compra con rubro, segmento y región.
- `quality_gate`: `autorizacion_unica` (no está duplicada), `dias_api_completos` (el 15-jul quedó completo en la
  segunda ronda: pasa), `movimiento_unico` (la copia re-enviada no entró), `moneda_de_la_cuenta`, `hora_en_utc`… Todas
  las bloqueantes pasan: el job termina en verde y la corrida queda en `ops.dq_results`.

Si el día 15 **no** se hubiera completado en ninguna ronda, `dias_api_completos` habría fallado, el job habría
terminado en rojo con un mail, y Genie seguiría viendo los datos de la corrida anterior. El watermark no habría
avanzado: la corrida siguiente lo vuelve a pedir.

## Paso 6 · Genie lo cuenta

El jefe de Prevención de Fraude le pregunta al agent **Tarjetas y fraude**:

> *¿Cuánto se consumió con débito en supermercados en julio de 2025?*

Genie busca en lo que tiene: la metric view `mv_tarjetas` tiene la medida `Consumo aprobado (ARS)` y las dimensiones
`Rubro`, `Tipo de tarjeta` y `Mes`. Genera algo como:

```sql
SELECT MEASURE(`Consumo aprobado (ARS)`)
FROM andes_bank.semantic.mv_tarjetas
WHERE `Rubro` = 'Supermercados' AND `Tipo de tarjeta` = 'DEBITO' AND `Mes` = '2025-07'
```

y nuestros ARS 18.500 están adentro de ese número. La gerenta Comercial, en cambio, le pregunta al agent **Comercial**
por la clienta y Genie usa la función certificada `fn_resumen_cliente('CUY-0001234')`, que lee `mart_cliente_360`.

## El mismo recorrido, en una tabla

| paso | dónde | archivo del repo | problema del catálogo que se cruzó |
|---|---|---|---|
| pasa | `sim.*` | `generator/entities.py`, `events.py`, `truth.py` | — |
| se entrega | Azure Blob / API | `generator/exports.py`, `upload.py`, `azure/card_api/` | DAT-01 (re-envío), API-01/04/06 (429, token, página salteada) |
| aterriza | Volume + `ops.landing_files` | `ingestion/landing_sync.py` | ING-01/02/03 (si hubiera pasado) |
| bronze | `bronze.core_movimientos`, `bronze.cards_api_authorizations` | `ingestion/bronze_files.py`, `api_client.py`, `bronze_api.py` | API-05/06 (dedupe y conciliación), R14 (rondas) |
| silver | `silver.transactions`, `silver.card_authorizations` | `silver/transforms.py`, `core.py`, `cards.py`, `load.py` | DAT-01, DAT-02, DAT-05, DAT-06, API-09 |
| gold | `fact_*`, `mart_*` | `gold/model.py`, `marts.py` | DAT-13 (dólar del día) |
| gobierno | máscaras, filtros | `sql/governance/` | DAT-11 |
| calidad | `ops.dq_results` | `quality/gate.py` | todas las reglas |
| Genie | metric view, función | `sql/semantic/`, `genie/src/` | — |
