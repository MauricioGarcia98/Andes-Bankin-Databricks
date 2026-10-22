# 02 · El generador: la empresa en código

> Carpeta `src/andes_bank/generator/`. Fabrica un banco argentino completo (clientes, cuentas, tarjetas, comercios,
> compras, movimientos, saldos y un CRM) de forma **determinística**, lo guarda como "verdad" en `sim.*` y después lo
> **ensucia** como lo haría cada sistema origen real, para subirlo a Azure.

![El generador](../diagramas/09_generador.svg)

## 1. Para qué existe

Para aprender a limpiar datos hacen falta datos sucios **con la respuesta correcta al lado**. Un dataset público no
la tiene; un banco real no la publica. El generador produce las dos cosas:

1. **La verdad** (`sim.*`): limpia, coherente entre sistemas (la compra con débito aparece en el procesador *y* en
   el core; el pago del resumen de la tarjeta sale de la cuenta), y con las respuestas a las preguntas difíciles
   (qué persona tiene dos números de cliente, qué tarjetas sufrieron fraude).
2. **Los exportes sucios**: lo que cada sistema origen *entrega*, con los problemas del catálogo inyectados en
   proporciones conocidas.

El lakehouse nunca lee `sim`. Los tests sí: si el lakehouse recupera la verdad a partir de lo sucio, está bien.

## 2. Orden de los módulos

```
reference.py   constantes del negocio (sucursales, segmentos, rubros, inflación, dólar, feriados, hitos)
rand.py        azar determinístico como expresiones de Spark
entities.py    sucursales, meses, clientes, cuentas, tarjetas, comercios
events.py      autorizaciones → liquidaciones → movimientos (con saldo) → saldos de cierre
truth.py       orquesta: genera todo y lo guarda en sim.*
exports.py     ensucia y escribe los CSV/JSON en el Volume (_staging/)
upload.py      sube a Azure con el nombre de cada sistema; rompe archivos (vacío, truncado, duplicado); espejo BCRA
crm.py         el CRM comercial (Python puro: ~1.900 registros) y su publicación
daily.py       el día nuevo: agrega un día por día a sim.* después de la historia, lo exporta y lo sube
```

## 3. `reference.py`: el negocio en constantes

| constante | qué es | para qué sirve |
|---|---|---|
| `CORE_MIGRATION_DATE` = 2024-03-11 | migración del core | antes: CSV latin1 con `;`, fechas `dd/MM/yyyy`, montos `1.234,56`, columnas en mayúscula. Después: UTF-8, `,`, ISO, gzip, snake_case. Obliga a tener **dos contratos** en bronze (DAT-03) |
| `CRM_GO_LIVE` = 2024-06-03 | arranque del CRM comercial | fecha mínima de los registros del CRM |
| `API_V2_LAUNCH` = 2025-04-01 / `API_V1_LAST_EXPORT` = 2025-06-30 | API v2 del procesador y fin de la integración vieja | entre abril y junio de 2025 las autorizaciones llegan **por los dos caminos** (API-09) |
| `CANAL_DIGITAL_SINCE` = 2025-01-02 | columna nueva en el CSV de movimientos | schema drift de archivos (ING-05) |
| `WALLET_TYPE_SINCE` = 2026-03-01 | campo nuevo en la API | schema drift de API (API-11) |
| `DEVALUATION_DATE` = 2023-12-13 | devaluación | el dólar pasa de 366 a 808 en un mes: los montos en USD de los marts lo muestran |
| `BRANCH_CITIES`, `branches()` | 62 sucursales en Cuyo, NOA y Centro + la 999 (digital) | no escalan: son siempre 62 |
| `REGION_WEIGHTS` | Cuyo 46 %, NOA 27 %, Centro 17 %, Digital 10 % | Cuyo es el banco original (1998), NOA el adquirido (2008) |
| `FIRST_NAMES_*`, `LAST_NAMES`, `COMPANY_*`, `EMAIL_DOMAINS` | listas para nombres sintéticos | nombres creíbles, con acentos (eso genera el problema R08) |
| `SEGMENTS` / `SEGMENT_WEIGHTS` | MASIVO 62 %, PLUS 22 %, PREMIUM 8 %, PYME 8 % | PYME = persona jurídica |
| `TXN_TYPES` | tipo → (signo, mediana ARS ene-2023, sigma, peso en caja de ahorro, peso en cuenta corriente) | la mezcla y el tamaño de los movimientos |
| `SALARY_MEDIAN_ARS_2023` = 210.000, `SWEEP_INCOME_MONTHS` = 2,5 | sueldo mediano y techo de saldo | ver "plazo fijo" más abajo |
| `MCC` | rubro → (nombre, mediana, sigma, peso, riesgo de fraude relativo) | 15 rubros; joyerías y apuestas son los más riesgosos |
| `FOREIGN_MERCHANT_SHARE` = 4 % | comercios del exterior | compras en USD |
| `DECLINE_REASONS` | motivo de rechazo → probabilidad | ~6 % de rechazos |
| `MONTHLY_INFLATION`, `USD_OFFICIAL_EOM`, `monthly_index_rows()` | inflación mensual y dólar de fin de mes 2023-2026 | índice de precios (base ene-2023 = 1) que **mueve todos los montos en pesos**; el dólar sólo lo usa el generador (el lakehouse usa el del BCRA real) |
| `HOLIDAYS` | feriados nacionales 2023-2026 | `dim_fecha.es_feriado`; días sin cotización (DAT-13) |

## 4. `rand.py`: azar determinístico y distribuido

**El problema:** con Spark, `rand()` depende de cómo se reparten las filas entre executors; dos corridas dan bancos
distintos. Y un `random.Random` de Python no escala a 170 M de filas.

**La solución:** cada número al azar es un **hash** de la clave de la fila y una "sal":

```python
uniform("customer_key", salt="1998-cust-region")   # = xxhash64(customer_key, sal) llevado a [0, 1)
```

Misma fila + misma sal = mismo número, siempre, en cualquier máquina. Sales distintas = números independientes.

| función | qué devuelve | se usa para |
|---|---|---|
| `uniform(*cols, salt)` | uniforme en [0, 1) | todo |
| `uniform_sql` | lo mismo como texto SQL | dentro de lambdas (`transform`, `filter`) |
| `int_between(u, lo, hi)` | entero en [lo, hi] | edades, días, cantidades |
| `pick(u, valores)` | un valor con probabilidad uniforme | nombres, ciudades |
| `weighted_pick(u, valores, pesos)` | un valor según pesos (arma un `CASE WHEN` con los acumulados) | región, segmento, rubro, hora del día |
| `zipf_index(u, n, skew)` | índice 1..n con ley de potencias | pocos comercios concentran muchas compras, como en la realidad |
| `normal(u1, u2)` | normal estándar por Box-Muller | base de la lognormal |
| `lognormal(u1, u2, mediana, sigma)` | monto con cola larga | montos de compras y movimientos |
| `poisson_approx(u1, u2, rate)` | cantidad de eventos con media `rate` (aproximación normal, nunca negativa) | cuántas compras o movimientos tiene cada tarjeta/cuenta |
| `prng(seed, *keys)` | un `random.Random` sembrado con un crc32 de las claves | Python puro (CRM) |

## 5. `entities.py`: las entidades

Todas arrancan de `spark.range(1, n+1)` (una fila por entidad, en paralelo) y agregan columnas con `uniform`.

### `customers_df`: clientes

Paso a paso:

1. **Región** (ponderada) → **sucursal** dentro de la región (`_branch_for_region`; la región DIGITAL va a la 999) →
   **segmento** → **tipo de persona** (PYME = JURIDICA).
2. **Edad** 18-85 → **fecha de nacimiento** → **DNI correlacionado con la edad** (los jóvenes tienen DNI más altos,
   como en la realidad: `47.000.000 − edad × 520.000 + ruido`).
3. Nombre (o razón social), **CUIT/CUIL válido** con `cuit_col` / `cuit_company_col`.
4. **Antigüedad** según la historia: Cuyo desde 1998, NOA desde 2008, Centro desde 2016, Digital desde 2020. ~8 % son
   **altas dentro del período** (generan CDC de inserción). ~5 % se **dan de baja** entre 30 y 900 días después.
5. ~10 % **cambian de segmento** (MASIVO → PLUS → PREMIUM) y ~8 % **cambian de email** dentro del período: son los
   cambios que tiene que capturar el SCD2.
6. Provincia, ciudad, teléfono con característica real de la provincia, email (personas: `nombre.apellido<n>@dominio`;
   empresas: `administracion@<empresa>.com.ar`), **ingreso mensual 2023** según segmento con ruido, y si cobra el
   sueldo en el banco (60 % de las personas humanas).
7. `customer_id` con prefijo de origen: `CUY-`, `NOA-` o `AB-`.
8. **Los duplicados de la fusión (DAT-08):** un 6 % de los clientes humanos del NOA se copia como cliente de Cuyo
   (`customer_key + n`, prefijo `CUY-`, nombre **en mayúsculas y sin acentos** como lo cargaba el sistema viejo, alta
   entre 2001 y 2007, sin sueldo) con `is_legacy_duplicate = true` y el **mismo `person_id`**. Es la verdad que silver
   tiene que redescubrir con `customer_xref`.

Columnas de verdad que el core **no** exporta: `person_id`, `is_legacy_duplicate`, `has_salary`,
`monthly_income_2023`, `new_in_window`.

### `accounts_df`: cuentas

Cada cliente tiene caja de ahorro en pesos (siempre), en dólares (40 %) y cuenta corriente (70 % de PYME y PREMIUM,
menos del resto). Se arma con un array de `struct` + `explode` + filtro `has`. `account_key = customer_key × 10 + n`
(determinístico y único), `account_id = AC000…`, **CBU válido** con `cbu_col`, fecha de apertura (la cuenta en USD o la
corriente se abre hasta 700 días después del alta), límite de descubierto en la corriente.

### `cards_df`: tarjetas

Débito (85 % de las cajas en pesos, siempre VISA) y crédito (90 % PREMIUM, 60 % PLUS, 50 % PYME, menos MASIVO; 55 %
VISA, 45 % MASTERCARD). `card_token` = sha256 de la semilla y la clave (el procesador nunca ve el número de tarjeta, sólo
el token), últimos 4, límite según segmento. Un **0,4 % queda comprometido** (`compromised`) en una fecha al azar: es la
verdad del fraude.

### `merchants_df`: comercios

Rubro ponderado (`MCC`), nombre de fantasía ("LA GONZÁLEZ SUPERMERCADOS"), 4 % del exterior (país US/CL/BR/ES/UY,
moneda USD), e-commerce para servicios digitales, apuestas y aerolíneas (o 15 % al azar).

### `branches_df`, `monthly_index_df`

Las sucursales de `reference` y una tabla mes → (índice de precios, dólar oficial) que usan los eventos para inflar
montos y convertir dólares.

## 6. `events.py`: lo que pasa todos los días

El orden reproduce cómo están conectados los sistemas de un banco:

```
autorizaciones (procesador) ──► liquidaciones (procesador)
      │ compras con débito aprobadas
      ▼
movimientos del core ◄── pago del resumen de la tarjeta de crédito (día 10 del mes siguiente)
      │
      ▼
saldos de cierre (core)
```

Constantes: `TZ_OFFSET_HOURS = 3` (Argentina es UTC−3 sin horario de verano), `POSTING_CUTOFF_HOUR = 21` (lo
posterior a las 21 h va al día contable siguiente), `RISKY_MCC`, `HOUR_WEIGHTS` (casi nada de madrugada, picos al
mediodía y a la tarde).

Helpers: `_random_ts` (instante al azar entre dos fechas con la distribución horaria, devuelto en UTC),
`posting_date` (fecha contable con el corte de las 21 h).

### `authorizations_df`

1. **Compras normales:** cada tarjeta activa tiene `Poisson(1,6 × meses activos)` compras (×1,15 si es de crédito),
   con instante al azar y comercio elegido con **Zipf** (pocos comercios muy populares).
2. **Fraude:** cada tarjeta comprometida recibe una **ráfaga de 3 a 12 compras** el día del compromiso, en rubros
   riesgosos o del exterior, con montos 6 veces mayores. `is_fraud = true` (el procesador no lo informa: es verdad).
3. **Monto:** lognormal con la mediana del rubro × índice de precios del mes; las del exterior en USD y convertidas a
   pesos con el dólar del mes (`billing_amount_ars`).
4. **Respuesta:** rechazos según `DECLINE_REASONS` (débito no rechaza por límite y crédito no rechaza por fondos: se
   corrige el código), el 40 % del fraude se rechaza por sospecha.
5. `entry_mode` (contactless, chip, banda), `wallet_type` (Google/Apple Pay desde mar-2026), y **`processing_date`**:
   el 1,2 % se informa 1 a 3 días tarde (API-08, datos tardíos).

### `settlements_df`

Las aprobadas se liquidan 1 a 3 días después, salvo un 1,7 % (reversadas o nunca liquidadas: DAT-15). En restaurantes,
la mitad suma propina (hasta 15 %); las del exterior se valúan al dólar del día de liquidación.

### `transactions_df`: los movimientos del core, con saldo

Cuatro orígenes que se unen:

1. **Ingresos mensuales:** sueldo (si cobra en el banco) o transferencia recibida (70 % de los meses), entre el día 1 y
   el 5, ajustados por inflación.
2. **Movimientos propios:** `Poisson(3,5 × meses)` por cuenta (×0,25 en dólares, ×2 en corriente), con la mezcla de
   `TXN_TYPES` (otra mezcla para la corriente: más cobranzas). Monto lognormal × índice × factor de ingreso del
   cliente. Canal según el tipo (cajero, débito automático, sistema, o app/homebanking/sucursal).
3. **Compras con débito aprobadas** → débito `COMPRA_DEBITO` en la caja en pesos, con `reference = auth_id`. Esto
   permite **conciliar el procesador contra el core** (`mart_conciliacion_tarjetas`).
4. **Pago del resumen de crédito:** el día 10 del mes siguiente, por lo consumido en el mes.

Después:

- `txn_id` = hash de la semilla, cuenta, instante, tipo, monto y referencia.
- **Plazo fijo a fin de mes (bitácora R01):** al principio los saldos crecían sin techo (mediana de 12 M de pesos)
  porque entraba más plata de la que salía. Ahora, lo que excede 2,5 ingresos mensuales sale a plazo fijo. El cálculo
  no usa bucles: con `B_m` el saldo acumulado a fin del mes y `T_m` el techo, lo invertido acumulado es
  `S_m = max(0, max_{k≤m}(B_k − T_k))` (un **máximo corrido** con una ventana) y la constitución del mes es
  `S_m − S_{m−1}`.
- **Saldo inicial:** lo justo para que la caja nunca quede negativa (más un colchón de 30 % del ingreso o USD 500).
  Para las cuentas abiertas antes del período es `SALDO_INICIAL`; para las nuevas, un depósito.
- `balance_after`: suma acumulada por cuenta, ordenada por instante e id. Cada movimiento lleva el **saldo posterior**:
  así silver puede detectar movimientos faltantes (DAT-10).
- `posting_date` con el corte de las 21 h.

### `balances_df`

El saldo de cierre que informa el core: una fila por cuenta y día **con movimiento** (el último saldo del día) más
una **foto de fin de mes de todas las cuentas** (aunque no se hayan movido), propagando el último saldo con
`last(..., ignorenulls)`.

## 7. `truth.py`: generar todo

`generate_truth(spark, cfg)` escribe en orden `branches`, `months`, `customers`, `accounts`, `cards`, `merchants`,
`authorizations`, `settlements`, `transactions`, `balances`. **Cada tabla se escribe y se vuelve a leer** antes de la
siguiente: así cada paso parte de datos materializados y Spark no recalcula todo el linaje (con 150 M de filas sería
carísimo). Devuelve la cantidad de filas de cada una.

A SF 0,1 tarda unos 3,5 minutos; a SF 1, 17,7 minutos (172 M de movimientos).

Al final escribe `sim.generated_days` con el último día de la historia (`end_date`): desde ahí sigue `daily.py`.

## 8. `exports.py`: ensuciar

`sim.*` es limpio. Acá se escribe **lo que cada sistema entrega**, con la suciedad del [catálogo](../03_catalogo_de_problemas.md).

### Constantes de suciedad

| constante | valor | problema |
|---|---|---|
| `LATE_FILE` | el archivo de movimientos del 12-ago-2025 llega el 15 | ING-04/06 |
| `EMPTY_FILE` | 2-may-2025: llega vacío, reenvío al día siguiente | ING-02 |
| `CORRUPT_FILE` | 3-nov-2025: llega truncado, reenvío al día siguiente | ING-03 |
| `DUPLICATE_FILE` | 10-feb-2026: el mismo archivo dos veces con otro nombre | ING-01 |
| `DUPLICATE_ROW_SHARE` | 0,3 % de movimientos re-enviados en el archivo del día siguiente | DAT-01 |
| `MISSING_ROW_SHARE` | 0,02 % de movimientos que el core nunca manda | DAT-10 |
| `WRONG_CURRENCY_SHARE` | 0,2 % de movimientos de cuentas en USD informados como ARS | DAT-05 |
| `INVALID_CUIT_SHARE` | 0,5 % de CUIT con el dígito verificador cambiado | DAT-07 |
| `SHORT_CBU_SHARE` | 0,2 % de CBU de 21 dígitos | DAT-07 |
| `PII_NOTE_SHARE` | 3 % de clientes con teléfono y email escritos en `observaciones` | DAT-11 |

### Escritura

- `_write_csv(df, path, legacy)`: un directorio por **fecha de entrega** y, adentro, por **fecha del archivo** (casi
  siempre iguales; distintas cuando el archivo llega tarde). Legacy: latin1, `;`, sin comprimir. Nuevo: UTF-8, `,`,
  gzip. **Overwrite dinámico**: sólo se reemplazan las carpetas presentes en el DataFrame, así regenerar es
  idempotente.
  *Cómo se llegó (bitácora R03):* al principio se particionaba sólo por fecha de entrega y el archivo del 12 que llega
  el 15 se mezclaba con el del 15.
- `_split_write(df, path, legacy_cols, new_cols, drift_cols)`: antes de la migración escribe con las columnas viejas;
  después, con las nuevas; y desde `CANAL_DIGITAL_SINCE`, con la columna extra.
- `_ar_number`: `1234.5` → `"1.234,50"` (formato del core viejo). Los montos del formato nuevo van como
  `decimal(18,2)`. *Bitácora R02:* con `double`, Spark escribía `1.869293698E7`.

### Las fuentes del core

- `customer_cdc` + `write_customers`: eventos **CDC** (op I/U/D con la foto completa): la carga inicial (todos los vivos
  el 1-ene-2023), las altas, los cambios de segmento y de email, las bajas, y **la recarga completa del core nuevo** el
  día de la migración (todos los vivos otra vez como `I`; silver no tiene que duplicarlos). Suciedad: CUIT inválido,
  PII en observaciones, DNI con puntos en Cuyo (formato viejo), nombre en mayúsculas sin acentos (formato viejo).
- `write_accounts`: CDC de cuentas con CBU corto; moneda como código `1`/`2` en el formato viejo.
- `write_cards`: maestro de tarjetas (une el token con cliente y cuenta).
- `transaction_export` + `write_transactions`: faltantes, duplicados al día siguiente, moneda mal, el día que llega
  tarde, el día vacío, y la columna `canal_digital`. Formato viejo: `DEB_CRED` + importe positivo con formato
  argentino; formato nuevo: importe con signo.
- `write_balances`: saldos de cierre.

### Las fuentes del procesador

- `write_cards_legacy`: la integración **v1**, un JSON por día de proceso (hasta jun-2025), con **montos en centavos**
  (DAT-04), **epoch en milisegundos**, estado `A`/`R` y código de respuesta de 2 dígitos.
- `write_api_data`: los registros canónicos **v2** (desde abr-2025), un JSONL por día; `upload.py` los parte en páginas.
  `wallet_type` sólo aparece desde mar-2026.

`export_all(spark, cfg, only)` corre todo o sólo lo pedido (`["core/tarjetas"]`, `["api_data"]`…): el notebook 02 lo
expone como parámetro `only_sources` para regenerar una sola fuente.

## 9. `upload.py`: subir a Azure como lo haría cada sistema

Lee `_staging/` del Volume y sube a Blob con el nombre que usaría cada sistema:

```
core          clientes/2025/05/03/CLIENTES_20250503.csv.gz          (antes de mar-2024: .csv)
              movimientos/2025/08/15/MOVIMIENTOS_20250812.csv.gz    (el archivo del 12 que llegó el 15)
cards-legacy  autorizaciones/2024/01/10/autorizaciones_20240110.json
api-data      authorizations/2025/07/15/page-00001.json + _manifest.json
crm-data      Account.json, Contact.json, …
```

- `Upload`: un blob a subir con **carga perezosa** (`load`): si el blob ya existe, el contenido nunca se lee del
  Volume. Con miles de archivos es la diferencia entre segundos y media hora.
- `core_uploads`: concatena los part-files de Spark (los `.gz` se pueden concatenar tal cual) y **rompe archivos a
  propósito**, lo que Spark no puede hacer: ING-02 (sube sólo el encabezado el día del vacío y el contenido como
  `_REENVIO`), ING-03 (sube la mitad de los bytes, que pisa al completo, y el completo como `_REENVIO` al día
  siguiente), ING-01 (sube una `_copia`).
- `cards_legacy_uploads`: un JSON por día.
- `api_uploads`: parte cada día en **páginas de 1.000** + `_manifest.json` con páginas, registros y monto total (lo
  que la API usa para paginar y para los totales diarios). El manifiesto va último: si ya existe, el día entero se
  saltea.
- `upload_all(cfg, sas, …, workers=32, skip_existing=True)`: sube en paralelo con un semáforo (como mucho 2 × workers
  archivos en memoria). **`skip_existing`** evita re-subir: re-subir un archivo idéntico le cambia el etag y el
  lakehouse lo vería como "el origen cambió un archivo ya entregado" (`CHANGED`, bitácora R07). El truncado de ING-03
  espera a que termine el completo para pisarlo.
- **CRM:** `crm_uploads` + `upload_crm` (un JSON por objeto, siempre pisa: son ~2 MB y lo que importa es el
  `SystemModstamp` de cada registro).
- **Espejo del BCRA:** `fetch_bcra_series` baja la serie **real** de cotizaciones (USD, EUR, BRL) de
  `api.bcra.gob.ar`, paginada de a 1.000; `upload_fx_mirror` la deja en `fx-mirror/cotizaciones/<MONEDA>.json`.

## 10. `crm.py`: el CRM comercial

**Por qué es distinto:** el CRM **no escala** con el SF. Un equipo comercial carga a mano unos pocos miles de
registros. Son ~1.900, se arman en **Python puro** con `random.Random(semilla)` y el modelo de datos de Salesforce
(`Id`, `CreatedDate`, `SystemModstamp`, `IsDeleted`, campos custom `__c`).

**Por qué es simulado:** se intentó con una org real de Salesforce: URL de la interfaz en vez de la de la API (R15),
token de seguridad que no correspondía a la contraseña (R16), y finalmente el límite de rangos de IP que impide
habilitar las IP cambiantes de serverless (R17). HubSpot pedía un sitio web de empresa. El SaaS real no enseñaba nada
del lakehouse y trababa todo, así que se reemplazó por una fuente simulada servida por la misma API de Azure.

| función | qué hace |
|---|---|
| `crm_sample` | muestra fija de **460 clientes PYME y PREMIUM** activos y no duplicados (los que atiende un ejecutivo), ordenada por un hash con la semilla |
| `crm_records` | arma cuentas, contactos, leads (250), oportunidades (350) y casos (200) con la suciedad del catálogo y columnas `_true_*` con la verdad |
| `generate_crm` | escribe `sim.crm_<objeto>` |
| `crm_system` | lo que "tiene adentro" el CRM: `Id` con el prefijo de Salesforce (`001` Account, `003` Contact, `00Q` Lead, `006` Opportunity, `500` Case), fechas de alta y modificación sembradas, `AccountId` de los hijos, campos calculados (`IsClosed`, `IsWon`, `CaseNumber`, `ClosedDate`). Publicar dos veces da **exactamente** lo mismo: la ingesta incremental no ve cambios que no hubo |
| `crm_state` | lee `sim.crm_*` y devuelve lo publicado |

Suciedad del CRM:

- **DAT-09** `Core_Customer_Id__c`: vacío (12 %), con el número del banco viejo `MZA000123` (6 %) o con dos dígitos
  traspuestos (4 %).
- **DAT-16** 15 cuentas duplicadas: la misma empresa cargada dos veces (otro formato de nombre, sin ID del core).
- **DAT-17** CUIT con guiones (50 %), sin guiones o vacío (10 %).
- **DAT-11** el 12 % de los casos trae un teléfono o email escrito en la descripción.
- **DAT-18** leads de gente que **ya es cliente**.
- 40 **prospectos** (empresas que todavía no son clientes): no tienen match y está bien.
- Nombres en mayúsculas o sin acentos, segmento desactualizado (15 %), montos sin cargar (3 %).

## 11. `daily.py`: el día nuevo

**Para qué:** la historia termina el 20-oct-2026 (`config.HISTORY_END`). Sin esto, el job diario del lakehouse no
tendría nada nuevo que procesar. El job `andes-bank-dia-nuevo` (5:00, hora argentina) agrega cada mañana el día de
ayer: el banco "sigue vivo" y el lakehouse trabaja con datos nuevos todos los días, como en producción.

**El problema de diseño:** no se puede regenerar la historia con un `end_date` más largo. Casi todo el azar depende del
largo del período (cuántas compras tiene una tarjeta en todo el período, en qué fecha cambia de segmento un cliente):
con un día más, **cambiaría toda la historia** y los archivos ya entregados no coincidirían con los nuevos. Por eso la
historia queda congelada y el día nuevo se **agrega**.

**Cómo:** con las mismas fórmulas de `events.py` (que quedaron en funciones reutilizables: `enrich_authorizations`,
`income_rows`, `own_movements`, `debit_purchases`, `credit_card_payments`, `txn_sign`, `txn_id_col`). La refactorización
se verificó con un hash de las 10 tablas de `sim`: la historia generada es **idéntica** a la de antes.

| función | qué hace |
|---|---|
| `last_generated_day`, `days_to_generate` | el último día en `sim.generated_days` (o `HISTORY_END`) y los que faltan hasta `until` (ayer) |
| `ensure_months` | agrega a `sim.months` los meses nuevos; `reference.monthly_index_rows` repite la inflación y el dólar del último año conocido después de 2026 |
| `daily_authorizations` | cada tarjeta activa: **4 ensayos de Bernoulli** con probabilidad `1,6 / 30,4 / 4` (≈ Poisson con media chica: la aproximación normal de la historia sirve para medias grandes, no para 0,05 por día). Con probabilidad 0,4 % anual, a una tarjeta le roban los datos ese día (ráfaga de 3 a 12 compras en rubros riesgosos). Después, `enrich_authorizations`, igual que la historia. El `i` de los eventos nuevos arranca en 1.000.000 para no chocar con los ids de la historia |
| `daily_settlements` | las compras aprobadas de 1 a 3 días antes cuya liquidación cae hoy: `settlements_df` con `end_date = hoy`, filtrado al día. Así se liquidan también las compras del 18 al 20-oct que la historia dejó pendientes |
| `daily_transactions` | sueldo (`income_rows` con el mes del día: cae sólo si hoy es el día que le toca a esa cuenta), movimientos propios (Bernoulli), compras con débito del día, pago del resumen (si es día 10), **cobertura** (si los débitos del día superan el saldo de una caja de ahorro, entra una transferencia al empezar el día: la historia lo evita con el saldo inicial, el día nuevo no puede) y **plazo fijo** a fin de mes (lo que excede 2,5 ingresos del saldo real). El saldo posterior sigue desde el último saldo de cada cuenta |
| `daily_balances` | cuentas con movimiento contable ese día + la foto de fin de mes de todas las cuentas abiertas |
| `generate_day` | **idempotente**: primero borra de `sim.*` lo que haya de ese día (hora argentina), después agrega autorizaciones, liquidaciones, movimientos y saldos, en ese orden |
| `run_new_days` | para cada día que falta: generar → `exports.export_all(day=...)` (sólo lo que se entrega ese día, con overwrite dinámico) → `upload_all(start=end=día)` → `record_generated_day`. Un día se marca como generado recién después de subirlo |

**Qué no cambia:** clientes, cuentas, tarjetas, comercios y el CRM quedan como al final de la historia (no hay altas,
bajas ni CDC nuevos). Sin novedades, el core no manda archivo de clientes ese día.

**Tests:** `tests/test_daily.py` genera una historia corta (jul a sep-2026), le agrega 11 días (cruza un fin de mes y un
día 10) y verifica: cantidades parecidas a las de la historia, ids únicos, **cada saldo = saldo anterior + importe**
también en el cambio de historia a día nuevo, cajas nunca negativas, el pago del resumen, la foto de fin de mes, las
liquidaciones de compras de la historia, que regenerar un día da lo mismo y que se exporta y sube sólo ese día.
