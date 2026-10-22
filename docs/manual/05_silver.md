# 05 · Silver

> Carpeta `src/andes_bank/silver/`. Convierte lo que llegó (texto, dos formatos, duplicados, IDs rotos, PII suelta) en
> datos confiables: tipados, deduplicados, con historia, con la identidad resuelta y con lo inservible en cuarentena.

![Silver](../diagramas/03_silver.svg)

## 1. Los cinco módulos

| módulo | qué tiene |
|---|---|
| `transforms.py` | funciones chicas, **una por problema** del catálogo, todas expresiones de Spark |
| `core.py` | qué se hace con el core: eventos CDC → SCD2, identidad, movimientos, saldos, conciliación de saldos |
| `cards.py` | tarjetas v1 y v2 → un modelo canónico; cotizaciones BCRA + espejo |
| `crm.py` | CRM: parseo, match con el core, duplicados, leads que ya son clientes, PII |
| `load.py` | **cómo** se escribe todo: reconstrucción vs. incremental, cuarentena, watermarks |

La separación "qué" (core/cards/crm) y "cómo" (load) permite testear las transformaciones con DataFrames chicos sin
tocar tablas.

## 2. `transforms.py`: una función por problema

Todas son **expresiones de columna**, sin UDFs de Python: corren igual de rápido con 17 o con 170 millones de filas.

| función | problema | qué hace |
|---|---|---|
| `ar_number(col)` | DAT-02 | `'1.234,56'` → `1234.56`. Regla: si tiene coma, el punto es separador de miles y la coma es decimal; si no, se usa tal cual (así sirve para los dos formatos). Devuelve `decimal(18,2)` |
| `parse_date(col)` | DAT-02 | prueba `yyyy-MM-dd`, `dd/MM/yyyy` y `yyyyMMdd` con `try_to_timestamp` (devuelve nulo en vez de fallar) y se queda con el primero que funcione |
| `parse_local_ts(col)` | DAT-06 | lo mismo para fecha y hora local sin offset (tres formatos) |
| `local_to_utc(col)` | DAT-06 | hora argentina → UTC |
| `only_digits(col)` | DAT-02/17 | `'20.345.678'` → `'20345678'`; `'20-12345678-6'` → `'20123456786'` |
| `normalize_name(col)` | DAT-08 | para **comparar**: mayúsculas, sin acentos, sin dobles espacios |
| `title_case(col)` | — | para **mostrar**: `'JUAN LUNA'` → `'Juan Luna'` |
| `redact_pii(col)` / `has_pii(col)` | DAT-11 | reemplaza emails y teléfonos escritos a mano por `[EMAIL]` y `[TELEFONO]` (regex); marca si había |
| `currency_from_code(col)` | DAT-03 | core viejo: `1` = ARS, `2` = USD |
| `response_code_v1(col)` | API-09 | código de 2 dígitos de la v1 → código completo de la v2 (`51` → `51_FONDOS_INSUFICIENTES`) |

## 3. `core.py`

### SCD2 a partir de eventos CDC (`scd2_from_events`)

![SCD2 e identidad](../diagramas/13_scd2_identidad.svg)

Entrada: eventos con `op` (I/U/D) y la foto completa del registro. Salida: una fila por **versión** con `valid_from`,
`valid_to`, `is_current`, `version` y `estado` (ACTIVO o BAJA).

1. `estado` = BAJA si `op = 'D'`.
2. Se calcula un **hash de los atributos** (+ estado) y se compara con el del evento anterior de la misma clave.
3. **Sólo abre versión nueva si cambió algo.** Por eso la recarga completa del core nuevo (mar-2024), que re-envía
   todos los clientes como `I`, **no** genera versiones nuevas.
4. `valid_to` = `valid_from` de la versión siguiente (`lead`); `is_current` = no hay siguiente.

**Por qué reconstruir y no MERGE:** los maestros son chicos (miles de eventos). Reconstruir la historia completa desde
bronze en cada corrida es determinístico, idempotente y no tiene los casos borde de un MERGE de SCD2 (eventos que
llegan desordenados, re-procesos).

### Clientes

- `customer_events`: une los dos contratos (renombrando el viejo al nuevo), y limpia: nombre para mostrar
  (`title_case`) y normalizado, DNI sólo dígitos (sólo personas humanas), CUIT sólo dígitos + `cuit_valido`, fechas,
  sucursal, email en minúsculas, `event_ts_utc` desde la fecha de novedad local, y **PII**: `observaciones` redactada +
  `observaciones_con_pii`. `sistema_origen` = `core_legacy` o `core`.
- **SCD mixto (bitácora R08):** el core viejo mandaba "BELEN", el nuevo "Belén". Con el nombre como atributo SCD2, la
  recarga de la migración generaba cientos de versiones falsas. Solución: para **detectar cambios** se usa
  `nombre_normalizado` (`CUSTOMER_SCD2_ATTRS`); el **nombre para mostrar** es tipo 1: todas las versiones muestran el
  último que mandó el core (`customers_history` hace un join con el más reciente).
- `customers_history`: el resultado, `silver.customers_history`.

### Identidad (`customer_xref`, DAT-08)

La misma persona con dos números de cliente (CUY- y NOA-) por la fusión de 2008.

- Regla: **mismo DNI y mismo nombre normalizado** → misma persona (sólo personas humanas vigentes; las empresas se
  identifican por CUIT).
- La persona canónica (`persona_id`) es el número de cliente **más viejo** (menor fecha de alta).
- Columnas: `cliente_id`, `persona_id`, `dni`, `clientes_de_la_persona`, `regla` (`DNI+NOMBRE` o `UNICO`).

En gold, `persona_id` permite contar **personas** además de clientes. El test e2e verifica que los pares encontrados
son exactamente los `is_legacy_duplicate` de la verdad.

### Cuentas y tarjetas

`account_events` / `accounts_history` y `card_events` / `cards_history`: mismo patrón (dos contratos → eventos →
SCD2). Las cuentas no traen hora de novedad: el evento ocurre al inicio del día de entrega. `cbu_valido` mide los CBU
de 21 dígitos (DAT-07).

### Movimientos

- `transactions_from_bronze`: unifica los dos contratos. Viejo: `DEB_CRED` + importe positivo con formato argentino →
  importe con signo. Nuevo: importe ya con signo. Fecha y hora local → UTC, fecha contable, y `canal_digital`: si el
  archivo trae la columna (desde 2025) se usa; si no, se deriva del canal (APP u HOMEBANKING). Guarda `importe_raw`
  para la cuarentena.
- `dedupe_first_arrival(df, key)` (DAT-01): por clave, **gana la primera copia que llegó** (menor fecha de entrega,
  después menor momento de ingesta y archivo).
- `fix_currency(txn, cuentas)` (DAT-05): la moneda de un movimiento es **la de su cuenta**. Si el core informó otra, se
  corrige y se marca `moneda_corregida`. `cuenta_conocida` = la cuenta existe en el maestro.

### Saldos y conciliación

- `balances_from_bronze`: unifica, parsea, y arma `balance_key = cuenta|fecha`.
- `balance_breaks(txn)` (DAT-10): cada movimiento trae el saldo posterior. Si `saldo_anterior + importe ≠ saldo`
  (tolerancia 1 centavo), entre esos dos movimientos **falta al menos uno** que el core nunca mandó. Queda en
  `silver.balance_breaks` con la diferencia y `causa_probable = MOVIMIENTO_FALTANTE`. El test e2e verifica que se
  detectan los movimientos que el exporte omitió a propósito.

## 4. `cards.py`: dos caminos, un modelo

Entre abril y junio de 2025 las autorizaciones llegan **por los dos caminos** (API-09): el mismo `auth_id` dos veces,
con formatos distintos.

- `auth_from_v2(bronze)`: `from_json(raw, AUTH_V2_SCHEMA)` con el esquema **explícito** (incluido `wallet_type`), y
  renombre a castellano.
- `auth_from_v1(bronze)`: centavos / 100 (DAT-04), epoch ms → timestamp UTC, `A`/`R` → APPROVED/DECLINED, código de 2
  dígitos → completo. La fecha de proceso es la fecha de entrega del archivo.
- `prefer_v2(df, key)`: por id, **gana la v2**; entre iguales, la primera que llegó.
- `settlements_from_v2` / `settlements_from_v1`: lo mismo para liquidaciones.
- `fx_rates(bcra, mirror)`: cotización por día y moneda. Parsea el JSON, reemplaza la coma decimal, traduce los alias
  (`U$S`, `DOL`, `U$D` → `USD`), descarta valores no positivos y filas donde la moneda pedida no coincide con la
  informada, y **prioriza el BCRA real** sobre el espejo. `fx_fuente` dice de dónde salió cada día.

## 5. `crm.py`: el CRM contra el core

- `parsed(obj)`: bronze → columnas tipadas (`_schema` por objeto) **sin los borrados**.
- `core_lookup`: clientes vigentes con su CUIT y su **persona** (`customer_xref`).
- `crm_accounts` (DAT-09/16/17): normaliza el CUIT (sólo dígitos) y el ID cargado, y matchea en orden:
  1. `ID_CORE`: el `Core_Customer_Id__c` existe en el core **y** su CUIT no contradice el de la cuenta;
  2. `CUIT`: el CUIT de la cuenta es el de un cliente del core (cubre el ID vacío, el del banco viejo y el tipeado);
  3. `SIN_MATCH`: prospectos o datos inservibles.

  `problema_id_core` dice qué tenía mal el ID (VACIO, BANCO_VIEJO, INEXISTENTE, CONTRADICE_CUIT). **Duplicadas:**
  mismo CUIT en más de una cuenta → la **principal** es la que tiene ID del core (si no, la más vieja); el resto queda
  con `es_duplicada = true` y `cuenta_principal_id`.
- `crm_contacts`, `crm_opportunities`, `crm_cases` (descripción redactada + `tenia_pii`).
- `crm_leads` (DAT-18): cruza el CUIT con el core → `ya_es_cliente`.
- `load_crm`: reconstruye `silver.crm_*` completo (el CRM es chico).

## 6. `load.py`: cómo se escribe

| tabla | estrategia | por qué |
|---|---|---|
| `customers_history`, `customer_xref`, `accounts_history`, `cards_history`, `fx_rates`, `balance_breaks`, `crm_*` | **reconstrucción** (`overwrite`) | chicas o derivadas; determinístico e idempotente |
| `transactions`, `balances` | **incremental**: sólo filas de bronze con `_ingested_at` > watermark, **MERGE insert-only** por clave | grandes; la primera copia gana (DAT-01) |
| `card_authorizations`, `card_settlements` | incremental, **MERGE con update condicional**: la v2 pisa a la v1, nunca al revés | API-09 |
| `quarantine` | append | nunca se borra nada en silencio |

Funciones:

- `_new_rows(df, wm)` y `_max_ingested(...)`: filtran por watermark y calculan el nuevo (se guarda **después** de
  escribir: si la escritura falla, la próxima corrida reprocesa).
- `_quarantine(spark, cfg, entidad, df, clave, motivo)`: las filas con motivo van a `silver.quarantine` con la **fila
  entera como JSON**. Motivos de movimientos: `sin_id`, `fecha_invalida`, `importe_invalido`, `cuenta_desconocida`.
- `load_masters`, `load_transactions`, `load_balances`, `load_card_events`, `load_balance_breaks`.
- `ensure_quarantine`: la tabla existe aunque esté vacía (las reglas y Genie la consultan).
- `load_all`: el orden importa — maestros primero (los movimientos necesitan las cuentas vigentes para corregir la
  moneda), después hechos, conciliación y CRM (que necesita `customer_xref`). Devuelve un resumen con tiempos.

Los movimientos se escriben con **Liquid Clustering** por `cuenta_id, fecha_contable` (las consultas típicas filtran
por cuenta o por fecha).
