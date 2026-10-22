# 02 · Fuentes del MVP

Período simulado: **2023-01-01 → hoy**, con una corrida diaria que genera el día nuevo. Volúmenes a **SF 1**; a
SF 0,1 todo es ~10 veces menor (salvo el CRM y FX, que no escalan).

## Resumen

| fuente | sistema origen | dónde vive | formato | frecuencia | volumen SF 1 |
|---|---|---|---|---|---|
| `core_customers` | core bancario | Azure Blob `landing/core/customers/` | CSV CDC (op I/U/D) | diario | ~500 mil clientes |
| `core_accounts` | core bancario | Azure Blob `landing/core/accounts/` | CSV CDC | diario | ~800 mil cuentas |
| `core_transactions` | core bancario | Azure Blob `landing/core/transactions/` | Parquet (desde 2022) / CSV `latin1` (antes) | diario | **~150 M movimientos** |
| `core_balances` | core bancario | Azure Blob `landing/core/balances/` | CSV | diario (saldo de cierre) | ~800 mil por día (muestreado) |
| `card_authorizations` | **API** del procesador (Azure Functions) | `GET /v1/authorizations`, `GET /v2/authorizations` | JSON paginado | casi tiempo real | **~60 M** |
| `card_settlements` | **API** del procesador | `GET /v2/settlements` | JSON paginado | diario | ~55 M |
| `crm_account`, `crm_contact` | **API** del CRM comercial (Azure Functions) | `GET /crm/v1/<Objeto>` | JSON paginado | incremental por `SystemModstamp` | ~1.100 |
| `crm_lead`, `crm_opportunity`, `crm_case` | **API** del CRM comercial | `GET /crm/v1/<Objeto>` | JSON paginado | incremental | ~800 |
| `fx_bcra` | **API pública real del BCRA** (estadísticas cambiarias) | internet | JSON | días hábiles | ~1.000 filas |
| `fx_mirror` | espejo simulado de la API del BCRA, con caos | Azure Functions | JSON | días hábiles | ~1.000 filas |

> **El CRM no escala**: lo carga a mano el equipo comercial, así que tiene miles de registros, no millones. Es realista: el CRM de un banco tiene muchos menos
> registros que el core. El volumen lo ponen el core y las tarjetas. El *match* CRM ↔ core se hace sobre una muestra
> de clientes.

## Core bancario (archivos en Azure Blob)

- **Clientes**: persona humana o jurídica, DNI/CUIT, nombre, fecha de nacimiento, sucursal, segmento, email, teléfono.
  CDC: altas, cambios de domicilio/segmento, bajas.
- **Cuentas**: tipo (CA ARS, CA USD, CC), moneda, CBU (22 dígitos con dígito verificador real), fecha de apertura,
  estado.
- **Movimientos**: `txn_id`, cuenta, fecha-hora, tipo (depósito, extracción, transferencia, débito automático, compra
  con débito, acreditación de sueldo, intereses, comisiones), monto, moneda, saldo posterior, canal.
- **Saldos de cierre**: lo que el core dice que tiene cada cuenta al cierre del día. Sirve para **conciliar**: la suma
  de movimientos tiene que dar el saldo. Cuando no da, hay un problema (ver catálogo).

## API del procesador de tarjetas (Azure Functions)

Contrato v2:
```
GET /v2/authorizations?from=2025-07-01T00:00:00Z&to=2025-07-02T00:00:00Z&cursor=<opaco>&limit=1000
Authorization: Bearer <token OAuth client_credentials, vence en 15 min>

200 {"data": [...], "next_cursor": "eyJ...", "has_more": true, "total_estimate": 48211}
429 {"error": "rate_limited"}            + header Retry-After: 7
```
- v1 usa `offset`/`limit` y montos en centavos; v2 usa cursor y montos decimales con `currency`.
- La API **no guarda estado en una base**: sirve páginas de archivos pre-generados por día en Blob. Así cuesta
  centavos aunque sirva decenas de millones de filas.
- El caos se controla con una configuración (`CHAOS_PROFILE=off|normal|dificil`) y una semilla, para que cada falla
  sea **reproducible**.

## CRM comercial

Un CRM simulado con el modelo de datos de Salesforce: objetos estándar (Account, Contact, Lead, Opportunity, Case)
con campos custom: `Core_Customer_Id__c`, `CUIT__c` y `Segmento__c` en la cuenta, `Producto__c` en la oportunidad y
`Andes_Key__c` en todos. Esta última es la **clave de carga**: el generador relaciona hijos con padres por ella, y
el CRM le asigna a cada registro su `Id`.

- **Volumen fijo de unos 1.900 registros**: 515 cuentas, unos 600 contactos, 250 leads, 350 oportunidades y 200 casos.
- **Publicación:** la hace el notebook `05_crm_publish`, dentro del job de fuentes:
  1. Genera `sim.crm_*` a partir de una muestra fija de clientes PyME y Premium.
  2. Arma lo que tiene adentro el CRM (`Id`, `CreatedDate`, `SystemModstamp`, `IsDeleted` y los campos calculados),
     siempre igual para la misma semilla.
  3. Lo sube a `crm-data/<Objeto>.json` en Blob.
- **API:** `GET /crm/v1/<Objeto>?modified_since=...` en la misma Function que tarjetas, con el mismo OAuth y el mismo
  caos. Pagina de a 500 por cursor e incluye los borrados.
- **Ingesta:** la hace `14_bronze_crm`. Es incremental por `SystemModstamp`, concilia contra `total` y hace MERGE por
  Id, así que un borrado se propaga.
- **Suciedad:** DAT-09, DAT-16, DAT-17 y DAT-18 (ver el catálogo).

## FX: API real del BCRA + espejo con caos

- **Real**: la API pública de estadísticas cambiarias del BCRA da la cotización oficial por día. Es la única fuente
  100 % real del proyecto: si el BCRA cambia algo, nos enteramos como en la vida real.
- **Espejo**: la misma forma de respuesta servida desde Azure, con caos inyectado (feriados sin dato, valores con
  coma, una moneda que cambia de código). Sirve para practicar sin depender de un tercero.
- Silver usa la real cuando está y el espejo como respaldo, y lo deja registrado en una columna `fx_source`.

## El generador

Un paquete Python (`src/andes_bank/generator/`) genera **toda** la empresa con una semilla fija: los mismos
clientes, cuentas y movimientos cada vez. De ahí salen los archivos del core, los archivos que sirve la API de
tarjetas y el estado del CRM. Corre en Databricks (cómputo gratis de Free Edition) y escribe en Blob.
