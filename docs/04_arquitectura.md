# 04 · Arquitectura

## Vista general

```
                   ┌────────────────────────── AZURE (pago por uso, tope USD 10) ─────────────────────────┐
  Generador ─────► │  Blob Storage  landing/core/...  (archivos del core)                                 │
  (corre en        │  Blob Storage  api-data/...      (lo que sirve la API, por día)                      │
   Databricks)     │  Azure Functions  /v1 /v2 authorizations, settlements, fx-mirror  (+ caos)           │
                   └──────────────────────────────────────────────────────────────────────────────────────┘
                                │ archivos                 │ HTTP paginado
  Salesforce (org gratuita) ────┼──────── REST/Bulk ───────┤
  API real del BCRA ────────────┼──────── HTTP ────────────┤
                                ▼                          ▼
  ┌──────────────────────────── DATABRICKS FREE EDITION (Unity Catalog) ─────────────────────────────────┐
  │ Bronze: Auto Loader (archivos) + clientes Python robustos (APIs) → tablas crudas + ops.api_calls      │
  │ Silver: limpieza, dedupe, SCD2, resolución de identidad, conciliación, cuarentena                     │
  │ Gold:   modelo dimensional (dim_cliente, dim_cuenta, fact_movimiento, fact_autorizacion, ...)         │
  │ Semántica: metric views + funciones certificadas + comentarios + máscaras y row filters               │
  │ Consumo: Genie Agents por área · Genie One · Dashboards · App (opcional) · Genie Code para construir   │
  └──────────────────────────────────────────────────────────────────────────────────────────────────────┘
  Orquestación: Lakeflow Jobs  ·  Despliegue: Asset Bundles  ·  CI/CD: GitHub Actions
```

## Pull vs. push (por qué elegimos *pull*)

Free Edition restringe el acceso saliente a internet hasta que verificás la cuenta con LinkedIn. Como vas a
verificarla, **Databricks llama directamente a las APIs** (patrón *pull*), que es lo que hace un equipo de datos
real. Igual el primer paso de la construcción (el *spike*) lo comprueba con un `curl` desde un notebook a cada
dominio: Azure Functions, Blob, Salesforce y BCRA.

**Plan B si algún dominio queda bloqueado:** una Azure Function con timer consume esa API y deja los archivos en
Blob (*push*). Se documenta en un ADR y el resto del proyecto no cambia.

**Lectura de Blob desde Databricks:** la doc oficial dice que Free Edition no soporta *external locations* propias y
hay respuestas de la comunidad que dicen lo contrario. El *spike* lo prueba. Si no anda, Databricks lee Blob con una
**SAS de sólo lectura** guardada en un *secret scope*, y copia a un **Volume** gestionado donde corre Auto Loader.

## Unity Catalog

```
andes_bank (catálogo)
├── bronze      tablas crudas, una por fuente
├── silver      tablas limpias + cuarentena
├── gold        dimensiones y hechos
├── semantic    metric views y funciones para Genie
├── ops         ingestion_runs, api_calls, quality_results, conciliaciones
└── landing     Volumes: archivos copiados, errores de API, checkpoints
```

## Herramientas y quién las usa

| herramienta | para qué |
|---|---|
| **Azure CLI (`az`)** | crear el resource group, Storage, Function App, presupuesto y alertas; desplegar las Functions |
| **Databricks CLI** | `bundle deploy`, correr jobs, crear secret scopes, subir archivos, desplegar Genie |
| **Salesforce CLI (`sf`)** | crear el campo custom, cargar datos con Bulk API, consultar con SOQL |
| **GitHub Actions** | lint, tests, `bundle validate` y deploy con aprobación |

## Costos (tope USD 10/mes)

| recurso | plan | costo esperado |
|---|---|---|
| Storage Account (LRS, Hot) | pago por uso | ~20-30 GB a SF 1 ≈ **USD 0,5-1/mes** + operaciones |
| Azure Functions | Consumption (1 M ejecuciones/mes gratis) | **~USD 0** si se pagina de a 1.000 filas |
| Salida de datos (egress) Azure → Databricks | primeros 100 GB/mes gratis | **~USD 0** a SF 0,1; vigilar a SF 1 |
| Application Insights | muestreo activado, retención mínima | **~USD 0-1** |
| **Presupuesto** | alerta al 50 % (USD 5) y al 80 % (USD 8) | — |

Reglas para no pasarse:
- Nada que cobre por hora encendido (ni VMs, ni API Management en planes fijos, ni bases de datos).
- La carga histórica a SF 1 se hace **una sola vez**. El día a día corre a SF 0,1.
- Un script `scripts/azure_teardown.sh` borra todo el resource group con un comando.

## Seguridad (aunque los datos sean sintéticos)

- Ningún secreto en el repo: los secretos van a **Databricks secret scopes**, a la **configuración de la Function
  App** y a los **secrets de GitHub**.
- La Function exige un token (OAuth client credentials simulado) y valida el rango de fechas pedido.
- PII enmascarada en Unity Catalog; cada área ve sólo lo que le toca.
