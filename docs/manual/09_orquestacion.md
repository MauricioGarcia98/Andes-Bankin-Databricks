# 09 · Orquestación y CI

> `databricks.yml`, `resources/`, `notebooks/` y `.github/workflows/ci.yml`. Cómo se despliega todo en Databricks
> como código, cómo están armados los tres jobs, qué hace cada notebook y qué corre en cada PR.

![Despliegue y jobs](../diagramas/06_despliegue_jobs.svg)

## 1. El Asset Bundle (`databricks.yml`)

Un **Databricks Asset Bundle** declara en YAML todo lo que vive en el workspace (jobs, parámetros, permisos) y lo
despliega con la CLI. Nada se crea a mano en la interfaz.

```bash
# una sola vez, en el SQL Editor (Free Edition no deja crear catálogos por API):
#   CREATE CATALOG IF NOT EXISTS andes_bank;
databricks bundle validate
databricks bundle deploy -t dev                 # sube src/, notebooks/, sql/… y crea/actualiza los jobs
databricks bundle run andes_fuentes -t dev      # simula los sistemas origen y los sube a Azure
databricks bundle run andes_dia -t dev --params dias=3   # simula 3 días nuevos y corre el lakehouse hasta ahí
```

| sección | qué define |
|---|---|
| `bundle.name` | `andes-bank` |
| `include` | `resources/*.yml` (un archivo por job) |
| `variables` | `catalog` (`andes_bank`), `scale_factor` (`0.1`), `start_date`, `end_date`, `warehouse_id` (buscado por nombre: `Serverless Starter Warehouse`) |
| `targets.dev` | `mode: development`, `default: true` |

**Un solo target, y por qué.** `mode: development` prefija los recursos con `[dev <usuario>]`. Free Edition tiene **un
solo workspace**: un target `prod` en el mismo workspace creaba jobs duplicados. Se dejó sólo `dev`.

**Ningún job tiene horario.** Los datos son simulados: no hay un "día real" que esperar. Todo se corre **a pedido**,
cuando se quiere mostrar o probar el pipeline (al principio el lakehouse corría todos los días a las 7 y el día nuevo a
las 5; se sacó porque gastaba cuota de Free Edition sin aportar nada a un proyecto de demostración).

**CLI:** con la versión 0.270 el deploy fallaba porque la clave de firma de Terraform había vencido; con 0.290 funciona.

## 2. Los tres jobs

### `resources/andes_fuentes.job.yml` → job `andes-bank-fuentes`

Simula **el mundo exterior**. No es parte del lakehouse: se corre una vez (o para regenerar).

```
setup ──► generate_truth ──► export_sources ──► upload_to_azure
  │             └──► crm_publish
  └──► fx_mirror
```

| tarea | notebook | reintentos | qué hace |
|---|---|---|---|
| `setup` | 00 | — | esquemas, Volume y tablas de `ops` |
| `generate_truth` | 01 | — | `sim.*` |
| `export_sources` | 02 | — | exportes sucios en `_staging/` (parámetro `only_sources`) |
| `upload_to_azure` | 03 | 1 | sube core, cards-legacy y api-data con la SAS de **escritura** |
| `fx_mirror` | 04 | 2 (cada 1 min) | baja la serie real del BCRA y la deja en el espejo |
| `crm_publish` | 05 | 1 | genera el CRM y lo publica en `crm-data/` |

`max_concurrent_runs: 1`, timeout de 3 horas, parámetros `catalog`, `scale_factor`, `start_date`, `end_date`,
`only_sources`. Para la prueba de volumen se corre con `catalog=andes_bank_sf1` y `scale_factor=1`
([operación](../operacion.md)).

### `resources/andes_lakehouse.job.yml` → job `andes-bank-lakehouse`

El pipeline medallion. Se corre a mano (`databricks bundle run andes_lakehouse -t dev`) o lo lanza el job de simular
días como su última tarea.

```
            ┌─► landing_sync ─► bronze_files ─┐
setup ──────┼─► bronze_api ───────────────────┤
            ├─► bronze_fx ────────────────────┼─► silver ─► gold ─► governance_semantic ─► quality_gate
            └─► bronze_crm ───────────────────┘
```

| tarea | reintentos | por qué |
|---|---|---|
| `landing_sync` | 2, cada 1 min | Azure puede fallar un rato |
| `bronze_files` | 1 | |
| `bronze_api` | 2, **cada 5 min** | el cliente ya reintenta cada pedido; esto cubre caídas largas de la API. El watermark retoma donde quedó |
| `bronze_fx` | 2, cada 2 min | separada de tarjetas a propósito: si el procesador está caído, las cotizaciones entran igual |
| `bronze_crm` | 2, cada 2 min | |
| `silver`, `gold`, `governance_semantic`, `quality_gate` | — | deterministas: si fallan, es un bug, no la red |

Las cuatro ingestas corren **en paralelo** (archivos y APIs no dependen entre sí). `run_id` = `{{job.run_id}}`: queda en
`ops.dq_results` y une cada resultado de calidad con su corrida. Timeout de 4 horas, `max_concurrent_runs: 1`
(dos corridas a la vez pisarían watermarks).

**`end_date`: hasta dónde procesar.** Por defecto, el fin de la historia (`2026-10-20`). Cuando lo lanza el job de
simular días, le pasa el último día simulado. Todo es incremental (Auto Loader, watermarks): cada corrida procesa sólo
lo nuevo, y el quality gate vuelve a controlar gold.

### `resources/andes_dia.job.yml` → job `andes-bank-dia-nuevo` (simular días)

El mundo exterior avanza **a pedido**: es la forma de mostrar el pipeline trabajando con datos nuevos (para el posteo,
una demo o probar un cambio).

```bash
databricks bundle run andes_dia -t dev --params dias=3
```

| tarea | notebook / job | qué hace |
|---|---|---|
| `nuevo_dia` | 06 | `daily.run_new_days(days=3)`: simula los 3 días siguientes al último generado en `sim.*`, los exporta y los sube a Azure. Deja el último día en el *task value* `ultimo_dia` |
| `fx_mirror` | 04 | vuelve a copiar la serie real del BCRA al espejo (el BCRA real nunca se pide después de hoy) |
| `lakehouse` | el job `andes-bank-lakehouse` (`run_job_task`) | corre el lakehouse completo con `end_date = {{tasks.nuevo_dia.values.ultimo_dia}}` |

Los días simulados **no siguen el calendario**: se pueden simular 30 días en una tarde. Si el lakehouse corre antes de
que un día esté publicado, la API devuelve totales en 0: el día queda como "no publicado", no se concilia ni se escribe,
y el watermark no lo saltea (ver [04 · Ingesta](04_ingesta.md)).

## 3. Los notebooks (`notebooks/`)

Cada notebook es **delgado**: `%run ./_bootstrap` y una o dos llamadas al paquete. La lógica está en `src/`.

### `_bootstrap.py`

1. Agrega `../src` al `sys.path` (así `import andes_bank` funciona sin instalar nada).
2. Fija la zona horaria de la sesión en **UTC** (`*_utc` en UTC, `*_local` en hora argentina).
3. Declara los widgets comunes (`catalog`, `scale_factor`, `start_date`, `end_date`): en un job los llenan los
   *job parameters*.
4. Lee `storage_account` del secret scope (si no hay scope, sólo falla lo que necesita Azure).
5. Construye `cfg` con `config_from_widgets` e imprime catálogo, escala, rango, Volume y storage.

### Uno por tarea

| notebook | llama a | muestra |
|---|---|---|
| `00_setup.py` | `create_namespace`, `create_ops_tables` | los esquemas |
| `01_generate_truth.py` | `generate_truth` | filas por tabla y segundos |
| `02_export_sources.py` | `export_all(only)` | rutas escritas |
| `03_upload_to_azure.py` | `upload_all` con la SAS de escritura (parámetros `upload_from`, `parts`) | blobs subidos por parte |
| `04_fx_mirror.py` | `upload_fx_mirror` | cotizaciones por moneda |
| `05_crm_publish.py` | `generate_crm`, `upload_crm(crm_state)` | registros por objeto |
| `10_landing_sync.py` | `run_sync(azure_files(…, sas_read, ["core", "cards-legacy"]))` | archivos por estado |
| `11_bronze_files.py` | `ingest_all(tables)` | filas nuevas por tabla |
| `12_bronze_api.py` | `ApiClient` con los secretos + `ingest_cards_api` (parámetro `max_days`) | pedidos por endpoint y código |
| `13_bronze_fx.py` | `ingest_fx` del BCRA real y del espejo; si el espejo falla, **avisa y sigue** | — |
| `14_bronze_crm.py` | `ingest_crm(crm_api_fetcher(client))` | filas y borrados por objeto |
| `20_silver.py` | `load_all` | resumen JSON |
| `30_gold.py` | `build_gold`, `build_marts`, `build_crm_gold` | filas por tabla |
| `35_governance_semantic.py` | `run_folder("governance")`, `run_folder("semantic")`, `run_folder("crm")` si hay CRM | sentencias por archivo |
| `40_quality_gate.py` | `gate` (en un `try/finally`: muestra los resultados **aunque falle**) | reglas, severidad, filas que fallan |

Los notebooks que usan Azure instalan `azure-storage-blob` con `%pip` (serverless no lo trae).

## 4. CI (`.github/workflows/ci.yml`)

| job | cuándo | qué hace |
|---|---|---|
| `lint-y-tests-rapidos` | cada PR y push a main | `ruff check src tests azure scripts` + `pytest -m "not spark"` (API, CRM, cliente de API, ingesta, ids, config, sql_runner: los que no llevan la marca `spark`) |
| `tests-spark` | push a main y a mano | Java 17 + `pytest -m spark` (generador, e2e: hasta 90 min) |
| `bundle-validate` | si la variable del repo `DATABRICKS_CONFIGURED` es `true` | `databricks bundle validate -t dev` con `DATABRICKS_HOST`/`TOKEN` como secrets del repo |

Los tests con Spark no corren en cada PR porque son lentos; el e2e es el que encontró R08 y R09, así que corren en
`main` y antes de cerrar una etapa.
