# 20 · Etapa 16 · Asset Bundle y jobs

## 🎯 Objetivo

Todo lo de Databricks como **código** y desplegado con un comando: los dos jobs (fuentes y lakehouse), sus tareas, sus
reintentos, su horario y sus alertas.

**Archivos:** `databricks.yml`, `resources/andes_fuentes.job.yml`, `resources/andes_lakehouse.job.yml`,
`notebooks/*.py`, `notebooks/_bootstrap.py`.

![Despliegue, CI y orquestación](../docs/diagramas/06_despliegue_jobs.svg)

## 📚 Conceptos

- **Asset Bundle:** un `databricks.yml` con variables y *targets* + archivos `resources/*.yml` con los recursos. Se
  valida (`bundle validate`), se despliega (`bundle deploy`) y se corre (`bundle run`).
- **`mode: development`:** prefija los recursos con `[dev <usuario>]` y pausa los schedules que no dicen nada. El del
  lakehouse dice `pause_status: UNPAUSED` a propósito: Free Edition tiene un solo workspace.
- **Notebook delgado:** cada tarea es un notebook de pocas líneas que lee widgets, arma la config y llama al paquete.
  `_bootstrap.py` hace eso una vez para todos.
- **Serverless:** las tareas no declaran cluster: corren en el cómputo serverless de Free Edition.

## 🧱 Paso a paso

### 1. El bundle (`databricks.yml`)

```yaml
bundle:
  name: andes-bank
include:
  - resources/*.yml
variables:
  catalog: {default: andes_bank}
  scale_factor: {default: "0.1"}
  warehouse_id:
    lookup: {warehouse: Serverless Starter Warehouse}
targets:
  dev:
    mode: development
    default: true
```

### 2. El job del lakehouse

```yaml
schedule:
  quartz_cron_expression: "0 0 7 * * ?"           # todos los días a las 7
  timezone_id: America/Argentina/Buenos_Aires
  pause_status: UNPAUSED
email_notifications:
  on_failure: ["${workspace.current_user.userName}"]
tasks:
  - task_key: bronze_api
    depends_on: [{ task_key: setup }]
    # el cliente ya reintenta cada pedido; esto es para caídas de la API más largas (el watermark retoma)
    max_retries: 2
    min_retry_interval_millis: 300000
```

El orden: `setup` → (`landing_sync` → `bronze_files`) ∥ `bronze_api` ∥ `bronze_fx` ∥ `bronze_crm` → `silver` → `gold` →
`governance_semantic` → `quality_gate`.

### 3. El notebook delgado

```python
# MAGIC %run ./_bootstrap
from andes_bank.ingestion import bronze_api, bronze_crm
client = ApiClient(sec("card_api_base_url"), sec("card_api_client_id"), sec("card_api_client_secret"), run_id=run_id)
out = bronze_crm.ingest_crm(spark, cfg, bronze_crm.crm_api_fetcher(client), run_id)
```

### 4. `setup` crea todo lo de `ops` vacío

```python
def create_ops_tables(spark, cfg):
    """Crea vacías todas las tablas de operación. Así las vistas y los Genie Agents que las consultan existen desde
    el primer deploy, aunque la tarea que las llena todavía no haya corrido."""
```

## 🤔 Por qué así

- **¿Por qué dos jobs?** El de fuentes simula el mundo exterior (se corre cuando querés regenerar el banco); el del
  lakehouse es el pipeline de verdad (corre todos los días). Mezclarlos haría que cada corrida diaria regenere 17 M de
  movimientos.
- **¿Por qué reintentos por tarea si el cliente ya reintenta?** El cliente cubre fallas de segundos; el reintento de
  la tarea cubre una API caída 10 minutos. El watermark hace que el reintento retome donde quedó.

## 🧪 Probalo

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run andes_lakehouse -t dev
databricks jobs list | grep andes        # el lakehouse con schedule UNPAUSED
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `governance_semantic` falla: `ops.dq_results` no existe | una vista lee una tabla que crea una tarea posterior | `setup` crea vacías todas las tablas de `ops` (R11) |
| `error downloading Terraform: openpgp: key expired` | CLI vieja | actualizá la CLI de Databricks |
| `cannot set trigger pause status to UNPAUSED` en `presets` | development no deja des-pausar por preset | `pause_status: UNPAUSED` en el schedule del job |

## ✅ Punto de control

`git diff etapa-16 -- databricks.yml resources/`

## 🏋️ Ejercicio

Agregá un tercer job, `andes-bank-mantenimiento`, semanal, que corra `OPTIMIZE` y `VACUUM` sobre las tablas de silver
y gold. ¿Qué pasa con el time travel después de un `VACUUM`?
