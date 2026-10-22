# 26 · Etapa 23 · Operar con el caos: corrida diaria, diagnóstico y prueba de volumen

## 🎯 Objetivo

Dejar de construir y empezar a **operar**:

- el job del lakehouse corre solo todos los días a las 7 (hora argentina) y avisa por mail si falla;
- cuando falla, sabés **qué mirar** y **qué hacer**, tarea por tarea;
- sabés cuánto aguanta Free Edition: la **prueba de volumen** a SF 1 (10 veces más datos).

**Archivos:** `resources/andes_lakehouse.job.yml` (schedule), `scripts/azure/04_costo.sh`, `docs/operacion.md`.

![Operar con el caos](../docs/diagramas/16_operacion.svg)

## 📚 Conceptos

- **Re-correr es seguro:** todas las escrituras son idempotentes (watermarks, MERGE, `replace_where`). Ante la duda,
  se vuelve a correr la tarea que falló.
- **Cada tarea tiene su tabla de `ops`:** `landing_files`, `api_calls`, `api_reconciliation`, `dq_results`. El
  diagnóstico empieza ahí, no en los logs.
- **El Genie de Plataforma** responde lo mismo en castellano: *"¿cómo estuvieron las APIs en los últimos 7 días?"*.

## 🧱 Paso a paso

### 1. La corrida diaria

```yaml
schedule:
  quartz_cron_expression: "0 0 7 * * ?"
  timezone_id: America/Argentina/Buenos_Aires
  pause_status: UNPAUSED          # mode: development pausa los schedules que no dicen nada
email_notifications:
  on_failure: ["${workspace.current_user.userName}"]
```

Para pausarlo (por ejemplo, si te quedás sin cuota de Free Edition): `pause_status: PAUSED` y `bundle deploy`.

### 2. El diagnóstico

Seguí el diagrama de arriba. La guía completa, con las consultas, está en [docs/operacion.md](../docs/operacion.md).

### 3. El costo

```bash
bash scripts/azure/04_costo.sh
#   TOTAL del mes                    0.0214 USD  (OK: tope USD 10)
```

### 4. La prueba de volumen (SF 1)

Se corrió en un catálogo aparte (`andes_bank_sf1`), para no tocar los datos que andan:

```bash
databricks jobs run-now --json '{"job_id": <andes-bank-fuentes>, "only": ["setup","generate_truth","export_sources"],
  "job_parameters": {"catalog": "andes_bank_sf1", "scale_factor": "1"}}'
```

Los resultados están en [docs/operacion.md](../docs/operacion.md#prueba-de-volumen-sf-1).

## 🤔 Por qué así

- **¿Por qué un catálogo aparte para la prueba?** Porque si la cuota de Free Edition corta la corrida a la mitad, el
  catálogo bueno queda intacto. Después se borra con `DROP CATALOG andes_bank_sf1 CASCADE`.
- **¿Por qué mail y no Slack?** Free Edition manda mails sin configurar nada. Un webhook de Slack es el ejercicio.

## 🧪 Probalo

```bash
databricks jobs list | grep lakehouse
databricks jobs get <job_id> | grep -A3 schedule        # UNPAUSED, 0 0 7 * * ?
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| El job no corre a la mañana | schedule pausado por `mode: development` | `pause_status: UNPAUSED` explícito |
| El job corre pero las tareas quedan en cola | se acabó la cuota diaria de Free Edition | esperá al día siguiente o pausá el schedule |
| El mail de falla no llega | `email_notifications` sin destinatario | `${workspace.current_user.userName}` |

## ✅ Punto de control

`git diff etapa-23 -- resources/andes_lakehouse.job.yml docs/operacion.md`

## 🏋️ Ejercicio

Agregá un aviso por Slack (`webhook_notifications`) cuando falle el quality gate, con el nombre de la regla que frenó.
