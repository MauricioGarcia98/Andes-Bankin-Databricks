# 26 · Etapa 23 · Operar con el caos: simular días, diagnóstico y prueba de volumen

## 🎯 Objetivo

Dejar de construir y empezar a **operar**:

- **simulás días nuevos a pedido** y el lakehouse los procesa (para una demo, el posteo o probar un cambio);
- cuando falla, sabés **qué mirar** y **qué hacer**, tarea por tarea;
- sabés cuánto aguanta Free Edition: la **prueba de volumen** a SF 1 (10 veces más datos).

**Archivos:** `src/andes_bank/generator/daily.py`, `notebooks/06_new_day.py`, `resources/andes_dia.job.yml`,
`scripts/azure/04_costo.sh`, `docs/operacion.md`.

![Operar con el caos](../docs/diagramas/16_operacion.svg)

## 📚 Conceptos

- **Re-correr es seguro:** todas las escrituras son idempotentes (watermarks, MERGE, `replace_where`). Ante la duda,
  se vuelve a correr la tarea que falló.
- **Cada tarea tiene su tabla de `ops`:** `landing_files`, `api_calls`, `api_reconciliation`, `dq_results`. El
  diagnóstico empieza ahí, no en los logs.
- **El Genie de Plataforma** responde lo mismo en castellano: *"¿cómo estuvieron las APIs en los últimos 7 días?"*.

## 🧱 Paso a paso

### 1. Simular días nuevos (a pedido)

La historia termina el 20-oct-2026. Para ver el pipeline trabajar con datos **nuevos** (lo incremental, los
watermarks, el quality gate de cada corrida), el job `andes-bank-dia-nuevo` simula los N días siguientes y después corre
el lakehouse hasta ahí:

```bash
databricks bundle run andes_dia -t dev --params dias=3
```

- `nuevo_dia` (notebook 06 → `daily.run_new_days(days=3)`): agrega a `sim.*` los 3 días siguientes al último generado
  (con las mismas fórmulas que la historia), los exporta sucios y los sube a Azure. Deja el último día en el *task
  value* `ultimo_dia`.
- `fx_mirror`: actualiza el espejo del BCRA.
- `lakehouse`: corre el job del lakehouse con `end_date = {{tasks.nuevo_dia.values.ultimo_dia}}`.

**Por qué a pedido y no con horario:** los días son simulados, no hay un "día real" que esperar. Un horario sólo
gastaría cuota de Free Edition. El detalle de cómo se simula un día está en el
[manual técnico](../docs/manual/02_generador.md#11-dailypy-el-día-nuevo).

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

## 🧪 Probalo

```bash
databricks bundle run andes_dia -t dev --params dias=1
# después, en el SQL Editor:
#   SELECT * FROM andes_bank.sim.generated_days ORDER BY day DESC;              -- el día simulado
#   SELECT max(fecha_contable) FROM andes_bank.gold.fact_movimiento;            -- llegó a gold
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| El job corre pero las tareas quedan en cola | se acabó la cuota diaria de Free Edition | esperá al día siguiente o simulá menos días |
| Gold no tiene las compras del día simulado | el día se subió a medias antes de tiempo (bitácora R20) | la historia corta en `end_date`; el día nuevo pisa las páginas de la API del día |

## ✅ Punto de control

`git diff etapa-23 -- resources/andes_lakehouse.job.yml docs/operacion.md`

## 🏋️ Ejercicio

Agregá un aviso por Slack (`webhook_notifications`) cuando falle el quality gate, con el nombre de la regla que frenó.
