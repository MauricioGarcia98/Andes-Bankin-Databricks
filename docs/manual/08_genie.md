# 08 · Genie

> Carpeta `genie/` y los scripts `scripts/build_genie.py`, `deploy_genie.py`, `validate_genie_sql.py`,
> `genie_ask.py` y `genie_benchmark.py`. Genie es el producto final del proyecto: la forma en que cada área le
> pregunta a los datos en castellano. Acá los agents son **código**: se escriben en YAML, se compilan, se despliegan
> y se **miden**.

![La pirámide de Genie](../diagramas/15_piramide_genie.svg)

## 1. La pirámide: dónde se arregla una respuesta mala

Cuando Genie responde mal, la tentación es agregar instrucciones al agent. Casi siempre es el lugar equivocado. El
orden en que se arregla, de abajo hacia arriba:

1. **El dato y su grano** (gold): ¿la tabla tiene lo que dice que tiene? (R18: el mart decía "clientes vigentes" y
   traía bajas).
2. **Comentarios** de tablas y columnas (`sql/governance/01_comments.sql`).
3. **Métricas oficiales** (metric views) y **funciones certificadas** (`sql/semantic/`).
4. **Ejemplos de SQL** del agent.
5. **Instrucciones** de texto del agent: lo último, y lo más corto posible.

## 2. `genie/`: la estructura

```
genie/
├── src/                    LA FUENTE (lo que se edita): un YAML por agent
│   ├── 01_comercial_clientes.yml
│   ├── 02_tarjetas_fraude.yml
│   ├── 03_finanzas_tesoreria.yml
│   └── 04_plataforma_datos.yml
├── agents/                 GENERADO por build_genie.py: el serialized_space de la API (no se edita a mano)
├── deployed_dev.json       slug → space_id de lo desplegado (lo escribe deploy_genie.py)
└── benchmarks_dev.json     el resultado de la última medición (lo escribe genie_benchmark.py)
```

### Los 4 agents

| agent | para quién | tablas | metric views | función | benchmarks | acierto |
|---|---|---|---|---|---|---|
| **Comercial y clientes** | Gerencia de Banca Individual y Pymes | `mart_cliente_360`, `dim_cliente`, `dim_cuenta`, `dim_sucursal`, CRM (`dim_cuenta_crm`, `fact_oportunidad`, `fact_caso`, `mart_pipeline_comercial`) | `mv_clientes` | `fn_resumen_cliente` | 20 | **90 %** |
| **Tarjetas y fraude** | Prevención de fraude | `fact_autorizacion`, `dim_comercio`, `dim_tarjeta`, `mart_fraude_diario` | `mv_tarjetas` | `fn_alertas_fraude` | 16 | **87,5 %** |
| **Finanzas y tesorería** | Tesorería | `fact_saldo_diario`, `mart_tesoreria_mensual`, `fact_movimiento`, `dim_fecha` | `mv_depositos`, `mv_movimientos` | `fn_posicion_dolares` | 15 | **80 %** |
| **Plataforma de datos** | el equipo de datos | `v_api_llamadas`, `v_calidad`, `v_archivos_recibidos`, `ops.api_reconciliation`, `balance_breaks`, `quarantine`, `mart_conciliacion_tarjetas` | `mv_plataforma_apis` | `fn_salud_ingesta` | 15 | **100 %** |

### Anatomía de un YAML (`genie/src/*.yml`)

| clave | qué es |
|---|---|
| `title`, `domain`, `description`, `users` | identidad del agent (el título es la clave para encontrarlo al desplegar) |
| `tables`, `metric_views`, `functions` | a qué tiene acceso, sin el catálogo (`gold.mart_cliente_360`) |
| `instructions` | el texto para el agent: vocabulario del área ("clientes" vs. "personas"), qué tabla usar para qué, formatos, avisos sobre máscaras y filtros por región |
| `sample_questions` | las preguntas sugeridas que ve el usuario |
| `examples` | pares pregunta → SQL **de referencia** (con `${catalog}`) |
| `benchmarks` | pares pregunta → SQL con **la respuesta oficial**: lo que se usa para medir (mínimo 15 por agent) |

Un benchmark tiene que ser **determinístico** (sin `current_date()`: usar la fecha máxima de los datos) y devolver lo
mínimo (un número o una fila). La skill `.assistant/skills/benchmark-genie` lo resume.

## 3. Los scripts

### `build_genie.py`: YAML → JSON

Convierte cada YAML en `genie/agents/<slug>.json` con la forma que importa/exporta la API de Genie (`version: 2`):
`config.sample_questions`, `data_sources.tables` (tablas + metric views), `instructions.text_instructions`,
`example_question_sqls`, `sql_functions` y `benchmarks.questions`.

- Los `id` son **hashes estables del contenido** (md5 de slug + tipo + texto): reconstruir no cambia nada si no cambió
  el YAML, y el diff en git muestra sólo lo que de verdad se tocó.
- Cada lista se **ordena por id** porque la API lo exige ("must be sorted by id").
- El texto se parte en líneas (la API guarda listas de líneas).
- El catálogo queda como `${catalog}`; lo reemplaza el deploy según el target.
- Falla si un agent tiene menos de 15 benchmarks.

### `validate_genie_sql.py`: correr todo el SQL antes de desplegar

Ejecuta en el SQL warehouse **todos** los ejemplos y benchmarks. Un benchmark que no corre es uno que Genie "falla"
siempre: mejor enterarse acá. `--show` imprime la primera fila de cada uno. Sale con código 1 si alguno falla.

### `deploy_genie.py`: crear o actualizar

1. Busca el warehouse (`Serverless Starter Warehouse`) y lista los spaces existentes **por título**.
2. Por cada JSON: reemplaza `${catalog}`, agrega `[dev]` al título fuera de prod, y **verifica las funciones**
   (`check_functions`): si alguna tiene parámetros `DATE`/`TIMESTAMP`, aborta con un mensaje claro (R10).
3. Si el space existe hace `PATCH`, si no `POST` (idempotente).
4. Guarda `deployed_<target>.json`, **mezclando** con lo anterior (con `--only` no se pierden los agents no tocados).

Extras: `--export SPACE_ID` (trae la configuración de un space para versionar cambios hechos desde la interfaz) y
`--grant-app` (permisos para una Databricks App: `CAN_RUN` en los spaces y `SELECT/EXECUTE` en gold, semantic y ops).
Las credenciales salen de `DATABRICKS_HOST/TOKEN` o `~/.databrickscfg` y nunca se imprimen.

### `genie_ask.py`: preguntar por API

Usa la **Conversation API**: `start-conversation`, espera (cada 3 s, hasta 180) a que el mensaje termine, y de los
adjuntos saca el texto, el **SQL que generó Genie** y el resultado (`query-result`). Devuelve `rows` (las 5 primeras,
para mostrar) y `all_rows` (todas, para comparar).

```bash
python scripts/genie_ask.py 01_comercial_clientes "¿Cuántos clientes tenemos por segmento?"
```

### `genie_benchmark.py`: medir

Por cada benchmark de cada agent:

1. Corre el **SQL esperado** en el warehouse.
2. Le hace **la misma pregunta** a Genie con `genie_ask.ask`.
3. Compara con `matches()`: acierta si **cada valor esperado** aparece en el resultado de Genie. Números con tolerancia
   de 0,5 % (y también ×100 o /100, por los porcentajes), textos sin importar mayúsculas, y en preguntas de sí/no un 0
   esperado equivale a "No". Genie puede devolver columnas o filas **de más** (el nombre al lado del número): no se
   penaliza.
4. Espera 4 s entre preguntas (cuota de la API). Un error de red o de cuota cuenta como fallo, no frena la corrida.

Escribe `genie/benchmarks_<target>.json` con el detalle (esperado, lo de Genie, el SQL de Genie) y sale con código 1
si algún agent queda por debajo de **80 %**. El diagrama `17_en_numeros` lee ese archivo.

## 4. Cómo se llegó a los números

Primera medición: Comercial **70 %**. Mirando el detalle en `benchmarks_dev.json`, los fallos no eran de Genie:

- `mart_cliente_360` traía bajas (Genie filtraba bien, el dato estaba mal);
- nada decía que `tipo_persona = JURIDICA` es "empresa";
- en `v_api_llamadas` no había forma de separar la API de tarjetas de la del CRM.

Se arregló la base de la pirámide (grano del mart, comentarios, sinónimos, columna `api`) y se sumaron dos
aclaraciones a las instrucciones. Resultado: Comercial 90 %, Tarjetas 87,5 %, Finanzas 80 %, Plataforma 100 %
(59 de 66 benchmarks, 89 %). Ver bitácora R18.

## 5. Flujo completo para cambiar un agent

```bash
# 1. editar genie/src/<agent>.yml (o mejor: el comentario o la métrica que corresponda)
python scripts/build_genie.py                          # 2. YAML → JSON
python scripts/validate_genie_sql.py                   # 3. todo el SQL corre
python scripts/deploy_genie.py --target dev            # 4. crear o actualizar
python scripts/genie_benchmark.py --only <agent>       # 5. medir
```
