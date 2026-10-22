# Manual técnico de Andes Bank

> **Qué es este manual.** `construir/` es un **curso**: te lleva de la mano para rehacer el proyecto, etapa por etapa.
> Este manual es la **referencia**: explica *cada carpeta, cada archivo y cada pieza de código* del repo tal como
> quedó, con cuatro preguntas para cada cosa: **qué es, para qué está, cómo funciona por dentro y cómo se llegó a
> eso** (decisiones y errores reales). Si `construir/` es "cómo lo hago", el manual es "cómo es y por qué es así".

![Andes Bank: el proyecto completo](../diagramas/00_panorama.svg)

## 1. Los objetivos reales del proyecto

Andes Bank **no** es un banco y no tiene usuarios. Es un proyecto de formación y de portfolio. Sus objetivos de
verdad, en orden de importancia:

| # | objetivo | qué significa en concreto | cómo se mide que se cumplió |
|---|---|---|---|
| 1 | **Aprender ingeniería de datos con problemas de verdad** | Los tutoriales tienen datos limpios y APIs que nunca fallan. Acá los datos llegan sucios (37 problemas catalogados en [docs/03](../03_catalogo_de_problemas.md)), las APIs tiran 429, 5xx, tokens vencidos y páginas salteadas, y los archivos llegan tarde, vacíos, rotos o duplicados | Cada problema tiene **dónde se inyecta**, **dónde se resuelve** y **qué regla de calidad lo mide** |
| 2 | **Diseñar para la falla** | Un pipeline que no duplica ni pierde datos aunque la fuente falle: reintentos, idempotencia, watermarks, conciliación contra totales | El test de punta a punta compara el lakehouse con la **verdad** generada y da igual; en Azure, 6.151 pedidos con 163 rate limits y ningún día incompleto |
| 3 | **Exprimir Databricks Free Edition** | Construir un lakehouse completo (Auto Loader, Delta, Unity Catalog, metric views, Genie, Asset Bundles, jobs) en la edición gratuita y documentar sus límites | Funciona a SF 0,1 todos los días y a SF 1 (172 M de movimientos) en 43 minutos; los límites quedaron en ADRs |
| 4 | **Poner a Genie en el centro** | El producto final no es una tabla: es que un gerente pregunte en castellano y obtenga una respuesta correcta y auditable | 4 Genie Agents, 66 benchmarks, entre **80 y 100 %** de aciertos por agent, medido con un script |
| 5 | **Todo como código y reproducible** | Infraestructura de Azure con scripts `az`, Databricks con Asset Bundles, Genie desde YAML, datos con semilla fija | Se puede borrar todo y reconstruirlo ([construir/01](../../construir/01_borrar_todo.md)) |
| 6 | **Costo casi cero** | Las fuentes "reales" viven en Azure con tope de USD 10/mes | USD 0,02 en el mes (`scripts/azure/04_costo.sh`) |
| 7 | **Poder contarlo** | Un posteo de LinkedIn con números reales y un curso para que otro lo reproduzca | [docs/07](../07_posteo_linkedin.md) y `construir/` |

Y un objetivo transversal: **aprender a trabajar con IA sin delegar el criterio**. Claude Code escribió la referencia;
las decisiones de diseño, qué se mide y qué se acepta como "bien resuelto" son del dueño del proyecto, y cada error
real quedó en la [bitácora](../08_bitacora.md).

### Lo que el proyecto *no* busca

- No es un banco real ni usa datos de personas: **todo dato personal es sintético** (DNI, CUIT y CBU con dígito
  verificador válido, pero inventados; el código de banco `998` no existe).
- No hay ML productivo, alta disponibilidad ni redes privadas: se mencionan en "cómo sería en un banco real".
- El CRM no es Salesforce real: se intentó y se abandonó ([bitácora R17](../08_bitacora.md)); es una simulación con el
  modelo de datos de Salesforce.

## 2. La idea central en una imagen mental

```
           EL "MUNDO EXTERIOR" (simulado)                         │            EL LAKEHOUSE (lo que se aprende)
                                                                  │
  generador (Spark, semilla 1998) ──► sim.*  = LA VERDAD          │
        │                                 (nunca la lee el        │
        │ ensucia                          lakehouse; sólo        │
        ▼                                  los tests)             │
  exportes sucios ──► Azure Blob (core, cards-legacy) ────────────┼──► landing (Volume) ─► bronze ─► silver ─► gold
                      Azure Function: API tarjetas v2 + caos ─────┼──► bronze (cliente robusto)        │         │
                                      API CRM + caos ─────────────┼──► bronze (incremental)            │         ▼
                                      espejo BCRA + caos ─────────┼──► bronze                      quality   semantic
  API real del BCRA ──────────────────────────────────────────────┼──► bronze                       gate     (metric views,
                                                                  │                                  │      funciones)
                                                                  │                                  ▼         │
                                                                  │                             ops.dq_results ▼
                                                                  │                                        4 Genie Agents
```

La clave pedagógica es la **verdad conocida**: el generador sabe exactamente qué pasó (cuántos movimientos, qué
persona tiene dos números de cliente, qué tarjeta fue víctima de fraude). El lakehouse sólo ve la versión sucia. Si
el pipeline está bien, **recupera la verdad**; los tests lo verifican fila por fila.

## 3. Cómo leer este manual

| capítulo | qué cubre | carpetas y archivos |
|---|---|---|
| [01 · Repo, configuración y utilidades](01_repo_y_configuracion.md) | cómo está armado el repo, la configuración central, el setup de Unity Catalog, el runner de SQL y las utilidades (Delta, CUIT/CBU, Spark local) | `pyproject.toml`, `src/andes_bank/{config,setup,sql_runner}.py`, `utils/` |
| [02 · El generador: la empresa en código](02_generador.md) | cómo se fabrica un banco determinístico y cómo se lo ensucia | `src/andes_bank/generator/` |
| [03 · Azure: las fuentes "reales"](03_azure.md) | la Azure Function (API de tarjetas, CRM, espejo BCRA), el caos, los tokens y los scripts `az` | `azure/card_api/`, `scripts/azure/` |
| [04 · Ingesta: landing y bronze](04_ingesta.md) | el borde (Blob → Volume), Auto Loader, el cliente de API robusto, bronze de APIs y CRM | `src/andes_bank/ingestion/` |
| [05 · Silver](05_silver.md) | limpieza problema por problema, SCD2, identidad, cuarentena, incremental | `src/andes_bank/silver/` |
| [06 · Gold y calidad](06_gold_y_calidad.md) | modelo estrella, marts, CRM en gold y el quality gate | `src/andes_bank/gold/`, `quality/` |
| [07 · SQL: gobierno y capa semántica](07_sql_gobierno_y_semantica.md) | comentarios, claves, tags, máscaras, filtros por región, vistas, metric views, funciones certificadas | `sql/` |
| [08 · Genie](08_genie.md) | agents como código, build, deploy, preguntar y medir | `genie/`, `scripts/*genie*` |
| [09 · Orquestación y CI](09_orquestacion.md) | Asset Bundle, los tres jobs, los notebooks, CI de GitHub | `databricks.yml`, `resources/`, `notebooks/`, `.github/` |
| [10 · Tests](10_tests.md) | qué prueba cada test y cómo se prueba sin nube | `tests/` |
| [11 · Scripts, docs, diagramas y asistentes](11_scripts_docs_y_diagramas.md) | diagramas como código, tags de etapa, documentación, Genie Code | `scripts/diagramas/`, `scripts/etapas.sh`, `docs/`, `construir/`, `.assistant*` |
| [12 · El recorrido de un dato](12_recorrido_de_un_dato.md) | un movimiento y una compra con tarjeta, desde que "pasan" hasta que Genie los cuenta | todo junto |

Si querés **una sola lectura**: este README, después el [12](12_recorrido_de_un_dato.md) (el viaje de un dato) y
después el capítulo de la capa que te interese.

## 4. Mapa del repositorio

```
Andes-Bankin-Datbricks/
├── README.md                     portada: estado, índice de docs, MVP
├── pyproject.toml                paquete andes_bank, dependencias (dev/azure/genie), pytest y ruff        → cap. 01
├── databricks.yml                Asset Bundle: variables y target dev                                    → cap. 09
├── resources/                    los tres jobs (fuentes, día nuevo y lakehouse)                          → cap. 09
├── notebooks/                    notebooks DELGADOS: cada uno llama al paquete (una tarea de job c/u)    → cap. 09
├── src/andes_bank/               TODO el código de datos, testeado
│   ├── config.py                 nombres de UC, rutas del Volume, escala                                 → cap. 01
│   ├── setup.py                  esquemas, volumen y tablas de ops                                       → cap. 01
│   ├── sql_runner.py             corre los .sql de sql/ con ${variables}                                 → cap. 01
│   ├── utils/                    delta (MERGE, replace_where, watermarks), ids (CUIT/CBU), spark local   → cap. 01
│   ├── generator/                el banco simulado: verdad, exportes sucios, subida a Azure, CRM         → cap. 02
│   ├── ingestion/                landing_sync, bronze_files, api_client, bronze_api, bronze_crm          → cap. 04
│   ├── silver/                   transforms, core, cards, crm, load                                      → cap. 05
│   ├── gold/                     model (estrella), marts, crm                                            → cap. 06
│   └── quality/gate.py           22 reglas y el quality gate                                             → cap. 06
├── sql/                          gobierno (comentarios, PK/FK, tags, máscaras), CRM y capa semántica     → cap. 07
├── genie/                        src/*.yml (fuente) → agents/*.json (build) → deployed_dev.json          → cap. 08
├── azure/card_api/               la Azure Function: API de tarjetas + CRM + espejo BCRA, con caos        → cap. 03
├── scripts/
│   ├── azure/                    infraestructura, secretos, deploy de la API, costo, teardown           → cap. 03
│   ├── build_genie.py · deploy_genie.py · validate_genie_sql.py · genie_ask.py · genie_benchmark.py      → cap. 08
│   ├── diagramas/                Excalidraw como código (generar, exportar, íconos)                      → cap. 11
│   └── etapas.sh                 tags etapa-NN para el curso                                             → cap. 11
├── tests/                        49 tests: unitarios + punta a punta contra la verdad                    → cap. 10
├── .github/workflows/ci.yml      lint, tests rápidos, tests con Spark, bundle validate                  → cap. 09
├── .assistant_instructions.md · .assistant/skills/   reglas del proyecto para Genie Code              → cap. 11
├── docs/                         definición, empresa, fuentes, catálogo, arquitectura, Genie, bitácora…  → cap. 11
│   ├── adr/                      decisiones de arquitectura (acceso saliente, landing sync)
│   ├── diagramas/                19 diagramas Excalidraw (00 a 17 y el de íconos; .excalidraw + .svg)
│   └── manual/                   ESTE manual
└── construir/                    el curso: 28 capítulos para reconstruir todo sin IA
```

## 5. Unity Catalog: las 7 capas del catálogo `andes_bank`

| esquema | qué guarda | quién escribe | quién lee |
|---|---|---|---|
| `sim` | la **verdad** del banco simulado (`customers`, `accounts`, `transactions`…) y el CRM (`crm_*`) | job de fuentes | sólo tests, conciliaciones y la publicación del CRM. **El lakehouse nunca** |
| `landing` | el Volume `raw`: archivos copiados de Azure, `_staging/` del generador, checkpoints, esquemas de Auto Loader, `_errors/` | landing_sync, generador, Auto Loader | Auto Loader |
| `bronze` | lo que llegó **tal cual**: CSV como texto (dos contratos por fuente del core), JSON crudo de las APIs | tareas `bronze_*` | silver |
| `silver` | datos limpios, tipados, deduplicados, con historia (SCD2), identidad resuelta y cuarentena | tarea `silver` | gold, reglas de calidad |
| `gold` | modelo dimensional (dims y facts), marts por área, CRM unido al core | tarea `gold` | capa semántica, Genie, dashboards |
| `semantic` | vistas anchas, **metric views** y **funciones certificadas** | `governance_semantic` | Genie |
| `ops` | operación: `api_calls`, `api_reconciliation`, `landing_files`, `dq_results`, watermarks, `region_access` | todas las tareas | Genie Plataforma, operación |

## 6. Convenciones que atraviesan todo el código

- **El código vive en el paquete, no en los notebooks.** Un notebook tiene 5 a 20 líneas: arma `cfg` y llama a una
  función de `andes_bank`. Así todo se prueba con pytest en local, sin Databricks.
- **Determinismo.** Todo el azar del generador sale de `xxhash64(columnas, semilla)`: la misma semilla da el mismo
  banco, con 1 o con 100 executors. Los Id del CRM y sus fechas también.
- **Idempotencia.** Re-correr cualquier tarea no duplica nada: MERGE insert-only para hechos, `replace_where` para
  reemplazar un rango, reconstrucción completa para maestros y gold.
- **Bronze es sagrado.** Lo que llegó, tal cual llegó (texto o JSON crudo) + linaje (`_source_file`,
  `_delivery_date`, `_ingested_at`). Nunca se limpia en bronze.
- **Una función por problema.** Cada arreglo de silver tiene su ID del catálogo (DAT-02, API-06…) en el comentario,
  y una regla en el quality gate.
- **Nombres en castellano** en silver y gold (`importe`, `fecha_contable`), montos `decimal(18,2)` y siempre también
  `importe_ars` e `importe_usd` al oficial del día.
- **Horas:** `*_utc` en UTC (la sesión de Spark está en UTC), `*_local` en hora argentina (UTC−3 fijo).
- **Secretos** sólo por `dbutils.secrets.get("andes_bank", …)` o variables de entorno. Nunca en el repo.

## 7. Glosario rápido

| término | significado en este proyecto |
|---|---|
| **SF** (scale factor) | multiplicador de la cantidad de clientes: 0,1 = 50 mil (el de todos los días), 1 = 500 mil |
| **la verdad** | las tablas `sim.*`: lo que "pasó de verdad", antes de ensuciarlo |
| **contrato** | el formato que promete un archivo: columnas, separador, encoding. El core tiene dos (antes y después de mar-2024) |
| **CDC** | *change data capture*: eventos I (alta), U (cambio), D (baja) con la foto completa del registro |
| **SCD2** | historia tipo 2: una fila por versión, con `valid_from`, `valid_to`, `is_current` |
| **watermark** | marca de "hasta dónde ya procesé" (un día, un timestamp). Permite procesar sólo lo nuevo |
| **conciliar** | comparar lo recibido contra un total que publica la fuente (o contra otro sistema) |
| **quality gate** | la última tarea: si una regla bloqueante falla, el job falla y Genie sigue con los datos de la última corrida buena |
| **caos** | fallas inyectadas a propósito en la API (perfil `off`, `normal` o `dificil`) |
| **R01…R18** | entradas de la [bitácora](../08_bitacora.md): errores reales de la construcción |
| **DAT/API/ING-NN** | IDs del [catálogo de problemas](../03_catalogo_de_problemas.md) |
| **etapa-NN** | tags de git con el punto de control de cada capítulo de `construir/` |
