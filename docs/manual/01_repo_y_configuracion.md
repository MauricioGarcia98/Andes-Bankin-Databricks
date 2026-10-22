# 01 · Repo, configuración y utilidades

> Los cimientos: cómo está empaquetado el código, de dónde salen los nombres de las tablas y las rutas, cómo se crean
> las capas en Unity Catalog, cómo se corren los `.sql` y las utilidades que usan todas las capas.

## 1. Por qué un paquete Python y no notebooks

**Decisión:** todo el código de datos vive en `src/andes_bank/` (un paquete instalable). Los notebooks sólo arman la
configuración y llaman a una función.

**Para qué:**

- **Probar sin Databricks.** Con Spark y Delta OSS en local, `pytest` corre el pipeline entero (generador → API falsa
  → bronze → silver → gold → gate) en minutos. Un notebook no se puede testear así.
- **Revisar cambios.** Un diff de un `.py` se lee; el de un notebook con salidas, no.
- **Reutilizar.** La misma función (`ingest_cards_api`) la usan el notebook 12, el test y cualquier script.

**Cómo llega el paquete a Databricks:** sin instalarlo. `notebooks/_bootstrap.py` agrega `../src` al `sys.path`
(el bundle sube la carpeta `src/` junto con los notebooks), así que `import andes_bank` funciona en serverless sin
wheels ni librerías de cluster.

## 2. `pyproject.toml`

| sección | qué define | por qué así |
|---|---|---|
| `[project]` | nombre `andes_bank`, Python ≥ 3.10, **sin dependencias obligatorias** | en Databricks pyspark y delta ya vienen; instalar otra versión rompería el runtime |
| `[project.optional-dependencies] dev` | `pyspark==4.0.1`, `delta-spark==4.0.0`, `pytest`, `ruff`, `pyyaml` | versiones fijas: Delta 4.0 corresponde a Spark 4.0; mezclar versiones da errores de clase al arrancar |
| `azure` | `azure-storage-blob`, `requests` | sólo los necesitan el uploader, landing_sync y los clientes de API |
| `genie` | `databricks-sdk>=0.60` | los scripts de Genie |
| `[tool.setuptools.packages.find] where = ["src"]` | layout `src/` | evita importar por accidente el código sin instalar |
| `[tool.pytest.ini_options]` | marker `spark` | los tests con Spark son lentos: en un PR corren sólo `-m "not spark"` |
| `[tool.ruff]` | línea de 110, reglas E/F/W/I/B/UP/SIM, excluye `notebooks` y `docs` | los notebooks tienen globals mágicos (`spark`, `dbutils`, `display`) que ruff marcaría como errores |
| `known-first-party = [..., "api", "chaos", "store", "tokens"]` | módulos de la Azure Function | la Function corre con su carpeta como raíz y los importa por nombre suelto; isort tiene que saber que son "propios" |

Para trabajar en local: `uv venv -p 3.12 && uv pip install -e ".[dev,azure,genie]"` (con Java 17 para Spark).
Si el Python del sistema es otra versión, Spark se queja con `PYTHON_VERSION_MISMATCH`: se arregla exportando
`PYSPARK_PYTHON` al Python del venv.

## 3. `src/andes_bank/config.py`: el centro de todo

Un único objeto inmutable, `ProjectConfig`, responde todas las preguntas de "¿cómo se llama?" y "¿dónde está?".
Ninguna otra parte del código escribe a mano un `andes_bank.silver.xxx` o un `/Volumes/...`.

### Constantes de módulo

- `LAYERS = ("sim", "landing", "bronze", "silver", "gold", "semantic", "ops")`: las 7 capas. `schema()` rechaza
  cualquier otra (un typo en una capa falla enseguida, no crea un esquema nuevo).
- `CONTAINERS`: clave lógica → contenedor de Azure (`cards_legacy` → `cards-legacy`).
- `CORE_SOURCES`, `CARD_SOURCES`: las fuentes del MVP.
- `is_databricks()`: mira `DATABRICKS_RUNTIME_VERSION`. Se usa para ramas que sólo existen allá (Auto Loader,
  Liquid Clustering, `CREATE VOLUME`); esas ramas llevan `# pragma: no cover`.

### `ScaleProfile`: el banco a SF 1

Los parámetros "de negocio" del generador, a escala 1: 500.000 clientes, 20.000 comercios, 40 % con caja en dólares,
15 % con cuenta corriente, 60 % cobra el sueldo en el banco, 35 % con tarjeta de crédito, 85 % de las cajas en pesos
con débito, 3,5 movimientos propios por cuenta y mes, 1,6 compras por tarjeta y mes, 1,5 % de duplicados de la fusión.

### `ProjectConfig`

| campo | default | para qué |
|---|---|---|
| `catalog` | `andes_bank` | catálogo de UC. En local vale `spark_catalog` (Delta OSS sin UC) |
| `schema_prefix` | `""` | en local y en los tests: `andes_` o `t12345_` para que cada test tenga sus esquemas |
| `volume` | `raw` | el Volume de landing |
| `base_path` | `None` | en local, una carpeta temporal en vez de `/Volumes/...` |
| `scale_factor` | `0.1` | multiplica **clientes** (y con ellos todo lo demás) |
| `start_date`, `end_date` | 2023-01-01 → 2026-10-20 | el período del banco |
| `seed` | 1998 | año de fundación; la semilla de todo el azar |
| `storage_account` | `""` | la cuenta de Azure (sale del secret scope) |

**Métodos:**

- `schema(layer)` → `andes_bank.silver` (o `t123_silver` en local). `table(layer, name)` → `andes_bank.silver.transactions`.
- `root_path` → `/Volumes/andes_bank/landing/raw`. Sobre él: `landing_path(system, source)` (lo copiado de Azure),
  `staging_path(name)` (lo que deja el generador antes de subir), `checkpoint_path`, `schema_location` (Auto Loader)
  y `errors_path` (archivos corruptos).
- `scaled(n)`, `n_customers`, `n_merchants` (mínimo 200, para que el Zipf de comercios tenga sentido a escala chica),
  `n_days`.

**Por qué el scale factor escala clientes y no días:** la historia del banco tiene hitos en fechas fijas (devaluación
de dic-2023, migración del core en mar-2024, CRM en jun-2024, API v2 en abr-2025, `wallet_type` en mar-2026). Si se
achicara el período, a escala chica desaparecerían los problemas que hay que aprender a resolver.

**Funciones:**

- `local_config(base_path, scale_factor=0.002)`: 1.000 clientes, catálogo `spark_catalog`, prefijo `andes_`. Es lo que
  usan los tests.
- `config_from_widgets(dbutils)`: en Databricks, lee los widgets (que en un job llenan los *job parameters*). Si un
  widget no existe o viene vacío, usa el default. La cuenta de storage sale del widget o de `ANDES_STORAGE_ACCOUNT`.

## 4. `src/andes_bank/setup.py`

- `SCHEMA_COMMENTS`: el comentario de cada esquema (lo ve quien navega el catálogo y lo lee Genie).
- `create_namespace(spark, cfg)`: `CREATE SCHEMA IF NOT EXISTS` para las 7 capas y `CREATE VOLUME IF NOT EXISTS
  landing.raw`. **El catálogo no se crea acá**: Free Edition no deja crearlo por API, se crea una vez por SQL
  (`CREATE CATALOG IF NOT EXISTS andes_bank`).
- `create_ops_tables(spark, cfg)`: crea **vacías** `ops.api_calls`, `api_reconciliation`, `landing_files`,
  `dq_results`, `silver_watermarks`, `api_watermarks` y `silver.quarantine`, con los esquemas que importa de cada
  módulo dueño.

  **Cómo se llegó a esto (bitácora R11):** la tarea `governance_semantic` creaba una vista sobre `ops.dq_results`,
  pero esa tabla la llena el quality gate, que corre *después*. Primera corrida: tabla no encontrada. Crear todas las
  tablas de `ops` en `setup` hace que el orden de las tareas deje de importar.

## 5. `src/andes_bank/sql_runner.py`

Los `.sql` de `sql/` usan variables: `${catalog}`, `${bronze}`, `${silver}`, `${gold}`, `${semantic}`, `${ops}`.

- `render(text, cfg)`: reemplaza cada `${x}` por el nombre real (`${gold}` → `andes_bank.gold`). Una variable
  desconocida queda tal cual (no explota en silencio cambiándola por vacío).
- `split_statements(text)`: parte el archivo en sentencias por el `;` al final de línea, **salvo dentro de un bloque
  `$$ … $$`**: las metric views llevan YAML adentro y el YAML puede tener `;`. Las líneas de comentario `--` se saltean.
- `run_sql_file(spark, cfg, path)`: corre sentencia por sentencia. Si una falla, el error dice **qué archivo, qué
  número de sentencia y los primeros 400 caracteres** de la sentencia: encontrar el problema lleva segundos.
- `run_folder(spark, cfg, folder)`: corre todos los `.sql` de la carpeta **en orden alfabético** (por eso los archivos
  se llaman `01_…`, `02_…`: el orden importa, las máscaras dependen de las funciones que se crean antes).

**Por qué no un notebook SQL:** así el SQL se versiona como texto plano, se corre igual en el job y en los tests
(`test_sql_runner.py` verifica que todos los archivos del proyecto se parten bien) y las variables permiten otro
catálogo (por ejemplo `andes_bank_sf1` para la prueba de volumen).

## 6. `src/andes_bank/utils/`

### `delta.py`: escrituras idempotentes

| función | qué hace | dónde se usa |
|---|---|---|
| `create_if_not_exists(spark, name, df, cluster_by)` | crea la tabla vacía con el esquema de `df`; en Databricks con **Liquid Clustering** (`clusterBy`), que reemplaza a particionar + ZORDER | antes de cada MERGE |
| `merge_insert_only(spark, source, target, keys)` | MERGE que **sólo inserta** las claves que no existen: la primera copia que llegó gana | movimientos y saldos (DAT-01: re-envíos) |
| `merge_upsert_when(…, update_when)` | inserta lo nuevo y actualiza lo existente **sólo si** se cumple una condición | autorizaciones: la v2 pisa a la v1, nunca al revés |
| `replace_where(spark, df, target, condition)` | reemplaza atómicamente las filas que cumplen la condición | bronze de APIs (un día), FX (un rango), watermarks |
| `get_watermark` / `set_watermark` | último `_ingested_at` procesado por entidad, en `ops.silver_watermarks` | silver incremental |

**El error que hay detrás de `replace_where` (bitácora R09):** al principio se escribía con
`df.write.mode("overwrite").option("replaceWhere", cond).saveAsTable(t)`. En el test local faltaban ~20 mil
autorizaciones. La causa: en Spark OSS el formato por defecto de `saveAsTable` es **Parquet**, no Delta; las tablas
locales eran Parquet y la opción `replaceWhere` (que es de Delta) se ignoraba **en silencio**, así que cada lote de días
pisaba la tabla entera. Dos arreglos: `spark.sql.sources.default=delta` en la sesión local y `writeTo(t).overwrite(cond)`,
que es explícito y se comporta igual en los dos lados. Lección: el entorno local tiene que parecerse a Databricks en
lo que importa.

### `ids.py`: CUIT/CUIL y CBU con su dígito verificador real

Cada algoritmo existe dos veces: en **Python puro** (para los tests y para entenderlo) y como **expresión de columna
de Spark** (para generar y validar millones de filas sin UDF: una UDF de Python saca cada fila de la JVM y es 10 a 100
veces más lenta). `test_ids.py` verifica que las dos versiones den lo mismo.

- **CUIT/CUIL:** prefijo (20 varón, 27 mujer, 30 empresa) + 8 dígitos + verificador módulo 11 con pesos
  `5,4,3,2,7,6,5,4,3,2`. Si el verificador da 10, AFIP cambia el prefijo a 23 (personas) o 33 (empresas). Funciones:
  `cuit_check_digit`, `build_cuit`, `is_valid_cuit`, y en Spark `cuit_col`, `cuit_company_col`, `cuit_valid_col`
  (acepta con o sin guiones y exige un prefijo válido).
- **CBU:** 22 dígitos en dos bloques. Bloque 1: banco (3) + sucursal (4) + verificador con pesos `7,1,3,9,7,1,3`.
  Bloque 2: cuenta (13) + verificador con pesos `3,9,7,1,…`. El código de banco es **`998`, que no existe**: ningún
  CBU generado puede coincidir con uno real. Funciones: `build_cbu`, `is_valid_cbu`, `cbu_col`, `cbu_valid_col`.

Se usan en el generador (para crear identificadores válidos) y en silver (para marcar `cuit_valido` y `cbu_valido`,
que miden DAT-07).

### `spark.py`

- `get_spark()`: en Databricks devuelve la sesión activa; en local crea una.
- `build_local_spark(app_name, warehouse_dir)`: Spark local con Delta (`configure_spark_with_delta_pip`), 8
  particiones de shuffle (lo justo para datos chicos), **zona horaria UTC** (igual que la sesión del job) y formato por
  defecto Delta (R09). Con `warehouse_dir` usa un metastore Derby persistente. Con
  `ANDES_DELTA_JARS_IN_CLASSPATH=1` no baja los jars de Maven (para entornos sin salida a Maven).

## 7. Archivos de la raíz

| archivo | qué es |
|---|---|
| `README.md` | la portada: estado del MVP, índice de `docs/`, el diagrama panorámico |
| `.gitignore` | cachés de Python, `spark-warehouse/`, `metastore_db/`, `.databricks/` (estado local del bundle), `local.settings.json` (secretos locales de la Function), `node_modules/` y los PNG de los diagramas (se versiona el SVG) |
| `databricks.yml`, `resources/` | el Asset Bundle: ver [09](09_orquestacion.md) |
| `.assistant_instructions.md`, `.assistant/` | reglas para Genie Code: ver [11](11_scripts_docs_y_diagramas.md) |
