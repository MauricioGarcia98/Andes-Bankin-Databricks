# 04 · Etapa 02 · Cimientos del repo: paquete, configuración y CUIT/CBU

## 🎯 Objetivo

Dejar listo el esqueleto sobre el que se apoya todo lo demás:

- un **paquete Python** (`andes_bank`) que se instala con `pip install -e .` y se testea con `pytest`;
- **una sola configuración** (`ProjectConfig`) que resuelve nombres de tablas, rutas y escala;
- los **identificadores argentinos** (CUIT/CUIL y CBU) con su dígito verificador real;
- **Spark + Delta en local**, que se comporte como Databricks en lo que importa.

**Archivos:** `pyproject.toml`, `src/andes_bank/config.py`, `src/andes_bank/utils/ids.py`,
`src/andes_bank/utils/spark.py`, `tests/conftest.py`, `tests/test_config.py`, `tests/test_ids.py`.

## 📚 Conceptos

- **Paquete + notebooks delgados:** la lógica vive en `src/andes_bank` (testeable); los notebooks sólo llaman al
  paquete. Un notebook no se puede testear con `pytest`; una función, sí.
- **Unity Catalog, tres niveles:** `catálogo.esquema.tabla` → `andes_bank.silver.transactions`. Las capas son
  esquemas: `sim`, `landing`, `bronze`, `silver`, `gold`, `semantic`, `ops`.
- **`scale_factor`:** escala la **cantidad de clientes**, no los días. Así la historia del banco (devaluación,
  migración del core, API v2) está completa a cualquier escala. SF 0,1 = 50 mil clientes, ~17 M movimientos.
- **Dígito verificador:** el último dígito de un CUIT o de un CBU se calcula con los demás (módulo 11 / módulo 10).
  Sirve para detectar un número mal tipeado, y el banco tiene que validarlo (DAT-07).

## 🧱 Paso a paso

### 1. El paquete (`pyproject.toml`)

```toml
[project]
name = "andes_bank"
requires-python = ">=3.10"
dependencies = []                    # en Databricks ya están pyspark y delta

[project.optional-dependencies]
dev = ["pyspark==4.0.1", "delta-spark==4.0.0", "pytest>=8", "ruff>=0.6", "pyyaml>=6"]

[tool.pytest.ini_options]
markers = ["spark: necesita Spark local (más lento)"]
```

`dependencies` vacío a propósito: en Databricks el runtime ya trae Spark. Las versiones de `dev` tienen que ser las
mismas que las del runtime serverless.

### 2. La configuración (`config.py`)

```python
@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = "andes_bank"
    schema_prefix: str = ""          # en local: "andes_" para no chocar con otras bases
    scale_factor: float = 0.1
    start_date: date = date(2023, 1, 1)
    end_date: date = date(2026, 10, 20)
    seed: int = 1998                 # año de fundación del banco

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"
```

Nadie escribe `"andes_bank.silver.transactions"` a mano: todo pasa por `cfg.table("silver", "transactions")`. En los
tests, `local_config()` usa `spark_catalog` y un prefijo por test, así cada test tiene sus propias tablas.

### 3. CUIT y CBU (`utils/ids.py`)

Dos versiones de cada función: **Python puro** (para entender y testear) y **columnas de Spark** (para generar y
validar millones de filas):

```python
def cuit_check_digit(first10: str) -> int:
    total = sum(int(d) * w for d, w in zip(first10, CUIT_WEIGHTS, strict=True))
    dv = 11 - total % 11
    return 0 if dv == 11 else dv           # 10 = no hay dígito válido: AFIP cambia el prefijo a 23
```

### 4. Spark local que se parece a Databricks (`utils/spark.py`)

```python
.config("spark.sql.session.timeZone", "UTC")
# en Databricks el formato por defecto es Delta; en Spark OSS es Parquet. Sin esto, saveAsTable crea tablas
# Parquet en local y las opciones de Delta (replaceWhere) se ignoran EN SILENCIO (bitácora R09).
.config("spark.sql.sources.default", "delta")
```

### 5. Los tests

```python
def test_cuit_conocido():
    assert build_cuit(12345678, female=False) == "20123456786"     # ejemplo de manual
    assert not is_valid_cuit("20-12345678-5")

@pytest.mark.spark
def test_columnas_spark_igual_que_python(spark): ...               # la versión Spark da lo mismo que la Python
```

## 🤔 Por qué así

- **¿Por qué no UDFs de Python para el CUIT?** Una UDF saca cada fila de la JVM a Python y la vuelve a meter: con
  17 M de filas es 10 a 100 veces más lenta que una expresión de Spark.
- **¿Por qué `frozen=True`?** La configuración no cambia a mitad de una corrida; si alguien intenta pisarla, falla.
- **¿Por qué tests con y sin Spark?** Los rápidos (`-m "not spark"`) corren en cada PR en segundos; los de Spark
  tardan minutos y corren en `main`.

## 🧪 Probalo

```bash
pip install -e ".[dev,azure]"
pytest -q -m "not spark"
pytest -q tests/test_ids.py          # incluye el de Spark: la primera vez baja los jars de Delta
ruff check src tests
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `JAVA_GATEWAY_EXITED` | falta Java 17 o `JAVA_HOME` | instalá Java 17 |
| `PYTHON_VERSION_MISMATCH: worker 3.13, driver 3.12` | Spark lanza los workers con otro Python | `export PYSPARK_PYTHON=$(which python)` |
| Un `replaceWhere` "no hace nada" en local | tablas Parquet en vez de Delta | `spark.sql.sources.default=delta` (R09) |

## ✅ Punto de control

`git diff etapa-02 -- src/andes_bank/config.py src/andes_bank/utils/ tests/test_ids.py` no muestra diferencias de lógica.

## 🏋️ Ejercicio

Agregá `is_valid_dni(dni: str)` (7 u 8 dígitos, acepta puntos) con su test, y su versión en columnas de Spark.
