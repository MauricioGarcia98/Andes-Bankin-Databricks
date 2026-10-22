# 04 · Etapa 02 · Cimientos del repo: las carpetas, el paquete, la configuración y CUIT/CBU

> **Antes:** [03 · Spike](03_spike_free_edition.md). **Tiempo:** 4 a 5 horas (es el capítulo más largo de la parte 0:
> todo lo que viene se apoya acá). **Tag al terminar:** `etapa-02`.

## 🎯 Objetivo

Construir el **esqueleto** sobre el que se apoya todo el proyecto. Al terminar, tu repo tiene esto:

```
andes-bank/
├── .devcontainer/            (capítulo 02)
├── docs/adr/                 (capítulo 03)
├── .gitignore                qué archivos NO se guardan en Git
├── pyproject.toml            la "ficha" del paquete: nombre, dependencias, reglas de tests y de estilo
├── src/
│   └── andes_bank/           EL PAQUETE: todo el código del proyecto vive acá adentro
│       ├── __init__.py       marca la carpeta como paquete
│       ├── config.py         la configuración central: nombres de tablas, rutas, escala
│       └── utils/
│           ├── __init__.py
│           ├── ids.py        CUIT/CUIL y CBU con su dígito verificador real
│           └── spark.py      cómo se crea Spark en el codespace (y en Databricks)
└── tests/
    ├── conftest.py           lo que comparten todos los tests (una sesión de Spark)
    ├── test_config.py        prueba la configuración
    └── test_ids.py           prueba CUIT y CBU
```

Y lo más importante: **`pytest` corre y pasa** en tu codespace. Desde acá, cada cosa que construyas va a tener su test.

**Por qué esto va primero:** cada pieza del banco necesita saber **cómo se llama su tabla** (`config.py`), los clientes
necesitan **CUIT** y las cuentas **CBU** (`ids.py`), y todo se prueba con **Spark** en el codespace (`spark.py`). Si
estas tres cosas no están, no se puede escribir ni probar nada más.

## 📚 Conceptos

### Programa, módulo y paquete

- Un **módulo** es un archivo `.py`. `config.py` es el módulo `config`.
- Un **paquete** es una carpeta con módulos adentro (y un archivo `__init__.py`). `andes_bank` es un paquete; `utils`
  es un paquete dentro de otro.
- Desde cualquier otro archivo se usa con `import`: `from andes_bank.config import ProjectConfig` = "del módulo
  `config` del paquete `andes_bank`, traeme `ProjectConfig`".

### Por qué todo el código va en un paquete y no en notebooks

Databricks invita a escribir todo en **notebooks**. Este proyecto no lo hace: los notebooks (capítulo 07 en adelante)
tienen 10 líneas y sólo **llaman** a funciones del paquete. Razones:

- **Se puede probar.** Una función de un paquete se prueba con `pytest` en el codespace, en segundos, sin Databricks.
  Un notebook no.
- **Se puede revisar.** El cambio en un `.py` se lee en GitHub línea por línea; el de un notebook con resultados, no.
- **Se reutiliza.** La misma función la usan el notebook, el test y cualquier script.

### Por qué la carpeta `src/`

El paquete podría estar en la raíz (`andes_bank/`), pero dentro de `src/` obliga a **instalarlo** para usarlo. Así los
tests prueban el paquete instalado, igual que lo usa Databricks, y no "lo que haya suelto en la carpeta".

### Test

Un **test** es una función que **usa** tu código y **verifica** (con `assert`) que el resultado es el esperado:

```python
def test_suma():
    assert 2 + 2 == 4        # si no se cumple, el test falla y te dice qué esperaba y qué obtuvo
```

`pytest` es el programa que encuentra todas las funciones que empiezan con `test_` en los archivos `test_*.py` y las
ejecuta. Cada test que pasa es una garantía: si mañana cambiás algo y lo rompés, el test te avisa.

### Spark, en una línea

**Spark** procesa tablas enormes repartiendo el trabajo entre muchas máquinas. Se programa con **DataFrames**: tablas
sobre las que se encadenan operaciones (`filtrar`, `agregar columna`, `agrupar`). Cada columna se describe con
**expresiones** (`F.col("dni")`, `F.concat(...)`), que Spark ejecuta en paralelo. En el codespace corre en "modo local"
(una sola máquina), pero el código es el mismo que en Databricks.

### Unity Catalog: nombres de tres partes

En Databricks cada tabla tiene un nombre de **tres niveles**: `catálogo.esquema.tabla`, por ejemplo
`andes_bank.silver.transactions`. El **catálogo** es el proyecto; el **esquema** es la capa (`bronze`, `silver`,
`gold`…); la **tabla**, la tabla. Este proyecto tiene siete esquemas:

| esquema | qué guarda |
|---|---|
| `sim` | la "verdad" del banco simulado (lo que genera el generador). El lakehouse **nunca** la lee |
| `landing` | los archivos tal cual llegaron de Azure (un *Volume*) |
| `bronze` | los datos crudos, tabla por fuente |
| `silver` | los datos limpios |
| `gold` | el modelo para el negocio |
| `semantic` | métricas y funciones para Genie |
| `ops` | la operación: llamadas a APIs, calidad, controles |

### El *scale factor*

El banco puede tener 50 mil o 500 mil clientes. El **scale factor** (SF) multiplica la **cantidad de clientes**: SF 1 =
500 mil, SF 0,1 = 50 mil (el que se usa siempre en Free Edition), SF 0,002 = 1.000 (el de los tests). **No**
cambia los días: la historia del banco (la devaluación de dic-2023, la migración del core en mar-2024…) tiene que estar
completa a cualquier escala, porque ahí están los problemas que se aprenden a resolver.

### Dígito verificador

El último dígito de un **CUIT** (o CUIL) y de un **CBU** no es libre: **se calcula** a partir de los demás. Sirve para
detectar errores de tipeo: si alguien cambia un número, el verificador deja de coincidir. Los datos del banco vienen
con CUIT mal cargados a propósito (problema DAT-07 del catálogo) y el proyecto tiene que detectarlos: por eso el
algoritmo tiene que ser **el real**.

## 🧱 Paso a paso

Abrí tu codespace de `andes-bank`. Todo lo que sigue se hace en la terminal y el editor de ahí.

### 1. Las carpetas

```bash
cd /workspaces/andes-bank
mkdir -p src/andes_bank/utils tests
```

| carpeta | para qué | qué NO va acá |
|---|---|---|
| `src/` | contiene el paquete (y sólo el paquete) | notebooks, scripts sueltos, datos |
| `src/andes_bank/` | **el paquete**: todo el código de datos del proyecto, en módulos | código de la API de Azure (ese va en `azure/`, capítulo 09: corre en otro lado) |
| `src/andes_bank/utils/` | utilidades que usan todas las capas: identificadores, Spark, Delta | lógica de negocio de una capa (va en su carpeta: `generator/`, `silver/`…) |
| `tests/` | los tests | código que se usa fuera de los tests |

Las demás carpetas del proyecto (`notebooks/`, `sql/`, `genie/`, `azure/`, `resources/`, `scripts/`) se crean en el
capítulo donde se usan por primera vez.

### 2. `.gitignore`: lo que Git no tiene que guardar

`code .gitignore`:

```gitignore
__pycache__/
*.egg-info/
.venv/
.pytest_cache/
.ruff_cache/
spark-warehouse/
metastore_db/
derby.log
.databricks/
dist/
build/
local.settings.json
.python_packages/
*.zip
node_modules/
docs/diagramas/*.png
```

Cada línea es un patrón de nombres que Git **ignora** (no los muestra en `git status` ni los sube):

| patrón | qué es | por qué no se guarda |
|---|---|---|
| `__pycache__/`, `*.egg-info/`, `dist/`, `build/` | archivos que Python genera solo al ejecutar o instalar | se regeneran; guardarlos sólo mete ruido |
| `.venv/` | un entorno virtual de Python (si alguien lo crea) | depende de cada máquina |
| `.pytest_cache/`, `.ruff_cache/` | memoria de `pytest` y `ruff` | ídem |
| `spark-warehouse/`, `metastore_db/`, `derby.log` | las tablas y el catálogo que crea **Spark local** | son datos de prueba, se recrean en cada corrida |
| `.databricks/` | el estado local de la CLI al desplegar (capítulo 20) | es de tu máquina |
| `local.settings.json` | la configuración local de la Azure Function (capítulo 09) | **tiene secretos** |
| `.python_packages/`, `*.zip` | paquetes armados para desplegar la Function | se generan al desplegar |
| `node_modules/`, `docs/diagramas/*.png` | dependencias de Node y PNG de los diagramas | se reinstalan / se regeneran |

**Qué NO hace:** no borra nada, y no saca de Git algo que ya se guardó antes (para eso es `git rm --cached`).

### 3. `pyproject.toml`: la ficha del paquete

`code pyproject.toml`. Es un archivo **TOML**: secciones entre corchetes y `clave = valor`.

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"
```

- Le dice a `pip` **con qué herramienta** se instala el paquete: `setuptools` (la estándar). No hace falta entender
  más: es siempre igual.

```toml
[project]
name = "andes_bank"
version = "0.1.0"
description = "Andes Bank: lakehouse bancario E2E en Databricks Free Edition con Genie en el centro."
readme = "README.md"
requires-python = ">=3.10"
license = { text = "MIT" }
authors = [{ name = "Mauricio García" }]
dependencies = []
```

- `name`: el nombre con el que se importa: `import andes_bank`.
- `requires-python = ">=3.10"`: el código usa sintaxis de Python 3.10 o más nuevo (por ejemplo `str | None`).
- `authors`: poné tu nombre.
- **`dependencies = []` vacío, a propósito.** Son las bibliotecas que `pip` instalaría **siempre**. En Databricks el
  runtime ya trae Spark y Delta en versiones exactas; si el paquete pidiera otra versión, `pip` la instalaría encima y
  rompería el runtime. Por eso todo lo demás va en grupos **opcionales**:

```toml
[project.optional-dependencies]
dev = [
  "pyspark==4.0.1",
  "delta-spark==4.0.0",
  "pytest>=8",
  "ruff>=0.6",
  "pyyaml>=6",
]
azure = ["azure-storage-blob>=12.20", "requests>=2.31"]
genie = ["databricks-sdk>=0.60"]
```

| grupo | qué trae | cuándo se usa |
|---|---|---|
| `dev` | Spark 4.0.1 + Delta 4.0.0 (Spark local), `pytest` (tests), `ruff` (revisor de estilo), `pyyaml` (leer YAML) | en el codespace y en la CI |
| `azure` | la biblioteca de Azure Storage y `requests` (pedidos HTTP) | subir archivos a Azure y llamar APIs |
| `genie` | el SDK de Databricks | los scripts que crean los Genie Agents |

- **Por qué versiones exactas (`==`) en Spark y Delta:** cada versión de Delta funciona con **una** versión de Spark.
  Si mezclás, Spark no arranca (`ClassNotFoundException`). Y tienen que coincidir con las del serverless de Databricks,
  para que lo que pasa en el codespace pase allá.
- Se instalan con `pip install -e ".[dev,azure,genie]"` (el `post_create.sh` del capítulo 02 ya lo hace cuando existe
  este archivo).

```toml
[tool.setuptools.packages.find]
where = ["src"]
```

- "Los paquetes están dentro de `src/`". Sin esto, `pip` no encuentra `andes_bank`.

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra -q"
markers = [
  "spark: necesita Spark local (más lento)",
]
```

- `testpaths`: dónde buscar tests.
- `addopts = "-ra -q"`: opciones que se agregan siempre: `-q` = salida corta (un punto por test que pasa), `-ra` = al
  final, un resumen de los que fallaron o se saltearon.
- `markers`: una **marca** `spark` para los tests que necesitan Spark (tardan más). Así se puede correr
  `pytest -m "not spark"` (sólo los rápidos) o `pytest -m spark` (sólo los lentos).

```toml
[tool.ruff]
line-length = 110
target-version = "py310"
extend-exclude = ["notebooks", "docs"]

[tool.ruff.lint]
select = ["E", "F", "W", "I", "B", "UP", "SIM"]
ignore = ["E501"]

[tool.ruff.lint.isort]
# los módulos de la Azure Function se importan por nombre (la Function corre con su carpeta como raíz)
known-first-party = ["andes_bank", "api", "chaos", "store", "tokens"]
```

- **`ruff`** es un **linter**: lee el código **sin ejecutarlo** y marca errores probables (una variable que no se usa,
  un import que falta) y problemas de estilo. Es lo primero que corre la CI.
- `line-length = 110`: largo máximo de línea.
- `extend-exclude`: no revisa `notebooks/` (usan variables "mágicas" de Databricks como `spark` y `dbutils`, que ruff
  marcaría como no definidas) ni `docs/`.
- `select`: qué familias de reglas revisa (errores, imports ordenados, posibles bugs, sintaxis moderna,
  simplificaciones). `ignore = ["E501"]`: no se queja por líneas largas (ya lo controla `line-length` al formatear).
- `known-first-party`: qué imports son "del proyecto" para ordenarlos aparte. `api`, `chaos`, `store` y `tokens` son
  módulos de la API de Azure (capítulo 09): se agregan desde ahora para no tener que volver a este archivo.

### 4. `src/andes_bank/__init__.py`: "esta carpeta es un paquete"

`code src/andes_bank/__init__.py`:

```python
"""Andes Bank: lakehouse bancario E2E en Databricks Free Edition con Genie en el centro."""

__version__ = "0.1.0"
```

- Un archivo `__init__.py` dentro de una carpeta la convierte en **paquete**: Python sabe que puede importar cosas de
  adentro. Se ejecuta la primera vez que alguien hace `import andes_bank`.
- La primera línea, entre triple comillas, es un **docstring**: la documentación del módulo. No hace nada al ejecutar;
  la leen las personas y las herramientas (`help(andes_bank)`).
- `__version__`: la versión del paquete, por convención en esta variable.

Y el de `utils`, **vacío** (sólo tiene que existir):

```bash
touch src/andes_bank/utils/__init__.py
```

### 5. `src/andes_bank/config.py`: la configuración central

**Qué es:** un único lugar que responde "¿cómo se llama esta tabla?", "¿dónde está este archivo?" y "¿cuántos clientes
genero?". **Ninguna otra parte del código escribe a mano** `andes_bank.silver.transactions` o `/Volumes/...`: se lo
pide a la configuración.

**Por qué:** el mismo código corre en tres lugares con nombres distintos: en Databricks (`andes_bank.silver.x`), en los
tests del codespace (sin catálogo, con un prefijo por test para que no se pisen: `t123_silver.x`) y en un catálogo de
prueba (`andes_bank_sf1.silver.x`). Si los nombres estuvieran repetidos por todo el código, cambiar de lugar sería
imposible.

`code src/andes_bank/config.py` y escribilo de a bloques.

#### 5.1 El docstring y los imports

```python
"""Configuración central de Andes Bank.

Un único objeto `ProjectConfig` resuelve:
  * los nombres de 3 niveles (catalog.schema.table) de Unity Catalog,
  * las rutas de los Volumes (lo que se copia desde Azure, checkpoints, errores de API),
  * los contenedores de Azure Blob donde "viven" los sistemas origen,
  * el `scale_factor`, que escala la CANTIDAD DE CLIENTES (y con ellos cuentas, tarjetas y movimientos).

¿Por qué escalar clientes y no días? Porque la historia del banco (devaluación de dic-2023, migración del
core en mar-2024, CRM nuevo en 2024, API v2 en 2025) tiene que estar completa a cualquier escala.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
```

- El docstring explica el **porqué** del módulo. En este proyecto cada módulo empieza así: es lo primero que lee alguien
  que llega al archivo.
- `from __future__ import annotations`: permite escribir los tipos (`str | None`) de forma moderna en cualquier versión
  de Python 3.10+. Va siempre primero.
- `os`: para leer variables de entorno. `dataclass` y `field`: para definir "fichas" de datos (abajo). `date`,
  `datetime`, `timedelta`: fechas, fecha y hora, y diferencias de tiempo. `ZoneInfo`: zonas horarias (Argentina).

#### 5.2 Las constantes

```python
# sim = "los sistemas origen" (la verdad que genera el generador; el lakehouse NO la lee, sólo los tests y
# las conciliaciones de control). El resto son las capas del lakehouse.
LAYERS = ("sim", "landing", "bronze", "silver", "gold", "semantic", "ops")
```

- Una **constante** es un valor que no cambia; por convención se escribe en MAYÚSCULAS.
- `(...)` es una **tupla**: una lista que no se puede modificar. Son las 7 capas (los esquemas de Unity Catalog).

```python
# Contenedores de Azure Blob: cada uno es un sistema origen distinto.
CONTAINERS = {
    "core": "core",                    # exportes nocturnos del core bancario (CSV / Parquet)
    "cards_legacy": "cards-legacy",    # exportes diarios de la integración vieja con el procesador (API v1)
    "api_data": "api-data",            # lo que sirve la API de tarjetas v2 (páginas pre-armadas por día)
}
```

- Un **diccionario**: nombre "lógico" en el código → nombre real del contenedor en Azure. Azure no acepta `_` en los
  nombres de contenedores (por eso `cards-legacy`), y Python no acepta `-` en los nombres de variables: el diccionario
  traduce. (Hay dos contenedores más, `fx-mirror` y `crm-data`, que se usan directo por su nombre en los capítulos 10
  y 11).

```python
# Último día de la historia que genera `generate_truth` de una sola vez. Después, el job "día nuevo" simula a pedido
# los días siguientes (generator/daily.py), para mostrar el lakehouse procesando datos nuevos.
HISTORY_END = date(2026, 10, 20)
TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")
```

- `HISTORY_END`: hasta dónde llega la historia que se genera de una vez (capítulos 05 y 06). Desde ahí, un job puede
  **simular a pedido** días siguientes, para mostrar el pipeline procesando datos nuevos (capítulo 26).
- `TZ_AR`: la zona horaria argentina (UTC−3 todo el año). Se usa para saber qué día es "ayer" en Argentina.

```python
# Fuentes del MVP (una carpeta por fuente dentro de su contenedor o endpoint).
CORE_SOURCES = ("customers", "accounts", "transactions", "balances")
CARD_SOURCES = ("authorizations", "settlements")
```

- Las fuentes del MVP: del core bancario, clientes, cuentas, movimientos y saldos; del procesador de tarjetas,
  autorizaciones y liquidaciones.

#### 5.3 ¿Estoy en Databricks?

```python
def is_databricks() -> bool:
    """True cuando el código corre dentro de un runtime de Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ
```

- `def nombre() -> bool:` define una **función** que devuelve un booleano (`True` o `False`). `-> bool` es una **anotación
  de tipo**: documenta qué devuelve (Python no lo obliga, pero el editor y ruff lo usan).
- `os.environ` es el diccionario de variables de entorno. Databricks define `DATABRICKS_RUNTIME_VERSION` en sus
  máquinas; tu codespace no. Así el mismo código sabe dónde está corriendo.
- **Para qué:** hay cosas que sólo existen en Databricks (Auto Loader, `CREATE VOLUME`, *Liquid Clustering*). El código
  pregunta `if is_databricks():` antes de usarlas, y en el codespace hace la versión local.

#### 5.4 `ScaleProfile`: el banco a escala 1

```python
@dataclass(frozen=True)
class ScaleProfile:
    """El banco a SF=1.

    500 mil clientes → ~800 mil cuentas → ~150 M movimientos y ~60 M autorizaciones de tarjeta entre 2023 y
    oct-2026. A SF 0,1 (el default en Free Edition) todo es 10 veces menor.
    """

    customers: int = 500_000
    merchants: int = 20_000
    usd_account_share: float = 0.40       # clientes con caja de ahorro en dólares
    checking_share: float = 0.15          # clientes con cuenta corriente (sobre todo pymes y premium)
    salary_share: float = 0.60            # clientes que cobran el sueldo en el banco
    credit_card_share: float = 0.35       # clientes con tarjeta de crédito
    debit_card_share: float = 0.85        # cajas de ahorro en pesos con tarjeta de débito
    txn_per_account_month: float = 3.5    # movimientos "propios" (sin sueldo ni compras con débito)
    card_purchases_month: float = 1.6     # compras por tarjeta por mes
    legacy_duplicate_share: float = 0.015 # clientes del NOA que también están como clientes de Cuyo (fusión 2008)
```

- Una **clase** es un molde para crear objetos. `@dataclass` (un **decorador**: modifica lo que está abajo) la
  convierte en una "ficha de datos": a partir de la lista de campos con su tipo y su valor por defecto, Python escribe
  solo el constructor, la comparación y la forma de imprimirla.
- `frozen=True`: **inmutable**. Una vez creado, un `ScaleProfile` no se puede modificar (`p.customers = 3` da error).
  Para una configuración es lo correcto: nadie la cambia por accidente a mitad de un proceso.
- `500_000`: el guion bajo es sólo para leer mejor; vale 500000.
- Son los **parámetros de negocio** del generador (capítulos 05 y 06): qué porcentaje de clientes tiene cada producto
  y cuánto se mueve cada cuenta. Están todos juntos para poder cambiar el banco sin tocar el código que lo genera.

#### 5.5 `ProjectConfig`: el objeto central

```python
@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = "andes_bank"
    schema_prefix: str = ""          # en local: "andes_" para no chocar con otras bases
    volume: str = "raw"
    base_path: str | None = None     # override local (por defecto /Volumes/...)
    scale_factor: float = 0.1
    start_date: date = date(2023, 1, 1)
    end_date: date = HISTORY_END
    seed: int = 1998                 # año de fundación del banco
    storage_account: str = ""        # cuenta de Azure Storage (se resuelve por variable o widget)
    profile: ScaleProfile = field(default_factory=ScaleProfile)
```

| campo | qué es | para qué |
|---|---|---|
| `catalog` | el catálogo de Unity Catalog | `andes_bank` en Databricks; `spark_catalog` en el codespace (Spark local no tiene Unity Catalog) |
| `schema_prefix` | un prefijo para los esquemas | en los tests, cada test usa uno distinto (`t123_`) y no se pisan las tablas |
| `volume` | el nombre del Volume de landing | `raw` |
| `base_path` | una carpeta para reemplazar `/Volumes/...` | en el codespace, una carpeta temporal |
| `scale_factor` | la escala | 0,1 por defecto |
| `start_date`, `end_date` | el período del banco | del 1-ene-2023 al fin de la historia |
| `seed` | la **semilla** del azar | con la misma semilla, el banco generado es siempre idéntico (capítulo 05). 1998 es el año en que se fundó |
| `storage_account` | la cuenta de Azure Storage | se completa en Databricks desde los secretos |
| `profile` | el `ScaleProfile` | `field(default_factory=ScaleProfile)` crea uno nuevo por cada config (un objeto como valor por defecto se escribe así en una dataclass) |

- `str | None` significa "un texto **o** nada (`None`)".

```python
    # ---------------------------------------------------------------- nombres
    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}. Válidas: {LAYERS}")
        if self.catalog == "spark_catalog":  # local: Delta OSS sin Unity Catalog
            return f"{self.schema_prefix}{layer}"
        return f"{self.catalog}.{self.schema_prefix}{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"
```

- Un **método** es una función que pertenece a una clase; `self` es el objeto mismo (para leer sus campos:
  `self.catalog`).
- `schema("silver")` → `"andes_bank.silver"` en Databricks; `"andes_silver"` en el codespace (sin catálogo, con
  prefijo).
- `raise ValueError(...)`: **lanza un error** a propósito. Si alguien escribe `cfg.schema("sliver")` (mal tipeado), falla
  enseguida con un mensaje claro, en vez de crear una capa nueva por error.
- `table("silver", "transactions")` → `"andes_bank.silver.transactions"`. **Es la función más usada del proyecto.**

```python
    # ------------------------------------------------------------------ rutas
    @property
    def root_path(self) -> str:
        if self.base_path:
            return self.base_path.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.schema_prefix}landing/{self.volume}"
```

- `@property`: se usa como un campo, sin paréntesis: `cfg.root_path`.
- En Databricks, un **Volume** es una carpeta de archivos dentro del catálogo, con ruta
  `/Volumes/<catálogo>/<esquema>/<volume>` → `/Volumes/andes_bank/landing/raw`. En el codespace, la carpeta temporal
  (`rstrip("/")` le saca una barra final si la tiene).

```python
    def landing_path(self, system: str, source: str) -> str:
        """Copia local (en el Volume) de lo que llegó de un sistema origen: core/transactions, cards/..."""
        return f"{self.root_path}/{system}/{source}"

    def staging_path(self, name: str) -> str:
        """Donde el generador deja los archivos antes de "subirlos" al sistema origen (Azure)."""
        return f"{self.root_path}/_staging/{name}"

    def checkpoint_path(self, name: str) -> str:
        return f"{self.root_path}/_checkpoints/{name}"

    def schema_location(self, name: str) -> str:
        return f"{self.root_path}/_schemas/{name}"

    def errors_path(self, name: str) -> str:
        return f"{self.root_path}/_errors/{name}"
```

Todas las carpetas del Volume, definidas en un solo lugar:

| método | carpeta | quién la usa |
|---|---|---|
| `landing_path("core", "movimientos")` | `.../raw/core/movimientos` | los archivos copiados de Azure (capítulo 12) |
| `staging_path(...)` | `.../raw/_staging/...` | el generador deja ahí los archivos antes de subirlos (capítulo 07) |
| `checkpoint_path(...)` | `.../raw/_checkpoints/...` | la "memoria" de Auto Loader: qué archivos ya leyó (capítulo 13) |
| `schema_location(...)` | `.../raw/_schemas/...` | el esquema que Auto Loader va descubriendo (capítulo 13) |
| `errors_path(...)` | `.../raw/_errors/...` | los archivos rotos que no pasan el borde (capítulo 12) |

Las que empiezan con `_` son "internas": no son datos que llegaron, son de la plataforma.

```python
    def blob_url(self, container_key: str) -> str:
        return f"https://{self.storage_account}.blob.core.windows.net/{CONTAINERS[container_key]}"
```

- La dirección de un contenedor de Azure: `https://<cuenta>.blob.core.windows.net/<contenedor>`.

```python
    # ---------------------------------------------------------------- volumen
    def scaled(self, n_real: int) -> int:
        """Cantidad a esta escala (mínimo 1)."""
        return max(1, int(round(n_real * self.scale_factor)))

    @property
    def n_customers(self) -> int:
        return self.scaled(self.profile.customers)

    @property
    def n_merchants(self) -> int:
        return max(200, self.scaled(self.profile.merchants))

    @property
    def n_days(self) -> int:
        return (self.end_date - self.start_date).days + 1
```

- `scaled(500_000)` con SF 0,1 → 50.000. `max(1, ...)` asegura al menos 1 (con SF muy chico daría 0).
- `n_merchants`: **mínimo 200 comercios**. Con 1.000 clientes (tests) darían 40 comercios, y las compras se concentran
  demasiado; 200 mantiene la mezcla de rubros realista.
- `n_days`: cuántos días tiene el período. Restar dos fechas da un `timedelta`; `.days` son sus días; `+ 1` porque se
  cuentan los dos extremos.

#### 5.6 La configuración de los tests y la de Databricks

```python
def local_config(base_path: str, scale_factor: float = 0.002, **kwargs) -> ProjectConfig:
    """Config para tests / ejecución local con Spark + Delta OSS (1.000 clientes)."""
    return ProjectConfig(
        catalog="spark_catalog",
        schema_prefix="andes_",
        base_path=base_path,
        scale_factor=scale_factor,
        **kwargs,
    )
```

- La configuración del codespace: sin catálogo de Unity Catalog, con una carpeta propia y 1.000 clientes (SF 0,002).
- `**kwargs` ("keyword arguments"): junta cualquier otro argumento con nombre y lo pasa tal cual. Así se puede pedir
  `local_config(carpeta, start_date=date(2026, 7, 1))` sin escribir un parámetro para cada campo.

```python
def yesterday_ar(now: datetime | None = None) -> date:
    """Ayer en hora argentina: el último día que el banco ya cerró."""
    return (now or datetime.now(TZ_AR)).astimezone(TZ_AR).date() - timedelta(days=1)


def parse_end_date(value: str) -> date:
    """`end_date` de un job: una fecha ISO o `ayer` (hasta ayer en hora argentina)."""
    return yesterday_ar() if value.strip().lower() == "ayer" else date.fromisoformat(value)
```

- `yesterday_ar()`: el día de ayer **en Argentina**. Importa: a las 22 h de Buenos Aires ya es el día siguiente en UTC
  (la hora de los servidores). `now or datetime.now(...)`: si le pasan una hora (los tests lo hacen, para que el
  resultado no dependa de cuándo corren) usa esa; si no, la de ahora.
- `parse_end_date("2026-10-20")` → esa fecha. `parse_end_date("ayer")` → ayer (sirve si algún día se quisiera correr
  con el calendario real; hoy los jobs usan fechas). `strip().lower()` acepta `" Ayer "` también.

```python
def config_from_widgets(dbutils) -> ProjectConfig:  # pragma: no cover - sólo Databricks
    """Construye la config a partir de los widgets/parámetros del job."""

    def get(name: str, default: str) -> str:
        try:
            value = dbutils.widgets.get(name)
            return value if value != "" else default
        except Exception:
            return default

    return ProjectConfig(
        catalog=get("catalog", "andes_bank"),
        scale_factor=float(get("scale_factor", "0.1")),
        start_date=date.fromisoformat(get("start_date", "2023-01-01")),
        end_date=parse_end_date(get("end_date", HISTORY_END.isoformat())),
        storage_account=get("storage_account", os.environ.get("ANDES_STORAGE_ACCOUNT", "")),
    )
```

- En Databricks, los parámetros de un job llegan a los notebooks como **widgets** (campos de texto). Esta función los
  lee y arma la configuración.
- `get` es una función **adentro** de otra: sólo se usa acá. Si el widget no existe o viene vacío, usa el valor por
  defecto. `try/except` hace que un widget faltante no rompa el notebook.
- Los widgets son siempre texto: `float(...)` convierte a número y `date.fromisoformat("2023-01-01")` a fecha.
- `# pragma: no cover`: avisa a las herramientas de cobertura de tests que esta función no se prueba en el codespace
  (necesita `dbutils`, que sólo existe en Databricks).

**Qué NO hace `config.py`:** no crea nada (ni tablas, ni carpetas, ni archivos) y no se conecta a ningún lado. Sólo
**calcula nombres**. Crear los esquemas es el trabajo de `setup.py` (capítulo 05).

### 6. `src/andes_bank/utils/ids.py`: CUIT/CUIL y CBU

**Qué es:** los algoritmos **reales** de los identificadores argentinos, para **generar** identificadores válidos (los
clientes del banco simulado necesitan CUIT y las cuentas CBU) y para **validar** los que llegan (y encontrar los mal
cargados: DAT-07).

**La idea clave del archivo:** cada algoritmo está **dos veces**:

1. en **Python común** (`build_cuit`, `is_valid_cbu`…): fácil de leer y de probar con un ejemplo;
2. como **expresión de Spark** (`cuit_col`, `cbu_valid_col`…): para aplicarlo a millones de filas.

¿Por qué no usar la versión Python dentro de Spark (una "UDF")? Porque Spark corre en Java: para usar una función de
Python tiene que sacar **cada fila** de Java, pasársela a Python y volver. Es entre 10 y 100 veces más lento. Una
expresión de Spark se ejecuta adentro de Java, en paralelo. Un test verifica que las dos versiones den **lo mismo**.

`code src/andes_bank/utils/ids.py`.

#### 6.1 Cabecera y constantes

```python
"""Identificadores argentinos con su dígito verificador real: CUIT/CUIL y CBU.

Hay dos versiones de cada función:
  * Python puro (`cuit_check_digit`, `is_valid_cbu`...): para tests y para entender el algoritmo.
  * Columnas de Spark (`cuit_col`, `cbu_valid_col`...): para generar y validar millones de filas sin UDFs
    (una UDF de Python saca los datos de la JVM fila por fila y es 10-100 veces más lenta).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

CUIT_WEIGHTS = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)
CBU_BLOCK1_WEIGHTS = (7, 1, 3, 9, 7, 1, 3)
CBU_BLOCK2_WEIGHTS = (3, 9, 7, 1, 3, 9, 7, 1, 3, 9, 7, 1, 3)
BANK_CODE = "998"  # código BCRA ficticio de Andes Bank (no existe en el sistema real)
```

- `Column` es el tipo de una expresión de columna de Spark; `functions as F` trae todas las funciones de columnas con
  el nombre corto `F` (`F.concat`, `F.substring`…). Es la convención de todo el mundo.
- Los **pesos**: los dos algoritmos multiplican cada dígito por un peso fijo y suman. Son los que publican AFIP (CUIT)
  y el BCRA (CBU).
- **`BANK_CODE = "998"`**: los tres primeros dígitos de un CBU son el código del banco. 998 **no existe**: así ningún
  CBU inventado por el proyecto puede ser el de una cuenta real.

#### 6.2 CUIT en Python

```python
# ------------------------------------------------------------------ CUIT/CUIL
def cuit_check_digit(first10: str) -> int:
    """Dígito verificador (módulo 11) de los primeros 10 dígitos. Devuelve 10 si no hay dígito válido."""
    total = sum(int(d) * w for d, w in zip(first10, CUIT_WEIGHTS, strict=True))
    dv = 11 - total % 11
    return 0 if dv == 11 else dv
```

El algoritmo con un ejemplo, el CUIT `20-12345678-?`:

```
dígitos:  2  0  1  2  3  4  5  6  7  8
pesos:    5  4  3  2  7  6  5  4  3  2
producto:10  0  3  4 21 24 25 24 21 16   → suma = 148
148 % 11 = 5          (% es "el resto de dividir": 148 = 11 × 13 + 5)
11 − 5 = 6            → el verificador es 6:  20-12345678-6
```

- `zip(first10, CUIT_WEIGHTS)` empareja cada dígito con su peso; `strict=True` falla si no son 10 (un error de
  programación, no de datos).
- `sum(int(d) * w for d, w in ...)`: multiplica cada par y suma (una **expresión generadora**: un `for` dentro de la
  suma).
- Si da 11, el verificador es 0. Si da **10**, no hay dígito válido: lo resuelve la función siguiente.

```python
def build_cuit(dni: int, female: bool) -> str:
    """CUIL de una persona humana: 20 (varón) / 27 (mujer); si el dígito da 10, AFIP usa prefijo 23."""
    prefix = "27" if female else "20"
    body = f"{prefix}{dni:08d}"
    dv = cuit_check_digit(body)
    if dv == 10:
        body = f"23{dni:08d}"
        dv = 4 if female else 9
    return f"{body}{dv}"
```

- El CUIL de una persona es: prefijo (20 varón, 27 mujer) + DNI con 8 dígitos + verificador.
- `{dni:08d}`: escribe el número con 8 dígitos, completando con ceros a la izquierda (`1234567` → `01234567`).
- Si el verificador da 10, AFIP cambia el prefijo a **23** y fija el verificador en 9 (varón) o 4 (mujer). Es la regla
  real, por eso existen CUIL que empiezan con 23.

```python
def is_valid_cuit(value: str | None) -> bool:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) != 11 or digits[:2] not in {"20", "23", "24", "27", "30", "33", "34"}:
        return False
    return cuit_check_digit(digits[:10]) == int(digits[10])
```

- Acepta el CUIT con o sin guiones: primero se queda sólo con los dígitos (`ch.isdigit()`). `(value or "")` evita un
  error si llega `None`.
- Valida tres cosas: 11 dígitos, un prefijo que existe (20/23/24/27 personas, 30/33/34 empresas) y el verificador.
- `digits[:2]` = los dos primeros; `digits[10]` = el undécimo (se cuenta desde 0).

#### 6.3 CUIT en Spark

```python
def _weighted_sum(s: Column, weights: tuple[int, ...], offset: int = 0) -> Column:
    return sum(
        (F.substring(s, offset + i + 1, 1).cast("int") * F.lit(w) for i, w in enumerate(weights)),
        F.lit(0),
    )
```

- La suma ponderada, pero como **expresión de Spark**: para cada posición, `F.substring(texto, posición, 1)` toma un
  dígito (en Spark las posiciones empiezan en **1**, por eso `+ 1`), `.cast("int")` lo convierte en número y se
  multiplica por el peso (`F.lit(w)` = un valor fijo como columna).
- `sum(..., F.lit(0))`: suma todas esas expresiones empezando desde una columna 0. El resultado **no es un número**: es
  una **fórmula** que Spark va a calcular para cada fila.
- El `_` al principio del nombre indica que es de uso **interno** del módulo.
- `offset`: desde qué posición empezar (el CBU lo necesita para su segundo bloque).

```python
def cuit_dv_col(first10: Column) -> Column:
    dv = F.lit(11) - _weighted_sum(first10, CUIT_WEIGHTS) % 11
    return F.when(dv == 11, F.lit(0)).otherwise(dv)
```

- El mismo cálculo que `cuit_check_digit`. `F.when(condición, valor).otherwise(otro)` es el `if/else` de las columnas.

```python
def cuit_col(dni: Column, female: Column) -> Column:
    """Versión Spark de `build_cuit` (dni entero, female booleano)."""
    dni8 = F.lpad(dni.cast("string"), 8, "0")
    body = F.concat(F.when(female, F.lit("27")).otherwise(F.lit("20")), dni8)
    dv = cuit_dv_col(body)
    return F.when(dv == 10, F.concat(F.lit("23"), dni8, F.when(female, F.lit("4")).otherwise(F.lit("9")))) \
        .otherwise(F.concat(body, dv.cast("string")))
```

- `F.lpad(texto, 8, "0")`: completa con ceros a la izquierda hasta 8 (lo mismo que `:08d`).
- `F.concat(...)`: pega textos. El resto es `build_cuit` traducido línea por línea. La `\` al final continúa la línea.

```python
def cuit_company_col(n8: Column) -> Column:
    """CUIT de una persona jurídica: 30 + 8 dígitos + DV (si da 10, AFIP usa 33 y DV 9)."""
    num8 = F.lpad(n8.cast("string"), 8, "0")
    body = F.concat(F.lit("30"), num8)
    dv = cuit_dv_col(body)
    return F.when(dv == 10, F.concat(F.lit("33"), num8, F.lit("9"))).otherwise(F.concat(body, dv.cast("string")))
```

- Para las **empresas** (los clientes PyME): prefijo 30, y 33 si el verificador da 10.

```python
def cuit_valid_col(value: Column) -> Column:
    """True si el CUIT (con o sin guiones) tiene 11 dígitos, prefijo válido y dígito verificador correcto."""
    d = F.regexp_replace(value, r"[^0-9]", "")
    prefix_ok = F.substring(d, 1, 2).isin("20", "23", "24", "27", "30", "33", "34")
    return F.coalesce(
        (F.length(d) == 11) & prefix_ok & (cuit_dv_col(d) == F.substring(d, 11, 1).cast("int")),
        F.lit(False),
    )
```

- `F.regexp_replace(value, r"[^0-9]", "")`: borra todo lo que **no** sea dígito (una **expresión regular**: `[^0-9]`
  = "cualquier carácter que no esté entre 0 y 9").
- `&` es el "y" entre condiciones de columnas (no se usa `and`).
- `F.coalesce(x, F.lit(False))`: si `x` da **nulo** (el CUIT venía vacío), devuelve `False`. En Spark, comparar un nulo
  da nulo, no falso: sin `coalesce`, un CUIT vacío no contaría como inválido.

#### 6.4 CBU en Python

```python
# ------------------------------------------------------------------------ CBU
def _cbu_dv(digits: str, weights: tuple[int, ...]) -> int:
    total = sum(int(d) * w for d, w in zip(digits, weights, strict=True))
    return (10 - total % 10) % 10


def build_cbu(branch: int, account_number: int, bank: str = BANK_CODE) -> str:
    """CBU de 22 dígitos: [banco 3 + sucursal 4 + DV] [cuenta 13 + DV]."""
    block1 = f"{bank}{branch:04d}"
    block2 = f"{account_number:013d}"
    return f"{block1}{_cbu_dv(block1, CBU_BLOCK1_WEIGHTS)}{block2}{_cbu_dv(block2, CBU_BLOCK2_WEIGHTS)}"


def is_valid_cbu(value: str | None) -> bool:
    v = value or ""
    if len(v) != 22 or not v.isdigit():
        return False
    return _cbu_dv(v[:7], CBU_BLOCK1_WEIGHTS) == int(v[7]) and _cbu_dv(v[8:21], CBU_BLOCK2_WEIGHTS) == int(v[21])
```

El CBU tiene **dos bloques**, cada uno con su verificador (módulo 10). Ejemplo: sucursal 12, cuenta 345678.

```
bloque 1:  998 0012           banco 998 + sucursal 0012
dígitos:   9  9  8  0  0  1  2
pesos:     7  1  3  9  7  1  3
producto: 63  9 24  0  0  1  6   → suma 103 → 103 % 10 = 3 → (10 − 3) % 10 = 7
bloque 2:  0000000345678      la cuenta con 13 dígitos → suma ponderada 133 → verificador 7

CBU = 9980012 7 0000000345678 7  =  9980012700000003456787
```

- `(10 - total % 10) % 10`: el último `% 10` hace que si da 10 quede 0.
- `is_valid_cbu`: exige 22 dígitos y que **los dos** verificadores coincidan. `v[:7]` = primeros 7, `v[7]` = el octavo,
  `v[8:21]` = del noveno al vigésimo primero.

#### 6.5 CBU en Spark

```python
def _cbu_dv_col(s: Column, weights: tuple[int, ...], offset: int) -> Column:
    return (F.lit(10) - _weighted_sum(s, weights, offset) % 10) % 10


def cbu_col(branch: Column, account_number: Column, bank: str = BANK_CODE) -> Column:
    block1 = F.concat(F.lit(bank), F.lpad(branch.cast("string"), 4, "0"))
    block2 = F.lpad(account_number.cast("string"), 13, "0")
    return F.concat(
        block1, _cbu_dv_col(block1, CBU_BLOCK1_WEIGHTS, 0).cast("string"),
        block2, _cbu_dv_col(block2, CBU_BLOCK2_WEIGHTS, 0).cast("string"),
    )


def cbu_valid_col(value: Column) -> Column:
    ok = (
        (F.length(value) == 22)
        & value.rlike(r"^[0-9]+$")
        & (_cbu_dv_col(value, CBU_BLOCK1_WEIGHTS, 0) == F.substring(value, 8, 1).cast("int"))
        & (_cbu_dv_col(value, CBU_BLOCK2_WEIGHTS, 8) == F.substring(value, 22, 1).cast("int"))
    )
    return F.coalesce(ok, F.lit(False))
```

- Lo mismo que en Python. En `cbu_valid_col`, el segundo bloque se lee desde la posición 9 (`offset=8`).
- `value.rlike(r"^[0-9]+$")`: el texto tiene **sólo** dígitos (`^` = principio, `+` = uno o más, `$` = final).

**Qué NO hace `ids.py`:** no lee ni escribe tablas, no sabe nada de clientes. Sólo recibe números o columnas y devuelve
identificadores o verdadero/falso. Quién lo usa: el generador (capítulo 05) para crear CUIT y CBU, y silver (capítulo
16) para marcar los inválidos.

### 7. `src/andes_bank/utils/spark.py`: Spark en el codespace

**Qué es:** la forma de conseguir una **sesión de Spark** (el objeto por el que se le habla a Spark: `spark.table(...)`,
`spark.sql(...)`). En Databricks ya existe una; en el codespace hay que crearla, y **configurarla para que se comporte
como Databricks** en lo que importa.

`code src/andes_bank/utils/spark.py`:

```python
"""Helpers para obtener/crear la SparkSession en Databricks y en local."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from andes_bank.config import is_databricks


def get_spark(app_name: str = "andes_bank") -> SparkSession:
    """En Databricks devuelve la sesión activa (serverless). En local crea una con Delta Lake OSS."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():  # pragma: no cover
        return SparkSession.builder.getOrCreate()
    return build_local_spark(app_name)
```

- `from andes_bank.config import is_databricks`: el primer import **entre módulos del proyecto**: `spark.py` usa algo de
  `config.py`.
- Si ya hay una sesión activa (en Databricks siempre la hay), la devuelve; si no, en Databricks pide una, y en el
  codespace crea una local.

```python
def build_local_spark(app_name: str = "andes_bank-local", warehouse_dir: str | None = None) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # en Databricks el formato por defecto es Delta; en Spark OSS es Parquet. Sin esto, saveAsTable crea tablas
        # Parquet en local y las opciones de Delta (replaceWhere) se ignoran EN SILENCIO (bitácora R09).
        .config("spark.sql.sources.default", "delta")
    )
```

- `from delta import ...` está **adentro** de la función: así el paquete se puede importar en Databricks aunque no
  exista el módulo `delta` de pip (allá Delta viene incluido).
- Cada `.config(...)` es una perilla:

| perilla | qué hace | por qué |
|---|---|---|
| `.master("local[*]")` | Spark en esta máquina, usando todos los núcleos | no hay un cluster en el codespace |
| `spark.sql.extensions` + `spark.sql.catalog...` | activan **Delta Lake** | sin esto no se pueden crear tablas Delta ni hacer `MERGE` |
| `shuffle.partitions = 8` | en cuántas partes reparte Spark los datos al agrupar | el default (200) es para clusters grandes; con datos chicos, 8 es mucho más rápido |
| `session.timeZone = UTC` | la hora de la sesión en UTC | igual que en los jobs de Databricks: las fechas y horas se calculan igual en los dos lados |
| `spark.ui.enabled = false` | apaga la página web de monitoreo de Spark | en tests no sirve y ocupa un puerto |
| `schema.autoMerge` | permite agregar columnas nuevas al escribir | igual que en Databricks |
| **`sources.default = delta`** | las tablas se crean en formato Delta | **un error real** (abajo) |

**El error que explica la última perilla (bitácora R09):** en el test de punta a punta faltaban 20 mil autorizaciones
de tarjeta. En Databricks, `saveAsTable` crea tablas **Delta**; en Spark del codespace, **Parquet**. Una opción de
escritura que sólo entiende Delta se ignoraba **sin avisar** y cada escritura pisaba la tabla entera. La lección: el
entorno de pruebas tiene que parecerse a producción en lo que importa.

```python
    if warehouse_dir:
        # metastore Derby persistente: las tablas sobreviven entre ejecuciones locales
        builder = (
            builder.config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={warehouse_dir}/metastore_db;create=true")
        )
```

- Si se pasa una carpeta, las tablas se guardan ahí con su catálogo (un *metastore* "Derby", una base de datos chiquita
  que Spark trae). Los tests lo usan con una carpeta temporal.

```python
    if os.environ.get("ANDES_DELTA_JARS_IN_CLASSPATH") == "1":
        # entornos sin acceso a Maven: los jars de Delta ya están en pyspark/jars
        return builder.getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()
```

- Normalmente `configure_spark_with_delta_pip` hace que Spark **baje** de internet (de *Maven*, el repositorio de
  bibliotecas de Java) los archivos de Delta la primera vez. En entornos sin acceso a Maven se copian a mano y se
  avisa con esa variable. En tu codespace no hace falta.
- `getOrCreate()`: crea la sesión (o devuelve la que ya hay).

**Qué NO hace:** no se usa en los notebooks de Databricks (ahí `spark` ya existe). Es para el codespace y los tests.

### 8. Los tests

#### 8.1 `tests/conftest.py`: lo que comparten los tests

`code tests/conftest.py`:

```python
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from andes_bank.utils.spark import build_local_spark

    wh = tmp_path_factory.mktemp("warehouse")
    session = build_local_spark("andes-bank-tests", warehouse_dir=str(wh))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

- `conftest.py` es un archivo especial que `pytest` lee **antes** de los tests: lo que se define acá lo pueden usar
  todos.
- `sys.path.insert(0, .../src)`: agrega `src/` a los lugares donde Python busca módulos. Así los tests encuentran
  `andes_bank` aunque no se haya instalado. `Path(__file__).resolve().parents[1]` = la carpeta de arriba de `tests/`,
  o sea, la raíz del repo.
- Un **fixture** es algo que un test **pide por nombre** en sus parámetros: un test que escribe `def test_x(spark):`
  recibe la sesión de Spark que arma esta función.
- `scope="session"`: se crea **una sola vez** para todos los tests (arrancar Spark tarda 10-20 segundos).
- `tmp_path_factory.mktemp(...)`: una carpeta temporal nueva (la da pytest).
- `setLogLevel("ERROR")`: Spark escribe muchísimos mensajes; sólo los errores.
- `yield session` entrega la sesión a los tests; cuando terminan **todos**, sigue la línea siguiente: `session.stop()`
  la apaga.

> Este archivo crece en capítulos siguientes: en el 05 se agregan los fixtures `cfg` (una configuración aislada por
> test) y `truth` (un banco de 1.000 clientes generado una vez para todos los tests).

#### 8.2 `tests/test_config.py`

`code tests/test_config.py`:

```python
from andes_bank.config import ProjectConfig


def test_nombres_y_rutas():
    c = ProjectConfig(storage_account="andesbankdev")
    assert c.table("bronze", "core_transactions") == "andes_bank.bronze.core_transactions"
    assert c.landing_path("core", "transactions") == "/Volumes/andes_bank/landing/raw/core/transactions"
    assert c.blob_url("core") == "https://andesbankdev.blob.core.windows.net/core"


def test_escala():
    assert ProjectConfig(scale_factor=0.1).n_customers == 50_000
    assert ProjectConfig(scale_factor=1).n_customers == 500_000
    assert ProjectConfig(scale_factor=0.0001).n_merchants == 200
```

- `test_nombres_y_rutas`: crea una configuración y verifica que los nombres y las rutas sean exactamente los que espera
  Databricks.
- `test_escala`: verifica la escala y el **mínimo de 200 comercios**.
- **Qué NO prueba:** que la tabla o la carpeta existan (no existen todavía). Prueba que el **nombre** es el correcto.

> En la referencia, `test_config.py` tiene dos tests más que llegan después: uno de `setup.py` (capítulo 05) y uno de
> `ayer` en hora argentina (capítulo 26).

#### 8.3 `tests/test_ids.py`

`code tests/test_ids.py`:

```python
import pytest

from andes_bank.utils.ids import (
    build_cbu,
    build_cuit,
    cbu_col,
    cbu_valid_col,
    cuit_col,
    cuit_valid_col,
    is_valid_cbu,
    is_valid_cuit,
)


def test_cuit_conocido():
    # ejemplo de manual: 20-12345678-6
    assert build_cuit(12345678, female=False) == "20123456786"
    assert is_valid_cuit("20-12345678-6")
    assert not is_valid_cuit("20-12345678-5")
    assert not is_valid_cuit("2012345678")
```

- El ejemplo calculado a mano en el paso 6.2. Prueba lo que **tiene** que pasar (el bueno es válido) y lo que **no**
  (verificador cambiado, un dígito de menos).

```python
@pytest.mark.parametrize("dni", [1, 9_999_999, 23_456_789, 40_111_222, 45_999_999])
def test_cuit_generado_siempre_valido(dni):
    assert is_valid_cuit(build_cuit(dni, female=True))
    assert is_valid_cuit(build_cuit(dni, female=False))
```

- `@pytest.mark.parametrize`: corre el mismo test **una vez por cada valor** de la lista (5 tests). Incluye casos borde:
  DNI de un dígito, de 7 y de 8. Si alguno cae en el caso "verificador 10", prueba también la regla del 23.

```python
def test_cbu():
    cbu = build_cbu(12, 345678)
    assert len(cbu) == 22 and is_valid_cbu(cbu)
    assert not is_valid_cbu(cbu[:-1] + str((int(cbu[-1]) + 1) % 10))
    assert not is_valid_cbu(cbu[:21])
```

- Un CBU generado tiene 22 dígitos y es válido. Si se cambia el último dígito (`cbu[:-1]` = todo menos el último), deja
  de serlo. Uno de 21 dígitos (el error DAT-07 del core), tampoco.

```python
@pytest.mark.spark
def test_columnas_spark_igual_que_python(spark):
    from pyspark.sql import functions as F

    rows = [(d, f, b, a) for d, f, b, a in [(12345678, False, 12, 345678), (30111222, True, 7, 1), (5, True, 9999, 99)]]
    df = spark.createDataFrame(rows, "dni long, female boolean, branch int, acct long")
    out = df.select(
        cuit_col(F.col("dni"), F.col("female")).alias("cuit"),
        cbu_col(F.col("branch"), F.col("acct")).alias("cbu"),
        "dni", "female", "branch", "acct",
    ).withColumn("cuit_ok", cuit_valid_col(F.col("cuit"))).withColumn("cbu_ok", cbu_valid_col(F.col("cbu"))).collect()
    for r in out:
        assert r.cuit == build_cuit(r.dni, r.female)
        assert r.cbu == build_cbu(r.branch, r.acct)
        assert r.cuit_ok and r.cbu_ok
```

- `@pytest.mark.spark`: este test usa Spark (es lento): se marca.
- Pide el fixture `spark` (por el nombre del parámetro).
- `spark.createDataFrame(filas, esquema)`: arma una tabla chica en memoria con tres casos. El esquema es texto:
  columna y tipo.
- `df.select(...)` calcula CUIT y CBU **con las versiones de Spark**; `.withColumn` agrega las validaciones;
  `.collect()` trae las filas a Python.
- Para cada fila: la versión de Spark tiene que dar **exactamente** lo mismo que la de Python, y validar. **Este test
  es el que justifica tener el algoritmo dos veces:** garantiza que son el mismo.

### 9. Instalar y probar

Como ahora existe `pyproject.toml`, instalá el paquete (o reconstruí el codespace: el `post_create.sh` lo hace solo):

```bash
pip install -e ".[dev,azure,genie]"
```

La primera vez tarda un par de minutos (Spark pesa unos 300 MB).

```bash
ruff check src tests
pytest -m "not spark"
pytest -m spark
```

- `ruff check`: tiene que decir `All checks passed!`. Si marca algo, el mensaje dice archivo, línea y qué regla.
- `pytest -m "not spark"`: los tests rápidos. Tienen que ser **9 puntos** (`.........`): 2 de config + 1 del CUIT
  conocido + 5 del parametrizado (uno por DNI) + 1 de CBU.
- `pytest -m spark`: el test de Spark. La primera vez Spark baja los archivos de Delta (1-2 minutos); después, unos 20
  segundos.

### 10. Guardar y comparar con la referencia

```bash
git add .gitignore pyproject.toml src tests
git commit -m "Etapa 02: paquete, configuración, CUIT/CBU y Spark local"
git push
```

**Comparar con la referencia.** Para que tu codespace pueda leer el repo privado `andes-bank-referencia`, agregá a tu
`.devcontainer/devcontainer.json`, dentro de `"customizations"`, este bloque (con **tu** usuario de GitHub) y reconstruí
el codespace (al abrirlo, GitHub te pide autorizar el acceso):

```jsonc
    "codespaces": {
      "repositories": {
        "<tu-usuario>/andes-bank-referencia": { "permissions": { "contents": "read" } }
      }
    },
```

Después, una sola vez:

```bash
git remote add referencia https://github.com/<tu-usuario>/andes-bank-referencia.git
git fetch referencia --tags
```

- Un **remote** es otro repositorio con el que tu repo puede intercambiar commits. `origin` es el tuyo; `referencia`,
  el terminado.

Y para comparar:

```bash
git diff etapa-02 -- src/andes_bank/config.py src/andes_bank/utils tests/test_ids.py
```

- Muestra las diferencias entre **tu** archivo y el del tag `etapa-02` de la referencia: líneas con `-` (están en la
  referencia y no en el tuyo) y con `+` (al revés). Sin salida = iguales. Si los tags no existen todavía en la
  referencia, compará contra la rama: `git diff referencia/main -- <archivo>`.

## 🤔 Por qué así

- **¿Por qué una sola configuración y no constantes sueltas?** Porque el mismo código corre en el codespace, en
  Databricks y en un catálogo de prueba. Cambiar de lugar es cambiar **un** objeto.
- **¿Por qué `frozen=True`?** Una configuración que se puede modificar a mitad de un proceso es una fuente de errores
  muy difíciles de encontrar. Si necesitás otra, se crea otra (`dataclasses.replace(cfg, end_date=...)`).
- **¿Por qué el algoritmo real de CUIT y CBU, si los datos son inventados?** Porque el proyecto tiene que **detectar**
  los mal cargados (DAT-07). Con números al azar no habría nada que validar.
- **¿Por qué tests desde el primer archivo?** Porque cada capítulo que sigue construye encima. Un error en `ids.py`
  aparecería tres capítulos después como "un cliente con CUIT inválido", lejos de su causa.

## 🧪 Probalo

| comando | qué tenés que ver |
|---|---|
| `python -c "import andes_bank; print(andes_bank.__version__)"` | `0.1.0` |
| `python -c "from andes_bank.config import ProjectConfig as C; print(C().table('gold','dim_cliente'))"` | `andes_bank.gold.dim_cliente` |
| `python -c "from andes_bank.utils.ids import build_cbu; print(build_cbu(12, 345678))"` | `9980012700000003456787` |
| `ruff check src tests` | `All checks passed!` |
| `pytest -m "not spark"` y `pytest -m spark` | todo en verde |

## 🔥 Si falla

| síntoma | por qué | qué hacer |
|---|---|---|
| `ModuleNotFoundError: No module named 'andes_bank'` | el paquete no está instalado o falta `src/andes_bank/__init__.py` | `pip install -e ".[dev]"` y revisá el archivo |
| `ModuleNotFoundError: No module named 'pyspark'` | no se instaló el grupo `dev` | `pip install -e ".[dev]"` |
| `JAVA_GATEWAY_EXITED` o `Java not found` | falta Java | el codespace no se creó con tu receta: *Rebuild Container* |
| `PYTHON_VERSION_MISMATCH` | Spark encontró dos Pythons distintos | `export PYSPARK_PYTHON=$(which python)` y volvé a correr |
| el test de Spark tarda mucho la primera vez | está bajando Delta de internet | esperá; la segunda vez es rápido |
| un test falla con `assert '2012345678X' == '20123456786'` | un error en tu `ids.py` | el mensaje muestra lo esperado y lo obtenido: compará con el paso 6 |

## ✅ Punto de control

- `pytest` en verde (rápidos y de Spark), `ruff` sin errores.
- `git diff etapa-02 -- src/andes_bank/utils tests/test_ids.py` sin diferencias. En `config.py` y `conftest.py` puede
  haber diferencias **sólo** en lo que el curso marcó como "llega después".

## 🏋️ Ejercicio

1. Sin mirar: escribí en un papel cómo se calcula el verificador del CUIT `27-30111222-?` y verificalo con
   `python -c "from andes_bank.utils.ids import build_cuit; print(build_cuit(30111222, True))"` (tiene que dar
   `27301112225`).
2. Agregá a `test_ids.py` un test que verifique que `cuit_valid_col` devuelve `False` (y no nulo) para un CUIT vacío
   (`None`). Pista: `spark.createDataFrame([(None,)], "cuit string")`.
