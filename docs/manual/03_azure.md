# 03 · Azure: las fuentes "reales"

> Carpetas `azure/card_api/` (una Azure Function en Python) y `scripts/azure/` (la infraestructura con `az`). Es el
> "mundo exterior" del banco: el storage donde los sistemas dejan archivos y la API HTTP del procesador de tarjetas,
> del CRM y del espejo del BCRA, **con fallas inyectadas a propósito**.

![La API y su caos](../diagramas/07_api_caos_cliente.svg)

## 1. Por qué Azure y no todo dentro de Databricks

- **Para que la ingesta sea de verdad.** Leer una tabla que ya está en el catálogo no enseña nada. Con Azure hay
  red, autenticación, credenciales con permisos mínimos, latencia, paginación y fallas.
- **Patrón *pull*.** Databricks Free Edition verificado tiene acceso saliente a internet ([ADR 0001](../adr/0001_free_edition_acceso_saliente.md)):
  el lakehouse **va a buscar** los datos, como haría con un proveedor real.
- **Costo casi cero.** Storage cobra por GB y operación; Flex Consumption cobra por ejecución, con 250 mil
  ejecuciones y 100 mil GB-s gratis por mes. El mes del MVP costó **USD 0,02**.

## 2. Los recursos

| recurso | nombre | para qué |
|---|---|---|
| Resource group | `rg-andes-bank` | todo junto: se borra de un saque |
| Storage de datos | `andesbank<sufijo>` | contenedores `core`, `cards-legacy`, `api-data`, `fx-mirror`, `crm-data` |
| Storage de la Function | `andesfn<sufijo>` | el código desplegado y el estado interno de la Function |
| Function App | `andes-card-api-<sufijo>` | Flex Consumption, Python 3.12, 512 MB, máximo 40 instancias |

El **sufijo** son los primeros 6 caracteres del sha256 del id de suscripción (`env.sh`): los nombres de storage y de
Function App son únicos **en el mundo**, así que cada persona que reconstruye el proyecto obtiene nombres propios y
siempre los mismos.

## 3. `scripts/azure/`

| script | qué hace | detalles que importan |
|---|---|---|
| `env.sh` | variables comunes (`RG`, `LOCATION=eastus2`, `STORAGE`, `FUNC_APP`, `CONTAINERS`, `TAGS`) y `az_login()` | en la nube entra con el service principal (`AZURE_CLIENT_ID/SECRET/TENANT_ID`); en tu máquina usa tu `az login`. Exige `AZURE_SUBSCRIPTION_ID` |
| `01_infra.sh` | crea resource group, las dos cuentas de storage (LRS, TLS 1.2, **sin acceso público**), los contenedores y la Function App; configura `DATA_STORAGE_CONNECTION` y el perfil de caos | **idempotente**: pregunta si existe antes de crear. Los contenedores se crean con `az storage container-rm create` (plano de administración), no con `container create` (plano de datos): la red del entorno de desarrollo bloqueaba `*.blob.core.windows.net` (bitácora R04) |
| `02_secrets.sh` | genera dos **SAS de cuenta**: escritura (`rwlac`, para los "sistemas origen") y **lectura** (`rl`, para el lakehouse), 365 días, sólo HTTPS. Genera `client_secret` y clave de firma con `openssl rand`. Guarda lo de la API en la Function App y todo lo del lakehouse en el **secret scope `andes_bank`** de Databricks | correrlo de nuevo **rota** las credenciales. Nunca imprime los valores: al final lista sólo los **nombres** de los secretos |
| `03_deploy_api.sh` | zipea `azure/card_api/` y lo despliega con `config-zip --build-remote` (Azure instala `requirements.txt` para Linux) | prueba `/api/health` hasta 6 veces con 10 s de espera |
| `04_costo.sh` | consulta Cost Management (costo real del mes, agrupado por servicio) | se consulta a nivel **resource group** porque el service principal no tiene permisos de facturación sobre toda la suscripción |
| `99_teardown.sh` | borra el resource group entero | pide escribir el nombre del RG para confirmar (salvo `--yes`) |

Secretos en el scope `andes_bank`: `storage_account`, `blob_sas_write`, `blob_sas_read`, `card_api_base_url`,
`card_api_client_id`, `card_api_client_secret`.

## 4. `azure/card_api/`: la Function

### Diseño: la lógica no depende de Azure

`function_app.py` es un **adaptador** de 50 líneas: recibe el `HttpRequest` de Azure, lo traduce a un `Request`
propio y llama a `CardApi.handle()`, que devuelve `(status, headers, body)`. Toda la lógica (y el caos) está en
`api.py`, `chaos.py`, `tokens.py` y `store.py`, **sin una sola importación de Azure Functions**. Por eso los tests
(`test_card_api.py`, `test_ingestion.py`, `test_e2e.py`) usan la API real en el mismo proceso, con stores en memoria
(`MemoryStore`) y una sesión HTTP falsa (`tests/fake_http.py`): sin nube y sin red.

| archivo | qué tiene |
|---|---|
| `function_app.py` | un solo HTTP trigger con ruta comodín `{*path}` (GET y POST), autenticación anónima a nivel Function (la API hace su propio OAuth). Arma `CardApi` con los stores de Blob, la config y el caos desde las *app settings* |
| `api.py` | los endpoints, la paginación y la aplicación del caos |
| `chaos.py` | los perfiles de caos y cómo se decide cada falla |
| `tokens.py` | OAuth client credentials mínimo con tokens firmados por HMAC |
| `store.py` | de dónde lee los datos: Blob (producción) o memoria (tests) |
| `host.json` | prefijo de ruta `api`, logs en Warning salvo la Function |
| `requirements.txt` | `azure-functions`, `azure-storage-blob` (versiones fijas) |
| `.funcignore` | no sube cachés, tests ni `local.settings.json` |

### Endpoints (`api.py`)

| endpoint | qué devuelve |
|---|---|
| `POST /oauth/token` | `client_credentials` → token Bearer de **15 minutos** (`invalid_client` si las credenciales están mal) |
| `GET /v2/authorizations?processing_date=YYYY-MM-DD[&cursor=…]` | página de 1.000 autorizaciones, `next_cursor`, `has_more`, `total_estimate` |
| `GET /v2/settlements?settlement_date=…` | lo mismo para liquidaciones |
| `GET /v2/<recurso>/daily-totals?…` | cantidad de registros, monto total y páginas del día (para **conciliar**) |
| `GET /v1/authorizations?processing_date=…&offset=&limit=` | la versión vieja: offset/limit, montos en centavos, epoch ms |
| `GET /fx/estadisticascambiarias/v1.0/Cotizaciones/<MONEDA>?fechadesde=&fechahasta=&offset=&limit=` | **espejo del BCRA** con la misma forma que la API real |
| `GET /crm/v1/<Objeto>?modified_since=<ISO>[&cursor=…]` | CRM: registros modificados desde una fecha, **borrados incluidos**, de a 500 |
| `GET /health` | estado y perfil de caos activo |

Detalles de implementación:

- **Cursor opaco:** el cursor es un JSON (`{"d": día, "p": página}`) en base64 URL-safe. El cliente no tiene que saber
  qué hay adentro; un cursor inválido da 400.
- **Antes del lanzamiento:** pedir un día anterior a 2025-04-01 a la v2 da **422** con un mensaje que manda a usar la
  integración v1.
- **v1 con offset inestable (API-07):** entre pedido y pedido "entran filas" y el offset se corre ±1 a 3 filas: se
  pierden o duplican filas en el borde de las páginas. Es la razón por la que el lakehouse no usa la v1 por API (la
  toma de los exportes diarios).
- **CRM:** filtra `SystemModstamp >= modified_since`, ordena por `(SystemModstamp, Id)` y guarda el filtro **dentro del
  cursor**, así las páginas siguientes no lo repiten.
- **Espejo BCRA con suciedad (`_dirty_fx`):** en sep-2024 el valor viene como texto con coma (`"960,5000"`) y en
  feb-2025 el código de moneda es `U$S` en vez de `USD`. Devuelve de la fecha más nueva a la más vieja, como el BCRA.
- `_to_v1`: convierte un registro canónico al formato v1.

### El caos (`chaos.py`)

Dos tipos de falla, y la diferencia es la lección más importante de la ingesta:

| tipo | fallas | cómo se decide | cómo se supera |
|---|---|---|---|
| **Transitorias** | 429 con `Retry-After` (API-01), 500/502/503 (API-02), respuesta lenta de 35 s (API-03), token vencido antes de tiempo (API-04), JSON cortado (API-10), 200 con `{"status": "error"}` (API-12) | `roll(rate)`: azar puro en cada pedido | un reintento bien hecho |
| **Estructurales** | página repetida (API-05), página salteada (API-06) | `structural(rate, recurso, día, página)`: un hash de la semilla, la **ventana de 10 minutos** y la página. Pedir lo mismo enseguida da **lo mismo** | conciliar contra los totales y **volver a pedir el día un rato después** |

Perfiles: `off` (sin fallas), `normal` (el del MVP: 3 % de 429, 2 % de 5xx, 1 % de token vencido, 1 % de página
repetida, 0,5 % de salteada, 30 % de offset inestable en v1) y `dificil` (más de todo, más JSON cortado y 200 con
error). Se elige con la *app setting* `CHAOS_PROFILE`.

`_guarded()` aplica, en orden: autenticación (y token vencido al azar), rate limit, error de servidor, lentitud,
y después de una respuesta 200 paginable, error-en-200 o cuerpo truncado.

### Los tokens (`tokens.py`)

Un token es `base64(payload).firma`, con `payload = {"cid": client_id, "exp": vencimiento}` y la firma
HMAC-SHA256 con `TOKEN_SIGNING_KEY`. La Function no necesita base de datos: verifica la firma (`hmac.compare_digest`,
resistente a ataques de tiempo) y el vencimiento. Motivos de rechazo: `malformed`, `bad_signature`, `token_expired`.

### Los stores (`store.py`)

La API **no tiene base de datos**: cada pedido lee **un blob** (`api-data/<recurso>/yyyy/mm/dd/page-00001.json` o el
`_manifest.json`, que se cachea con `lru_cache`). Por eso sirve millones de filas por centavos.

| clase | lee de |
|---|---|
| `BlobStore` / `MemoryStore` | páginas y manifiestos de tarjetas |
| `BlobFxStore` / `MemoryFxStore` | `fx-mirror/cotizaciones/<MONEDA>.json` |
| `BlobCrmStore` / `MemoryCrmStore` | `crm-data/<Objeto>.json` |

### Configuración (app settings, nunca en el código)

`DATA_STORAGE_CONNECTION`, `API_CLIENT_ID`, `API_CLIENT_SECRET`, `TOKEN_SIGNING_KEY`, `CHAOS_PROFILE`, `CHAOS_SEED`.

## 5. Cómo se probó en la nube

En la primera corrida real contra Azure, el quality gate frenó: a 2 días les faltaban **exactamente 1.000** registros.
Era la página salteada estructural: el cliente reintentaba 4 veces enseguida y la API, coherente con su diseño, le
salteaba la misma página. Los tests no lo habían visto porque el caos dependía del reloj real (bitácora R14, ver
[04 · Ingesta](04_ingesta.md)). Ese bug es el ejemplo de por qué el caos estructural existe.
