# 11 · Etapa 08 · El CRM comercial: generarlo y publicarlo en la API

## 🎯 Objetivo

Tener el CRM de Andes Bank funcionando como un **sistema origen más**: unas 515 cuentas, unos 600 contactos,
250 leads, 350 oportunidades y 200 casos, sucios a propósito. El CRM se sirve por **la misma API de Azure** que
tarjetas (`/crm/v1/<Objeto>`), con el mismo token y el mismo caos. Publicarlo dos veces tiene que dar exactamente lo
mismo.

Va después del generador (necesita los clientes de `sim.customers`) y antes de la ingesta: el capítulo 15 lo lee
desde Databricks.

**Archivos:**

- `src/andes_bank/generator/crm.py`
- `src/andes_bank/generator/upload.py` (`crm_uploads`, `upload_crm`)
- `azure/card_api/api.py` (`_crm`) y `azure/card_api/store.py` (`BlobCrmStore`)
- `notebooks/05_crm_publish.py`
- `tests/test_crm.py`

## 📚 Conceptos

- **Un CRM con el modelo de Salesforce:** el CRM no es un SaaS real, pero usa el modelo de datos más común del
  mercado, el de Salesforce. Si algún día lo cambiás por uno real, la ingesta y silver no se enteran. Los objetos son:
  - Account: una cuenta, que puede ser una empresa o una persona.
  - Contact.
  - Lead: un prospecto todavía no calificado.
  - Opportunity: una venta posible, con etapa y monto.
  - Case: un reclamo o una consulta.
- **Campos custom (`__c`):** los que agrega la empresa. Acá son:

  | campo | para qué |
  |---|---|
  | `Core_Customer_Id__c` | el número de cliente del core |
  | `CUIT__c` | el CUIT de la cuenta |
  | `Segmento__c` | el segmento que cargó el ejecutivo |
  | `Producto__c` | el producto de la oportunidad |
  | `Andes_Key__c` | la clave de carga |

- **Lo que pone el sistema, no el usuario:** `Id`, `CreatedDate`, `SystemModstamp` (la última modificación),
  `IsDeleted`, y los campos calculados (`IsClosed`, `IsWon`, `CaseNumber`, `ClosedDate`). Los arma `crm_system` con
  la semilla.
- **Incremental por `SystemModstamp`:** la API devuelve lo modificado desde una fecha (`modified_since`). Trae
  **también los borrados** (`IsDeleted: true`): si no, un registro borrado en el CRM no desaparecería nunca del
  lakehouse.

## 🧱 Paso a paso

### 1. El CRM en el generador (`generator/crm.py`)

Se genera en **Python puro**, no en Spark: son 1.900 registros, y en Python la lógica de suciedad se lee mejor.

```python
def crm_sample(spark, cfg):
    df = (spark.table(cfg.table("sim", "customers"))
          .where("segment IN ('PYME', 'PREMIUM') AND NOT is_legacy_duplicate AND closed_date IS NULL")
          .withColumn("_o", F.xxhash64(F.lit(cfg.seed), F.col("customer_key")))
          .orderBy("_o").limit(N_ACCOUNTS_FROM_CORE).select(*cols))
```

**Por qué `xxhash64` y no `sample()`:** `sample()` depende de cómo se reparten los datos entre las particiones. El
hash de la semilla y la clave da **siempre** los mismos 460 clientes.

La suciedad del ID del core (DAT-09):

```python
def _dirty_core_id(customer_id, customer_key, r):
    x = r.random()
    if x < 0.12: return None, "VACIO"
    if x < 0.18: return f"MZA{customer_key:06d}", "BANCO_VIEJO"   # numeración anterior a la migración
    if x < 0.22: ...  # dos dígitos cambiados de lugar
    return customer_id, "OK"
```

Cada registro guarda también su **verdad** (`_true_customer_id`, `_dirt`) en `sim.crm_accounts`. Al publicar se
descartan las columnas que empiezan con `_`, y el test las usa para comprobar que el match es correcto.

La demás suciedad:

- **DAT-16:** 15 cuentas duplicadas.
- **DAT-17:** CUIT con y sin guiones.
- **DAT-11:** teléfonos y mails en la descripción de los casos.
- **DAT-18:** leads que ya son clientes.

### 2. Lo que tiene adentro el CRM (`crm_system`)

```python
def crm_system(records, seed, end):
    r = random.Random(seed * 11 + 5)
    ...
    row = {"Id": f"{ID_PREFIX[obj]}{n:015d}", "IsDeleted": False, "CreatedDate": _crm_ts(t0), **rec}
    if parent:
        row["AccountId"] = id_by_key[parent]
```

- Los hijos apuntan a su cuenta por la clave de carga (`Account.Andes_Key__c`). `crm_system` la traduce al
  `AccountId`, así que **el orden importa**: primero las cuentas, después el resto.
- Un hijo nace **después** que su cuenta. Una oportunidad nace entre 10 y 120 días antes de su `CloseDate`. Un caso
  cerrado tiene `ClosedDate` posterior al alta.
- Los `Id` salen del número de la clave (`ACC-00012` → `001000000000000012`) y las fechas, de la semilla.

### 3. El endpoint (`azure/card_api/api.py`)

```text
GET /crm/v1/Lead?modified_since=2026-10-01T00:00:00          → {"data": [...], "total": 87, "has_more": false, ...}
GET /crm/v1/Lead?cursor=eyJvIjoiTGVhZCIsInMiOiIiLCJwIjoyfQ   → la página 2 (el cursor guarda el filtro)
```

- Ordena por `(SystemModstamp, Id)` y pagina de a 500.
- Pasa por `_guarded`, igual que tarjetas: sin token devuelve 401, y le tocan los 429, los 5xx, el JSON cortado y el
  "200 con error".
- Lee `crm-data/<Objeto>.json` de Blob con un caché de 10 minutos, como el espejo FX: el CRM es chico y cada pedido
  no necesita ir a Blob.

### 4. El notebook (`05_crm_publish.py`)

Es una tarea más del job `andes-bank-fuentes` (`crm_publish`, después de `generate_truth`):

```python
print(generate_crm(spark, cfg))
print(upload_crm(cfg, dbutils.secrets.get("andes_bank", "blob_sas_write"), crm_state(spark, cfg)))
```

`upload_crm` crea el contenedor `crm-data` si falta (la SAS de escritura puede crear contenedores) y **siempre
pisa**. Acá no hace falta el `skip_existing` de R07: lo que importa adentro es el `SystemModstamp` de cada registro,
no el etag del archivo.

Después de agregar el endpoint, volvé a desplegar la API:

```bash
bash scripts/azure/03_deploy_api.sh
```

## 🤔 Por qué así

- **¿Por qué no un Salesforce real?** Lo intentamos (bitácora R15 a R17). La org gratuita pide un token de
  seguridad atado a la contraseña, y el token no se puede evitar con rangos de IP desde una nube con IPs que cambian.
  Además, HubSpot pide un sitio web para registrarse. Un CRM simulado cuesta lo mismo que la API de tarjetas
  (centavos) y no depende de nadie.
- **¿Por qué en la misma Function?** Porque ya tiene OAuth, caos y despliegue. Una segunda Function App duplicaría
  todo eso sin enseñar nada nuevo.
- **¿Por qué `Andes_Key__c` si ya está `Core_Customer_Id__c`?** Porque `Core_Customer_Id__c` está **sucio a
  propósito**: vacío, viejo o mal tipeado. Una clave de carga tiene que ser perfecta; un dato de negocio, no.

## 🧪 Probalo

En local, sin Azure:

```bash
pytest -q tests/test_crm.py                 # estado, endpoint y cliente con caos "dificil"
pytest -q tests/test_e2e.py -k crm          # de punta a punta, con un borrado incremental
```

En Databricks:

```bash
databricks bundle deploy -t dev
databricks jobs run-now --json '{"job_id": <id de andes-bank-fuentes>, "only": ["crm_publish"]}'
```

Y contra la API desplegada (con un token de `/oauth/token`):

```bash
curl -s -H "Authorization: Bearer $TOKEN" "https://<function-app>.azurewebsites.net/api/crm/v1/Account" | head -c 300
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `404 {"error": "not_found", "message": "No existe GET /crm/v1/Account"}` | la Function desplegada es la vieja, sin el endpoint | `bash scripts/azure/03_deploy_api.sh` |
| `200` con `"total": 0` | todavía no se publicó el CRM, o la Function cacheó "sin datos" | corré `crm_publish` y esperá 10 minutos (el caché) o reiniciá la Function |
| `AuthorizationPermissionMismatch` al crear `crm-data` | la SAS de escritura no tiene permiso de crear contenedores | creá el contenedor con `bash scripts/azure/01_infra.sh` (ya lo incluye) |
| `RuntimeError: CRM Lead: llegaron N de M registros` | el recorrido quedó corto tres veces seguidas | mirá `ops.api_calls`: si son 5xx sostenidos, la API está caída; reintentá más tarde |

## ✅ Punto de control

`git diff etapa-08 -- src/andes_bank/generator/crm.py azure/card_api/` no muestra diferencias de lógica.

## 🏋️ Ejercicio

Agregá a `crm_records` un objeto `Task` (llamadas del ejecutivo) con 300 registros relacionados con las cuentas, y su
campo `Andes_Key__c`. No te olvides de `OBJECTS` e `ID_PREFIX` en `crm.py` ni de `CRM_OBJECTS` en la API.
