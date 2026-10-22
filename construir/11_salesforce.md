# 11 · Etapa 08 · Salesforce: la org, campos custom por API y carga con Bulk 2.0

## 🎯 Objetivo

Tener una org de Salesforce **real** con el CRM de Andes Bank: unas 515 cuentas, unos 600 contactos, 250 leads,
350 oportunidades y 200 casos, sucios a propósito. Tiene que poder **volver a cargarse sin duplicar** y prepararse
**sola**, sin clics en Setup.

Va después del generador (necesita los clientes de `sim.customers`) y antes de la ingesta: el capítulo 15 la lee
desde Databricks.

**Archivos:**

- `src/andes_bank/generator/crm.py`
- `src/andes_bank/salesforce/org.py` y `salesforce/seed.py`
- `notebooks/05_salesforce_seed.py`
- `scripts/salesforce/01_secrets.sh` y `02_check_login.py`
- `tests/fake_salesforce.py`

## 📚 Conceptos

- **Org:** tu instancia de Salesforce. Una **Developer Edition** es gratis y no vence. Una **prueba** dura 30 días.
  Las dos tienen unos **5 MB de datos**, y cada registro ocupa unos 2 KB. Por eso el CRM **no escala** con el
  `scale_factor`: unos 1.900 registros entran con margen.
- **Objetos estándar:** Account (cuenta: una empresa o una persona), Contact, Lead (prospecto todavía no
  calificado), Opportunity (una venta posible, con etapa y monto) y Case (reclamo o consulta).
- **Campos custom (`__c`):** los que agrega la empresa. Acá son:

  | campo | para qué |
  |---|---|
  | `Core_Customer_Id__c` | el número de cliente del core |
  | `CUIT__c` | el CUIT de la cuenta |
  | `Segmento__c` | el segmento que cargó el ejecutivo |
  | `Producto__c` | el producto de la oportunidad |
  | `Andes_Key__c` | la clave de carga |

- **External ID + upsert:** un campo marcado como *External ID* y *Unique* permite hacer **upsert**: "si existe un
  registro con esta clave, actualizalo; si no, crealo". Re-cargar no duplica. Además, un hijo se puede relacionar con
  su padre **por la clave externa** (`Account.Andes_Key__c`), sin conocer el Id que Salesforce le asignó.
- **FLS (*field-level security*):** un campo creado por API **no lo ve nadie**, ni siquiera el administrador, hasta
  que un perfil o un *permission set* le da permiso de lectura y edición.
- **Tooling API:** la API de "metadatos como datos". Con un POST a `sobjects/CustomField` se crea un campo.
- **Bulk API 2.0:** la carga masiva. Mandás un CSV, Salesforce lo procesa en segundo plano y te devuelve cuántos
  registros entraron y cuáles fallaron.
- **Token de seguridad:** para entrar por API con usuario y contraseña desde una IP desconocida (Databricks, tu
  máquina), Salesforce pide la contraseña **seguida** del token. `simple_salesforce` lo arma solo.

## 🧱 Paso a paso

### 1. Crear la org y sacar las credenciales

1. Entrá a <https://developer.salesforce.com/signup> y creá la Developer Edition. Si no está disponible, usá la
   prueba de 30 días.
2. **`SF_USERNAME`:** el usuario del mail de bienvenida. Tiene forma de email, pero no tiene por qué ser tu mail.
3. **`SF_PASSWORD`:** la contraseña que elegiste al activar la cuenta.
4. **`SF_SECURITY_TOKEN`:** Avatar → **Configuración** → **Mi información personal → Restablecer mi token de
   seguridad**. Llega por mail, y cambia cada vez que cambiás la contraseña.
5. **`SF_LOGIN_URL`:** `https://login.salesforce.com`. Si tu org exige su dominio propio, usá la URL de
   *Setup → Mi dominio*.

Exportalas en tu terminal. **Nunca** las pegues en un chat ni en el repo:

```bash
export SF_USERNAME=... SF_PASSWORD=... SF_SECURITY_TOKEN=... SF_LOGIN_URL=https://login.salesforce.com
pip install simple-salesforce
python scripts/salesforce/02_check_login.py
# ✔ conectado a '...' (Developer Edition, sandbox=False)
#   almacenamiento de datos: 5 de 5 MB libres
bash scripts/salesforce/01_secrets.sh      # las copia al secret scope andes_bank
```

### 2. El CRM en el generador (`generator/crm.py`)

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

Cada registro guarda también su **verdad** (`_true_customer_id`, `_dirt`) en `sim.crm_accounts`. La carga a
Salesforce las saca (`seed.sf_rows` descarta las columnas que empiezan con `_`) y el test las usa para comprobar que
el match es correcto.

La demás suciedad:

- **DAT-16:** 15 cuentas duplicadas.
- **DAT-17:** CUIT con y sin guiones.
- **DAT-11:** teléfonos y mails en la descripción de los casos.
- **DAT-18:** leads que ya son clientes.

### 3. Preparar la org (`salesforce/org.py`)

```python
CUSTOM_FIELDS = {"Account": {"Andes_Key__c": EXTERNAL_KEY, "Core_Customer_Id__c": ..., ...}, ...}

def ensure_fields(sf):
    for obj, fields in CUSTOM_FIELDS.items():
        existing = {f["name"] for f in getattr(sf, obj).describe()["fields"]}
        for name, meta in fields.items():
            if name not in existing:
                sf.toolingexecute("sobjects/CustomField", method="POST",
                                  data={"FullName": f"{obj}.{name}", "Metadata": dict(meta, label=_label(name))})
```

Es **idempotente**: mira qué campos ya existen (`describe`) y crea sólo los que faltan.

Después `ensure_permission_set` hace tres cosas:

1. Crea el permission set `Andes_Integracion`.
2. Le agrega un `FieldPermissions` (lectura y edición) por campo.
3. Se lo asigna a tu usuario.

Un campo recién creado tarda unos segundos en "existir" para los permisos: por eso hay un reintento con espera.

### 4. La carga (`salesforce/seed.py`)

```python
LOAD_ORDER = [("accounts", "Account"), ("contacts", "Contact"), ("leads", "Lead"),
              ("opportunities", "Opportunity"), ("cases", "Case")]

def upsert(sf, sobject, rows):
    results = getattr(sf.bulk2, sobject).upsert(records=rows, external_id_field="Andes_Key__c")
```

**El orden importa:** un contacto con `Account.Andes_Key__c = ACC-00012` falla si la cuenta `ACC-00012` todavía no
existe.

### 5. El notebook (`05_salesforce_seed.py`)

Es una tarea más del job `andes-bank-fuentes` (`salesforce_seed`, después de `generate_truth`):

1. Instala `simple-salesforce` con `%pip`.
2. Genera `sim.crm_*`.
3. Prepara la org.
4. Hace el upsert.

Si el scope no tiene credenciales de Salesforce, **termina sin error** y avisa: así podés correr el job antes de
tener la org.

## 🤔 Por qué así

- **¿Por qué no la Salesforce CLI (`sf`)?** Sirve, pero agrega Node.js y otro login, y no corre dentro del job de
  Databricks. Con `simple_salesforce` se usa el mismo código en tu máquina y en Databricks.
- **¿Por qué `Andes_Key__c` si ya está `Core_Customer_Id__c`?** Porque `Core_Customer_Id__c` está **sucio a
  propósito**: vacío, viejo o mal tipeado. Una clave de carga tiene que ser perfecta; un dato de negocio, no.
- **¿Por qué Bulk 2.0 y no un `create` por registro?** Mil registros uno por uno son mil llamadas a la API, y la
  org gratuita tiene un límite diario de llamadas. Bulk 2.0 usa unas pocas.

## 🧪 Probalo

En local, sin Salesforce: el test de punta a punta usa `tests/fake_salesforce.py`, que imita el upsert por clave
externa y el SOQL incremental.

```bash
pytest -q tests/test_e2e.py -k crm
```

En Databricks:

```bash
databricks bundle deploy -t dev
databricks jobs run-now --json '{"job_id": <id de andes-bank-fuentes>, "only": ["salesforce_seed"]}'
```

En Salesforce, abrí la app *Sales* → *Cuentas*: tienen que aparecer unas 515.

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `INVALID_LOGIN: Invalid username, password, security token; or user locked out` | falta el token, o cambiaste la contraseña y el token viejo dejó de servir | restablecé el token y volvé a correr `01_secrets.sh` |
| No aparece "Restablecer mi token de seguridad" | tu perfil tiene rangos de IP de inicio de sesión | *Setup → Perfiles → Administrador del sistema → Rangos de IP* (borralos) o usá una org nueva |
| `No such column 'Andes_Key__c' on entity 'Account'` | el campo existe pero no tenés permiso (FLS) | `prepare_org` crea y asigna el permission set; si lo creaste a mano, asignátelo |
| `DUPLICATE_VALUE` en `Andes_Key__c` | el campo se creó sin *Unique* o hay dos registros con la misma clave | revisá `EXTERNAL_KEY` en `org.py` |
| `STORAGE_LIMIT_EXCEEDED` | la org se llenó (5 MB) | *Setup → Almacenamiento* y borrá datos de prueba; no subas los volúmenes del generador |

## ✅ Punto de control

`git diff etapa-08 -- src/andes_bank/generator/crm.py src/andes_bank/salesforce/` no muestra diferencias de lógica.

## 🏋️ Ejercicio

Agregá a `crm_records` un objeto `Task` (llamadas del ejecutivo) con 300 registros relacionados con las cuentas, y su
campo `Andes_Key__c`. No te olvides de `CUSTOM_FIELDS` ni de `LOAD_ORDER`.
