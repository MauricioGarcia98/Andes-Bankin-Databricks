# 03 · *Spike* de Free Edition: probar antes de diseñar

> **Antes:** [02 · Cuentas, Codespaces y las herramientas](02_cuentas_y_herramientas.md). **Tiempo:** 1 hora 30.

## 🎯 Objetivo

Todo el diseño del proyecto depende de dos preguntas. Antes de escribir el banco, se **prueban**:

1. **¿Databricks puede llamar a una API de internet?** (tu API en Azure, la del BCRA). Si no puede, las APIs del
   proyecto tienen que funcionar al revés: alguien afuera trae los datos y los deja donde Databricks los lea.
2. **¿Databricks puede leer tu Azure Blob Storage directamente?** Si no puede, hace falta un paso que copie los
   archivos adentro.

Al terminar tenés:

- `docs/adr/0001_spike_outbound.py`: el notebook de prueba;
- `docs/adr/0001_free_edition_acceso_saliente.md` y `docs/adr/0002_landing_sync_blob_a_volume.md`: las **decisiones**
  que salen de la prueba, escritas.

## 📚 Conceptos

### Spike

Un **spike** es un experimento corto y descartable para sacarse una duda **antes** de construir. Lo valioso no es su
código: es la **decisión** que permite tomar. Media hora de prueba ahorra días de construir algo que después no anda.

### ADR (*Architecture Decision Record*)

Un documento cortito que registra **una** decisión importante: el **contexto** (qué problema había), la **prueba** (qué
se hizo para saber), la **decisión** y sus **consecuencias**. Dentro de seis meses, cuando alguien pregunte "¿por qué
copian los archivos en vez de leerlos directo?", la respuesta está escrita, con fecha. Se numeran (`0001`, `0002`…) y
nunca se borran: si una decisión cambia, se escribe un ADR nuevo que reemplaza al viejo.

### *Pull* y *push*

- ***Pull*** (tirar): Databricks **va a buscar** los datos (llama a la API, lee el storage). Es lo normal en un equipo
  de datos: el que procesa decide cuándo y cuánto trae.
- ***Push*** (empujar): la fuente **deja** los datos donde Databricks los lee. Hace falta cuando el que procesa no
  puede salir a buscarlos.

Para hacer *pull*, Databricks necesita **acceso saliente** a internet. Free Edition lo limita a unos pocos dominios
hasta que verificás la cuenta.

### HTTP en 30 segundos

Un pedido **HTTP** es lo que hace un navegador al abrir una página: se manda un pedido a una dirección (URL) y vuelve
una respuesta con un **código**: `200` = OK, `401` = no autorizado, `404` = no existe, `429` = demasiados pedidos,
`500` = error del servidor. Que vuelva **cualquier** código significa que la conexión llegó: alguien contestó. Lo que
indica que **no hay salida a internet** es un error de conexión, sin código.

### Notebook y job

Un **notebook** de Databricks es un archivo con código dividido en celdas. Se puede ejecutar a mano (en la interfaz) o
como **job** (una ejecución programada o puntual, sin que estés mirando). El curso usa jobs: se lanzan desde la terminal
y dejan registro.

## 🧱 Paso a paso

### 1. La carpeta de decisiones

```bash
cd /workspaces/andes-bank
mkdir -p docs/adr
```

- `docs/`: toda la documentación del proyecto (en el capítulo final tiene definición, arquitectura, bitácora…).
- `docs/adr/`: las decisiones de arquitectura, una por archivo.

### 2. El notebook de la prueba

`code docs/adr/0001_spike_outbound.py` y escribí:

```python
# Databricks notebook source
```

- Esa primera línea, exacta, le dice a Databricks que el archivo `.py` es un **notebook** (y no un script suelto) al
  importarlo.

```python
import urllib.request, json, ssl
```

- `import` trae módulos de la biblioteca estándar de Python (no hace falta instalar nada):
  `urllib.request` hace pedidos HTTP y `json` convierte entre texto JSON y estructuras de Python. (`ssl` no se usa: quedó
  de una versión anterior de la prueba).
- **Por qué `urllib` y no `requests`** (la biblioteca más común): para que el spike no dependa de nada instalado. Si
  falla, falla por la red, no por una dependencia.

```python
hosts = {
  "bcra": "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones",
  "azure_blob": "https://azure.microsoft.com",
  "azure_functions": "https://functions.azure.com",
  "salesforce": "https://login.salesforce.com",
  "github": "https://api.github.com",
}
```

- Un **diccionario** (`{clave: valor}`): a cada nombre le corresponde una URL a probar.
- Cada una representa una fuente posible: la API **real** del BCRA (cotizaciones del dólar), Azure (donde va a vivir la
  API del banco), Salesforce (el CRM que se intentó usar al principio y se abandonó: bitácora R17) y GitHub (un control:
  si esto falla, falla todo).

```python
res = {}
for k, u in hosts.items():
```

- `res = {}`: un diccionario vacío donde se guarda el resultado de cada prueba.
- `for k, u in hosts.items()`: recorre el diccionario; en cada vuelta `k` es el nombre y `u` la URL.

```python
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "andes-spike"}), timeout=15)
        res[k] = f"OK {r.status}"
```

- `try:` = "intentá esto; si algo falla, no te caigas: andá al `except`".
- `urllib.request.Request(u, headers=...)`: arma el pedido. El `User-Agent` es una "firma" del programa: algunos
  servidores rechazan pedidos sin firma.
- `urlopen(..., timeout=15)`: lo manda y espera como mucho 15 segundos.
- `f"OK {r.status}"`: un **f-string**, texto con valores adentro: queda `"OK 200"`.

```python
    except urllib.error.HTTPError as e:
        res[k] = f"HTTP {e.code} (llega)"
```

- Si el servidor contestó con un **error HTTP** (404, 401…), Python lo trata como excepción. Para el spike **es un
  éxito**: la conexión salió y alguien contestó. Por eso el texto dice "llega".

```python
    except Exception as e:
        res[k] = f"FALLA {type(e).__name__}: {str(e)[:120]}"
```

- Cualquier otro error (sin conexión, proxy, DNS) es el que importa: **no hay salida**. Se guarda el tipo de error y los
  primeros 120 caracteres del mensaje (`[:120]` corta el texto).

```python
print(json.dumps(res, indent=2))
dbutils.notebook.exit(json.dumps(res))
```

- `print(json.dumps(res, indent=2))`: muestra el resultado como JSON prolijo.
- `dbutils.notebook.exit(...)`: `dbutils` existe sólo dentro de Databricks; `exit` termina el notebook **devolviendo**
  ese texto. Así el resultado se puede leer desde la terminal sin entrar a la interfaz.

**Qué NO hace este notebook:** no prueba tu API de Azure (todavía no existe: capítulo 09) ni leer tu Blob (capítulo
08). Prueba que la **salida** a internet funciona.

### 3. Correrlo en Databricks como job

```bash
YO=$(databricks current-user me --output json | python -c "import json,sys; print(json.load(sys.stdin)['userName'])")
echo $YO
```

- Guarda tu mail de Databricks en la variable `YO`. `python -c "..."` ejecuta una línea de Python: lee el JSON que
  devolvió la CLI y saca el campo `userName`.

```bash
databricks workspace import /Users/$YO/spike_outbound --file docs/adr/0001_spike_outbound.py \
  --language PYTHON --format SOURCE --overwrite
```

- **Sube** el archivo al workspace de Databricks, en tu carpeta personal, como notebook de Python. `--overwrite`
  reemplaza si ya existía. La `\` al final de la línea significa "el comando sigue en la línea de abajo".

```bash
databricks jobs submit --json '{"run_name": "spike outbound", "tasks": [{"task_key": "spike",
  "notebook_task": {"notebook_path": "/Users/'$YO'/spike_outbound"}}]}'
```

- `jobs submit`: lanza **una** ejecución, sin crear un job permanente. El JSON describe una tarea que corre el notebook.
  Como no dice en qué cluster, Databricks usa **serverless** (el único cómputo de Free Edition).
- El comando **espera** a que termine (1 a 3 minutos) y muestra el estado: tiene que terminar en `SUCCESS`.

Para ver lo que devolvió el notebook: copiá el `run_id` que imprimió el comando y

```bash
databricks jobs get-run <run_id> --output json | python -c "import json,sys; print(json.load(sys.stdin)['tasks'][0]['run_id'])"
databricks jobs get-run-output <el run_id de la tarea> --output json | python -c "import json,sys; print(json.load(sys.stdin)['notebook_output'])"
```

(Una ejecución tiene un id y **cada tarea** adentro tiene el suyo; la salida del notebook se pide con el de la tarea.)
También lo ves en la interfaz: **Jobs & Pipelines → Job runs → spike outbound**.

### 4. La segunda pregunta: ¿se puede leer Blob directo?

Esta prueba se hizo con la referencia y su resultado está en el ADR 0002. Las dos formas "normales" de que Databricks
lea un Azure Blob Storage **no funcionan en Free Edition**:

- una ***external location*** de Unity Catalog (el catálogo apuntando a tu storage) → Free Edition no deja crearla
  hacia un storage propio;
- poner la credencial en la configuración de Spark (`spark.conf.set("fs.azure...", ...)`) → el cómputo serverless no
  lo acepta.

Lo que **sí** funciona: la biblioteca de Azure para Python (`azure-storage-blob`) con una **SAS** (una "llave" con
permisos limitados y vencimiento, capítulo 08). Con eso se puede **copiar** archivos de Blob a un **Volume** (una
carpeta dentro del catálogo de Databricks), y desde ahí leerlos. Ese es el capítulo 12. Cuando tengas Azure armado
(capítulo 08) podés repetir la prueba vos: el ejercicio de este capítulo te lo pide.

### 5. Escribir los dos ADR

`code docs/adr/0001_free_edition_acceso_saliente.md` — usá **tus** resultados y **tu** fecha:

```markdown
# ADR 0001 · Free Edition con acceso saliente: ingesta *pull*

- **Fecha:** 2026-10-21
- **Estado:** aceptado

## Contexto

La doc oficial de Free Edition dice que el acceso saliente a internet está restringido a dominios de confianza y que
se desbloquea verificando la cuenta con LinkedIn. Necesitábamos saber si Databricks puede llamar directamente a las
APIs del proyecto (Azure Functions, Blob, Salesforce, BCRA).

## Prueba

Un notebook (`0001_spike_outbound.py`) corrido como job serverless con `databricks jobs submit`, que hace un `GET` a
cada dominio. Resultado con la cuenta verificada:

| destino | resultado |
|---|---|
| `api.bcra.gob.ar` (API real de cotizaciones) | OK 200 |
| Azure (`azure.microsoft.com`, `functions.azure.com`) | OK 200 |
| `login.salesforce.com` | OK 200 |
| `api.github.com` | OK 200 |

## Decisión

**Patrón *pull*:** los jobs de Databricks llaman a las APIs y leen Blob directamente. El plan B (*push* con una Azure
Function que deja archivos) queda documentado pero no se construye.

## Consecuencias

- El cliente de API corre dentro de Databricks: los reintentos, el backoff y los checkpoints se escriben en Python y
  quedan registrados en `ops.api_calls`.
- Hay que repetir la prueba en tu cuenta nueva cuando reconstruyas el proyecto: si no verificaste la cuenta, el job
  falla con errores de conexión, no con códigos HTTP.
```

- Está escrito en **Markdown** (`.md`): texto con marcas simples. `#` = título, `-` = lista, `**negrita**`, `|` arma
  tablas. GitHub lo muestra con formato.
- Fijate que la decisión dice "leen Blob directamente": eso **cambió** con la prueba de la sección 4. Por eso existe el
  ADR 0002, que no borra al 0001: lo **corrige**.

`code docs/adr/0002_landing_sync_blob_a_volume.md`:

```markdown
# ADR 0002 · Blob → Volume con una SAS de sólo lectura (y validación en el borde)

- **Fecha:** 2026-10-21
- **Estado:** aceptado

## Contexto

Los sistemas origen dejan sus archivos en Azure Blob. Lo ideal sería que Auto Loader los lea directo
(`abfss://...`), pero en Databricks Free Edition:

- no se pueden crear *external locations* hacia un storage propio (la credencial de Unity Catalog necesitaría
  además guardar el secreto de un service principal de Azure en el workspace);
- el cómputo serverless no acepta credenciales de Azure en la configuración de Spark (`fs.azure.*`).

## Decisión

Un paso de **sincronización** (`andes_bank.ingestion.landing_sync`) lista los contenedores con una SAS de **sólo
lectura** (secret scope `andes_bank/blob_sas_read`) y copia lo nuevo al Volume `andes_bank.landing.raw`. Auto Loader lee
el Volume.

La copia es el borde del lakehouse, y ahí se valida cada archivo:

| estado | qué pasó | qué se hace |
|---|---|---|
| `LANDED` | archivo sano | se copia al Volume |
| `EMPTY` | sólo encabezado | se copia y queda la alerta (ING-02) |
| `CORRUPT` | el gzip no descomprime | va a `_errors/`, no al Volume (ING-03) |
| `DUPLICATE_CONTENT` | mismo contenido que otro ya aterrizado | no se copia (ING-01) |
| `CHANGED` | el blob cambió después de copiarlo | alerta; no se re-copia |

## Consecuencias

- Es un patrón real (ADF / Fivetran "copy to landing"), no sólo un parche de Free Edition.
- Un archivo corrupto ya no tumba el stream de Auto Loader: se queda en el borde con su motivo en `ops.landing_files`.
- En un workspace pago con Unity Catalog sobre Azure, se reemplaza por una *external location* y Auto Loader directo;
  la validación de contenido pasaría a un paso de calidad sobre bronze.
```

- Este ADR menciona cosas que todavía no existen (`landing_sync`, Auto Loader, `ops.landing_files`): es normal. Un ADR
  se escribe **cuando se decide**, antes de construir. Lo vas a entender del todo en el capítulo 12.

### 6. Guardar en Git

```bash
git add docs/
git commit -m "Spike de Free Edition: acceso saliente (ADR 0001) y Blob → Volume (ADR 0002)"
git push
```

## 🤔 Por qué así

- **¿Por qué no asumir que anda?** La documentación de Free Edition cambia, y si el acceso saliente no funciona, la
  mitad del proyecto (todas las APIs) cambia de diseño.
- **¿Por qué copiar a un Volume y no leer Blob directo?** Porque Free Edition no deja otra. Pero además es un patrón
  real (las herramientas de ingesta de las empresas hacen exactamente eso), y el borde es el mejor lugar para validar
  archivos antes de que entren.
- **¿Por qué guardar el notebook del spike si es descartable?** Porque es la **evidencia** del ADR: cualquiera puede
  volver a correrlo en su cuenta (y tiene que hacerlo, porque la verificación es por cuenta).

## 🧪 Probalo

La salida del notebook tiene que mostrar `OK 200` (o `HTTP …(llega)`) en las cinco entradas. Ninguna con `FALLA`.

## 🔥 Si falla

| síntoma | por qué | qué hacer |
|---|---|---|
| `FALLA ConnectionError` / `ProxyError` en casi todos | la cuenta de Free Edition no está verificada | verificala (capítulo 02, paso 2) y repetí el paso 3 |
| `databricks workspace import` dice `RESOURCE_ALREADY_EXISTS` | ya lo subiste | agregá `--overwrite` |
| `jobs submit` termina en `INTERNAL_ERROR` sin más | un error del notebook | mirá la ejecución en *Jobs & Pipelines → Job runs*: muestra la celda que falló |
| más adelante, `[UNSUPPORTED_FEATURE] external location` | Free Edition no admite storage propio | es lo esperado: por eso existe el capítulo 12 (bitácora R05) |

## ✅ Punto de control

`docs/adr/` tiene tres archivos. Tus dos ADR dicen lo mismo que los de la referencia, con **tus** resultados y tu fecha.

## 🏋️ Ejercicio

Cuando tengas la API desplegada (capítulo 09), agregá al diccionario `hosts` la URL
`https://<tu-function>.azurewebsites.net/api/health`, volvé a correr el spike y anotá el resultado en el ADR 0001 como
una fila más de la tabla.
