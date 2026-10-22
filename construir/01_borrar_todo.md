# 01 · Borrar todo (y guardar la referencia)

> **Antes:** [00 · Antes de empezar](00_antes_de_empezar.md). **Tiempo:** 45 minutos.

## 🎯 Objetivo

Claude Code construyó el proyecto completo y funcionando: esa es la **referencia**. Ahora lo vas a desarmar para
construirlo vos desde cero. Al terminar este capítulo:

- el repo terminado queda **guardado con otro nombre** (para comparar tu código con él en cada capítulo);
- en Databricks y en Azure **no queda nada** del proyecto (ni nada que cueste plata);
- tenés un repo **vacío** en GitHub, listo para el capítulo 02.

**Por qué empezar borrando:** si reconstruís encima de lo que ya existe, nunca sabés si algo anda porque lo hiciste
bien o porque quedó de antes. Por ejemplo, Auto Loader (capítulo 13) "se acuerda" de los archivos que ya leyó: si no
borrás esa memoria, tu versión nueva va a parecer que no lee nada.

> ⚠️ Si nunca tuviste el proyecto armado (llegaste directo al curso), saltá a la sección 7: sólo tenés que crear el
> repo vacío.

## 📚 Conceptos

### El orden importa: primero lo que depende, después lo que sostiene

Las piezas del proyecto se apoyan unas en otras:

```
Genie Agents ──usan──► tablas y funciones del catálogo
Jobs ─────────leen───► Azure (Storage y la API) y escriben ──► el catálogo
Catálogo ─────guarda─► los datos, los checkpoints y el Volume
Secret scope ─guarda─► las credenciales de Azure
Azure ────────guarda─► los archivos del banco y la API
```

Se borra **de arriba hacia abajo**: Genie, jobs, catálogo, secretos, Azure. Si borrás Azure primero, los jobs quedan
apuntando a un Storage que no existe: la próxima vez que alguien los corra, fallan con errores que confunden.

### Qué cuesta plata

Sólo **Azure**, y son centavos por mes. Databricks Free Edition es gratis. GitHub y Codespaces tienen una cuota gratis
que alcanza. Igual se borra todo: el objetivo es empezar de cero de verdad.

### Un *resource group* es una caja

En Azure todo recurso (un Storage, una Function) vive dentro de un **resource group**. Borrar el resource group borra
todo lo que tiene adentro: es la forma más segura de no olvidarse de nada.

## 🧱 Paso a paso

### 1. Guardá la referencia: renombrar el repo

En GitHub, en la página del repo terminado: **Settings → General → Repository name** → escribí
`andes-bank-referencia` → **Rename**.

- **Qué hace:** cambia el nombre del repo. La historia, las ramas y los tags quedan intactos.
- **Por qué renombrar y no borrar:** en cada capítulo vas a comparar tu código con el de la referencia (`git diff`).
- **Qué NO hace:** no toca Databricks ni Azure. Los jobs siguen andando hasta el paso 3.

### 2. Abrí un codespace sobre la referencia

En la página de `andes-bank-referencia`: botón verde **Code → Codespaces → Create codespace on main**. Esperá a que
termine de preparar el entorno (2 a 5 minutos la primera vez: instala Python, Java y las CLIs según
`.devcontainer/devcontainer.json`). Cuando abajo aparezca la terminal con algo como
`@tu-usuario ➜ /workspaces/andes-bank-referencia (main) $`, ya está.

Las credenciales (Databricks y Azure) se cargan como **secretos de Codespaces**: si todavía no lo hiciste, seguí la
sección 4 del [capítulo 02](02_cuentas_y_herramientas.md#4-los-secretos-de-codespaces) y volvé.

Comprobá que las dos CLIs te reconocen:

```bash
databricks current-user me --output json | grep userName    # tu email de Databricks
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" -o none
az account show --query name -o tsv                          # el nombre de tu suscripción de Azure
```

- `databricks current-user me`: le pregunta a Databricks "¿quién soy?". Usa las variables `DATABRICKS_HOST` y
  `DATABRICKS_TOKEN` que pusiste como secretos.
- `az login --service-principal ...`: entra a Azure con el "usuario robot" del proyecto (capítulo 02). `-o none` = no
  mostrar nada si salió bien.

### 3. Borrar los Genie Agents

```bash
python - <<'PY'
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()                                   # se conecta con DATABRICKS_HOST y DATABRICKS_TOKEN
for slug, space_id in json.load(open("genie/deployed_dev.json")).items():
    w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
    print("borrado", slug)
PY
```

- **Qué hace:** `python - <<'PY' ... PY` ejecuta el código que está entre las dos marcas `PY` sin crear un archivo.
  El programa lee `genie/deployed_dev.json` (donde el deploy anotó el id de cada agent) y le pide a Databricks que
  borre cada uno.
- **Por qué primero:** un agent apunta a tablas y funciones del catálogo. Si borrás el catálogo antes, el agent queda
  roto pero sigue apareciendo en la lista de Genie.
- **Qué NO hace:** no borra tablas ni datos.

### 4. Borrar los jobs

```bash
databricks bundle destroy -t dev --auto-approve
```

- **Qué hace:** lee `databricks.yml` (la descripción de todo lo que el proyecto creó en Databricks) y borra los tres
  jobs (`andes-bank-fuentes`, `andes-bank-dia-nuevo`, `andes-bank-lakehouse`) y la carpeta `.bundle/andes-bank` del
  workspace. `--auto-approve` = no preguntar "¿estás seguro?".
- **Por qué antes del catálogo y de Azure:** un job que quedó apuntando a cosas borradas falla en cuanto alguien lo corre.
- **Comprobalo:** en Databricks, menú **Jobs & Pipelines**: no tiene que quedar ninguno con el prefijo `[dev …]`.

### 5. Borrar el catálogo

El catálogo `andes_bank` tiene todas las tablas, el Volume con los archivos y los checkpoints de Auto Loader. Se borra
con SQL. En Databricks: menú **SQL Editor**, pegá y ejecutá:

```sql
DROP CATALOG IF EXISTS andes_bank CASCADE;
```

- `DROP CATALOG` borra el catálogo; `IF EXISTS` evita un error si ya no estaba; **`CASCADE`** borra también todo lo de
  adentro (sin `CASCADE`, falla si el catálogo tiene esquemas).
- **Qué NO hace:** no borra el warehouse SQL ni tu usuario.

### 6. Borrar los secretos

```bash
databricks secrets delete-scope andes_bank
```

El *secret scope* `andes_bank` guarda las credenciales para leer y escribir en Azure y para la API. Cuando se borre
Azure dejan de servir, pero no se dejan secretos colgados: es una costumbre de seguridad.

### 7. Borrar Azure

```bash
export AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
bash scripts/azure/99_teardown.sh            # te pide escribir el nombre del resource group para confirmar
```

- La primera línea guarda en una variable el id de tu suscripción (los scripts lo necesitan).
- `99_teardown.sh` borra el resource group `rg-andes-bank` con todo lo de adentro. Pide que escribas el nombre a mano:
  así no se borra nada por accidente.

Unos minutos después:

```bash
az group exists -n rg-andes-bank             # tiene que decir: false
```

**Lo que NO se borra:**

- el **presupuesto** de USD 10 (*Cost Management → Presupuestos*): no cuesta nada y te protege en la reconstrucción;
- el **service principal** `andes-bank-cli`: lo vas a usar de nuevo. Si querés empezar de cero total, borralo en
  *Microsoft Entra ID → Registros de aplicaciones* y crealo otra vez en el capítulo 02.

### 8. El repo nuevo, vacío

En GitHub: **+ (arriba a la derecha) → New repository**:

- **Repository name:** `andes-bank`
- **Private**
- ✅ **Add a README file** (así el repo no queda vacío y se puede abrir un codespace)
- **Create repository**

Cerrá el codespace de la referencia (☰ → *Codespaces: Stop Current Codespace*): no lo borres, lo vas a usar para
comparar.

## 🧪 Probalo

Desde el codespace de la referencia:

| qué | comando | qué tenés que ver |
|---|---|---|
| Genie | `databricks api get /api/2.0/genie/spaces` | ningún space `Andes · …` |
| Jobs | `databricks jobs list` | ningún `andes-bank-*` |
| Catálogo | `databricks catalogs list` | no aparece `andes_bank` |
| Secretos | `databricks secrets list-scopes` | no aparece `andes_bank` |
| Azure | `az group exists -n rg-andes-bank` | `false` |

## 🔥 Si falla

| síntoma | por qué | qué hacer |
|---|---|---|
| `DROP CATALOG` dice que tiene objetos | faltó `CASCADE` | agregalo |
| `bundle destroy` no encuentra el estado | no estás parado en la carpeta del repo | `cd /workspaces/andes-bank-referencia` y repetí; si sigue, borrá los jobs desde la interfaz |
| `az` responde `Please run 'az login'` | no entraste con el service principal | repetí el `az login --service-principal` del paso 2 |
| Azure sigue cobrando al otro día | quedó otro resource group | `az resource list --tag proyecto=andes-bank -o table` |

## ✅ Punto de control

- Existe `andes-bank-referencia` (con sus tags) y `andes-bank` (vacío, con un README).
- En Databricks y en Azure no quedó nada del proyecto, salvo el presupuesto.

## 🏋️ Ejercicio

¿Qué pasaría si borrás el secret scope **antes** que los jobs y alguien corre el lakehouse? Escribí en una línea qué
tarea fallaría primero y con qué error (pista: capítulo 12).
