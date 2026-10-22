# 01 · Borrar todo (y guardar la referencia)

## 🎯 Objetivo

Desmontar el proyecto que construyó Claude **sin dejar restos que cuesten plata** y **sin perder la referencia**: el
repo terminado queda guardado para comparar al final de cada etapa.

El orden importa. Primero se borra lo que **depende** de otra cosa y al final lo que la sostiene:

1. Genie.
2. Los jobs.
3. El catálogo.
4. Los secretos.
5. Azure.
6. El repo.

Si borrás Azure antes que los jobs, el job diario va a seguir intentando leer un Storage que ya no existe y te va a
llenar el mail de fallas.

## 📚 Conceptos

- **Lo que cuesta plata es sólo Azure.** Databricks Free Edition es gratis; igual se borra
  para empezar de cero de verdad.
- **Borrar un resource group borra todo lo que tiene adentro**: Storage, Function App y Application Insights. Es la
  forma más segura de no olvidarse de nada.
- **Un nombre global de Azure queda reservado unos minutos** después de borrar la cuenta de Storage. Si al
  reconstruir te dice que el nombre está tomado, esperá un rato.

## 🧱 Paso a paso

### 0. Guardá la referencia

El repo del proyecto terminado **no se borra**: se renombra. Así podés comparar cada etapa con su tag.

```bash
# en GitHub: Settings → General → Repository name → andes-bank-referencia
git clone https://github.com/<tu-usuario>/andes-bank-referencia.git ~/andes-bank-referencia
cd ~/andes-bank-referencia && git tag | sort -V     # etapa-02 … etapa-24
```

Anotá también las respuestas de los agents de Genie que te gustaron (capturas). Te van a servir para comparar al final.

### 1. Genie Agents

```bash
cd ~/andes-bank-referencia
python - <<'PY'
import json
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for slug, sid in json.load(open("genie/deployed_dev.json")).items():
    w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{sid}")
    print("borrado", slug)
PY
```

**Por qué primero:** un space apunta a tablas y funciones del catálogo. Si borrás el catálogo antes, el space queda
roto pero sigue apareciendo en la lista.

### 2. Jobs y archivos del bundle

```bash
databricks bundle destroy -t dev --auto-approve
```

Borra los dos jobs (`andes-bank-fuentes` y `andes-bank-lakehouse`) y la carpeta `.bundle/andes-bank` del workspace.
Revisá en *Jobs & Pipelines* que no quede ninguno con el prefijo `[dev …]`.

### 3. El catálogo (datos, checkpoints y Volume)

```bash
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "<id del Serverless Starter Warehouse>",
  "statement": "DROP CATALOG IF EXISTS andes_bank CASCADE", "wait_timeout": "50s"}'
```

`CASCADE` borra todos los esquemas: `sim`, `landing` (con el Volume `raw` y los checkpoints de Auto Loader),
`bronze`, `silver`, `gold`, `semantic` y `ops`. Si no borrás los checkpoints y reconstruís con el mismo nombre, Auto
Loader "se acuerda" de los archivos viejos y no lee los nuevos.

### 4. El secret scope

```bash
databricks secrets delete-scope andes_bank
```

Contiene las SAS de Blob, el client id/secret de la API (y, si los cargaste en una versión vieja, los `sf_*` de Salesforce). Las SAS dejan de servir igual
cuando borrás el Storage, pero no dejes secretos colgados.

### 5. Azure

```bash
export AZURE_SUBSCRIPTION_ID=<tu suscripción>
bash scripts/azure/99_teardown.sh             # pide que escribas el nombre del resource group para confirmar
az group exists -n rg-andes-bank              # unos minutos después: false
```

Revisá también:

- **Presupuesto:** el de USD 10 en *Cost Management → Presupuestos* **dejalo**. No cuesta nada y te protege en la
  reconstrucción.
- **Service principal** (si creaste uno para la CLI): *Microsoft Entra ID → Registros de aplicaciones* → borralo, o
  rotá su secreto.
- **Costos:** *Cost Management → Análisis de costos* al día siguiente. Tiene que aparecer el gasto del mes (centavos) y
  nada nuevo después del borrado.

### 6. El repo nuevo

```bash
mkdir ~/andes-bank && cd ~/andes-bank && git init -b main
gh repo create andes-bank --private --source . --push     # o desde la web de GitHub
```

Desde acá seguís con el capítulo 02.

## 🧪 Probalo

| qué | comando | resultado esperado |
|---|---|---|
| Genie | `databricks api get /api/2.0/genie/spaces` | sin spaces `Andes · …` |
| Jobs | `databricks jobs list` | sin `andes-bank-*` |
| Catálogo | `databricks catalogs list` | sin `andes_bank` |
| Secretos | `databricks secrets list-scopes` | sin `andes_bank` |
| Azure | `az group exists -n rg-andes-bank` | `false` |

## 🔥 Si falla

- **`DROP CATALOG` responde que el catálogo tiene objetos dependientes:** te faltó `CASCADE`.
- **`bundle destroy` dice que no encuentra el estado:** corrélo desde la carpeta del repo de referencia (ahí está
  `databricks.yml`). Si igual falla, borrá los jobs a mano desde la UI.
- **Azure sigue cobrando al otro día:** casi siempre es otro resource group. Buscá con
  `az resource list --tag proyecto=andes-bank -o table`.

## ✅ Punto de control

Todo borrado salvo el repo de referencia y el presupuesto de Azure.

## 🏋️ Ejercicio

Escribí un `scripts/borrar_todo.sh` que haga los pasos 1 a 5 en orden, y que **pida confirmación** antes del 3 y del 5.
