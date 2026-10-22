# 02 · Cuentas, Codespaces y las herramientas

> **Antes:** [01 · Borrar todo](01_borrar_todo.md). **Tiempo:** 2 horas (la mitad es esperar mails de confirmación).

## 🎯 Objetivo

Dejar listo **el lugar de trabajo** antes de escribir una línea de código del banco. Al terminar:

1. tenés las tres cuentas: **GitHub**, **Databricks Free Edition** y **Azure** (con un presupuesto que te avisa si se
   gasta de más);
2. tu repo `andes-bank` tiene un **codespace**: una computadora en la nube con Python 3.12, Java 17, `az` y
   `databricks` instalados solos;
3. las **credenciales** están guardadas como secretos de Codespaces, nunca en un archivo del repo;
4. las dos CLIs te responden con tu usuario.

**Por qué esto va antes que el código:** todo lo demás depende de estas cuentas. Si a mitad del curso descubrís que
tu Databricks no puede salir a internet o que Azure no te deja crear algo, tenés que rehacer trabajo. Acá se prueba
todo junto, una vez.

## 📚 Conceptos

### Variables de entorno

Una **variable de entorno** es un valor con nombre que la terminal le pasa a todos los programas que ejecuta. Por
ejemplo, `DATABRICKS_TOKEN=dapi123...`. La CLI de Databricks, cuando arranca, busca esa variable y la usa para
identificarse: no hace falta escribir el token en cada comando.

```bash
echo $HOME            # muestra el valor de la variable HOME
export SALUDO=hola    # crea una variable en ESTA terminal (se pierde al cerrarla)
echo $SALUDO
```

### Por qué los secretos nunca van en el repo

Un **token** o una **contraseña** en un archivo del repo queda en la historia de Git **para siempre** (aunque después
lo borres, el commit viejo lo tiene). Si el repo se hace público o alguien lo clona, tiene tus credenciales. Por eso:

- en tu codespace, las credenciales llegan como **secretos de Codespaces** (GitHub las guarda cifradas y las convierte
  en variables de entorno al abrir el codespace);
- en Databricks, el código las lee de un **secret scope** (capítulo 08);
- en la API de Azure, de la **configuración de la Function App** (capítulo 08).

### El "usuario robot" de Azure: service principal

Para que los scripts entren a Azure sin tu contraseña personal, se crea un **service principal**: una identidad para
programas, con su propio id y su propia clave. Si la clave se filtra, la borrás sin tocar tu cuenta.

### Codespaces y `devcontainer.json`

Un **codespace** es una máquina Linux en la nube. ¿Qué tiene instalado? Lo decide el archivo
`.devcontainer/devcontainer.json` del repo: una "receta" que GitHub lee al crear la máquina. Sin ese archivo, GitHub
usa una imagen genérica enorme con de todo. Con él, **todos** los que abran el repo tienen exactamente las mismas
versiones: el problema "en mi máquina anda" desaparece.

## 🧱 Paso a paso

### 1. GitHub y la cuota de Codespaces

1. Si no tenés cuenta, creala en <https://github.com/signup> y activá la verificación en dos pasos
   (**Settings → Password and authentication**).
2. **Settings → Codespaces → Default idle timeout:** `30 minutes`. Un codespace prendido consume horas aunque no lo
   uses; así se apaga solo.
3. Mirá tu cuota en **Settings → Billing and plans**. Una cuenta personal gratis tiene **120 horas-núcleo por mes** (con
   la máquina de 2 núcleos: 60 horas de uso) y 15 GB de almacenamiento. El curso entra holgado si apagás el codespace
   cuando terminás.

### 2. Databricks Free Edition

1. Creá la cuenta en <https://www.databricks.com/learn/free-edition>. Te crea un **workspace** (tu espacio en
   Databricks) con una URL del tipo `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`. **Anotá esa URL**: es tu
   `DATABRICKS_HOST`.
2. **Verificá la cuenta** (perfil → *Settings*, opción de verificación con LinkedIn). Sin verificar, Databricks sólo
   puede llamar a unos pocos sitios de internet "de confianza" y **no** va a poder llamar a tu API de Azure ni a la del
   BCRA. El capítulo 03 lo comprueba.
3. Creá un **token personal**: perfil → **Settings → Developer → Access tokens → Generate new token**. Nombre:
   `codespaces-andes-bank`, vencimiento: 90 días. Copialo **ahora** (después no se vuelve a mostrar): es tu
   `DATABRICKS_TOKEN`. Empieza con `dapi`.

**Qué NO podés hacer en Free Edition** (y el curso lo resuelve): crear clusters propios (todo es *serverless*), crear
catálogos por API (se crea uno a mano con SQL) y conectar tu propio storage como *external location* (capítulo 12).

### 3. Azure: cuenta, presupuesto y service principal

1. Creá la cuenta en <https://azure.microsoft.com/free> (pide tarjeta, pero el proyecto gasta centavos).
2. **Presupuesto:** en el portal, buscá *Cost Management* → **Budgets → Add**: USD 10 por mes, alertas al 50 %, 80 % y
   100 % a tu mail. Un presupuesto **no frena** el gasto: **avisa**. El tope real lo ponen las decisiones del proyecto
   (Storage barato y una Function que cobra por uso).
3. **El service principal**, desde tu codespace (todavía con la imagen genérica, que ya trae `az`). Abrí un codespace
   en `andes-bank` (**Code → Codespaces → Create codespace on main**) y en la terminal:

```bash
az login --use-device-code
```

`az login` necesita un navegador; en un codespace no hay, así que `--use-device-code` te muestra un código y una URL
(<https://microsoft.com/devicelogin>): la abrís en tu navegador, pegás el código y entrás con tu cuenta de Azure.

```bash
SUB=$(az account show --query id -o tsv)      # el id de tu suscripción, guardado en la variable SUB
echo $SUB
az ad sp create-for-rbac --name andes-bank-cli --role Contributor --scopes /subscriptions/$SUB
```

- `az account show --query id -o tsv`: muestra los datos de tu suscripción y se queda sólo con el `id`, como texto.
- `$(...)` ejecuta el comando de adentro y pone su resultado en la variable.
- `az ad sp create-for-rbac`: crea el service principal con el rol **Contributor** (puede crear y borrar recursos)
  **sólo** sobre tu suscripción.

La respuesta es un JSON con cuatro valores. **No lo pegues en ningún lado salvo en el paso siguiente:**

```json
{ "appId": "…", "displayName": "andes-bank-cli", "password": "…", "tenant": "…" }
```

| valor | será el secreto |
|---|---|
| `appId` | `AZURE_CLIENT_ID` |
| `password` | `AZURE_CLIENT_SECRET` |
| `tenant` | `AZURE_TENANT_ID` |
| el `$SUB` de antes | `AZURE_SUBSCRIPTION_ID` |

### 4. Los secretos de Codespaces

En GitHub: **Settings (tu perfil) → Codespaces → Secrets → New secret**. Creá estos seis, y en **Repository access**
elegí `andes-bank` en cada uno:

| nombre | valor |
|---|---|
| `DATABRICKS_HOST` | la URL del workspace, con `https://` y sin `/` al final |
| `DATABRICKS_TOKEN` | el token `dapi…` |
| `AZURE_SUBSCRIPTION_ID` | el id de la suscripción |
| `AZURE_TENANT_ID` | `tenant` |
| `AZURE_CLIENT_ID` | `appId` |
| `AZURE_CLIENT_SECRET` | `password` |

- **Qué hace:** cada vez que se abre un codespace de `andes-bank`, GitHub crea esas seis variables de entorno.
- **Por qué así:** el valor nunca toca un archivo; si borrás el codespace, el secreto sigue en GitHub para el próximo.
- **Qué NO hace:** no los manda a Databricks ni a Azure. Para que el código que corre **en** Databricks tenga
  credenciales, está el secret scope (capítulo 08).

Los secretos nuevos se cargan al **reiniciar** el codespace: ☰ → *Codespaces: Stop Current Codespace*, y volvé a
abrirlo desde **Code → Codespaces**.

### 5. La receta del codespace: `.devcontainer/`

Ahora le decís a GitHub qué instalar. En la terminal del codespace (estás parado en `/workspaces/andes-bank`):

```bash
mkdir -p .devcontainer
code .devcontainer/devcontainer.json
```

- `mkdir -p .devcontainer`: crea la carpeta. Empieza con punto porque es una carpeta "de configuración": los
  programas la buscan por ese nombre exacto y queda oculta en un `ls` común.
- `code ...`: crea el archivo y lo abre en el editor.

Escribí este contenido (el archivo es JSON; las líneas con `//` son comentarios, que este tipo de JSON acepta):

```jsonc
// El entorno de trabajo del proyecto en GitHub Codespaces: una computadora en la nube con todo instalado.
// GitHub lo lee al crear el codespace (botón "Code → Codespaces → Create codespace on main").
{
  "name": "Andes Bank",
```

- `"name"`: el nombre que ves en la lista de codespaces.

```jsonc
  // la base: Linux (Debian) con Python 3.12 ya instalado
  "image": "mcr.microsoft.com/devcontainers/python:1-3.12-bookworm",
```

- `"image"`: la **imagen** es una "foto" de un sistema operativo ya preparado. Esta es la oficial de Microsoft con
  Python 3.12 sobre Debian 12 (*bookworm*).
- **Por qué 3.12:** es la versión de Python del runtime serverless de Databricks. Si tus tests corren con otra, algo
  que anda acá puede fallar allá.

```jsonc
  "features": {
    // Java 17: Spark corre sobre Java (los tests en local lo necesitan)
    "ghcr.io/devcontainers/features/java:1": { "version": "17", "jdkDistro": "tem" },
    // la CLI de Azure: el comando `az`
    "ghcr.io/devcontainers/features/azure-cli:1": {},
    // Node: sólo para exportar los diagramas a SVG (capítulo de diagramas)
    "ghcr.io/devcontainers/features/node:1": { "version": "22" }
  },
```

- `"features"`: piezas que se agregan a la imagen, cada una con su versión.
- **Java 17** (distribución Temurin): Spark está escrito en Scala y corre sobre Java. Sin Java, `pyspark` no arranca.
- **azure-cli**: el comando `az`.
- **Node**: un entorno para programas JavaScript; el proyecto lo usa sólo para exportar los diagramas.

```jsonc
  // Spark local necesita memoria: al menos 2 núcleos y 8 GB (la máquina chica de Codespaces alcanza)
  "hostRequirements": { "cpus": 2, "memory": "8gb" },
```

- `"hostRequirements"`: el tamaño mínimo de la máquina. Los tests con Spark generan un banco chico (1.000 clientes) y
  necesitan unos 4 GB; con 8 GB sobra.

```jsonc
  // se corre UNA vez, cuando se crea el codespace
  "postCreateCommand": "bash .devcontainer/post_create.sh",
```

- `"postCreateCommand"`: un comando que se ejecuta al terminar de crear la máquina. Lo que no viene como *feature*
  (la CLI de Databricks y el paquete del proyecto) se instala en ese script.

```jsonc
  "customizations": {
    "vscode": {
      "extensions": ["ms-python.python", "charliermarsh.ruff", "databricks.databricks", "redhat.vscode-yaml"]
    }
  }
}
```

- Extensiones del editor: Python (colores y autocompletado), Ruff (marca errores de estilo), Databricks y YAML (los
  archivos de jobs son YAML).

Ahora el script. `code .devcontainer/post_create.sh`:

```bash
#!/usr/bin/env bash
# Se corre una sola vez al crear el codespace: instala la CLI de Databricks y el paquete del proyecto.
set -euo pipefail
```

- `#!/usr/bin/env bash`: la primera línea de un script dice con qué programa se ejecuta (*shebang*).
- `set -euo pipefail`: **frená ante el primer error** (`-e`), **fallá si uso una variable que no existe** (`-u`) y **si
  falla cualquier parte de un `|`** (`pipefail`). Sin esto, un script sigue de largo después de un error y el problema
  aparece mucho después, lejos de su causa.

```bash
# CLI de Databricks, versión fija (la 0.270 fallaba al desplegar: bitácora, capítulo 20)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.290.0/install.sh | sudo sh
```

- `curl` baja el instalador oficial de la CLI; `-fsSL` = fallar si hay error HTTP, sin barra de progreso, siguiendo
  redirecciones. `| sudo sh` le pasa ese instalador a la terminal para que lo ejecute como administrador (`sudo`),
  porque instala en `/usr/local/bin`.
- **Por qué una versión fija (`v0.290.0`):** con la "última" cada uno tendría una distinta. Pasó de verdad: con la 0.270
  el despliegue fallaba porque una clave interna de Terraform había vencido.

```bash
# el paquete del proyecto en modo editable, con lo necesario para los tests, Azure y Genie
if [ -f pyproject.toml ]; then
  pip install --upgrade pip
  pip install -e ".[dev,azure,genie]"
fi
```

- `if [ -f pyproject.toml ]`: sólo si existe el archivo `pyproject.toml` (lo creás en el capítulo 04). Hoy no existe,
  así que este bloque no hace nada todavía.
- `pip install -e ".[dev,azure,genie]"`: instala el proyecto en modo **editable** (`-e`): Python usa tus archivos tal
  como están, sin copiarlos, así cada cambio que hacés se ve enseguida. Entre corchetes, los grupos de dependencias
  extra (capítulo 04).

```bash
echo "Listo: $(python --version) · $(java -version 2>&1 | head -1) · $(az version --query '\"azure-cli\"' -o tsv) · $(databricks --version)"
```

- Imprime las versiones instaladas: si alguna falta, se ve acá. `2>&1` junta la salida de errores con la normal
  (`java -version` escribe en la de errores).

Hacé el script ejecutable, guardá todo en Git y reconstruí el codespace:

```bash
chmod +x .devcontainer/post_create.sh
git add .devcontainer
git commit -m "Codespaces: Python 3.12, Java 17, az y databricks"
git push
```

- `chmod +x`: le da permiso de ejecución al script.
- Después: **F1** (o Ctrl+Shift+P) → escribí `Rebuild Container` → **Codespaces: Rebuild Container**. La máquina se
  vuelve a crear leyendo tu receta (unos minutos). Al final, en la terminal de creación tiene que aparecer la línea
  `Listo: Python 3.12.x · openjdk version "17..." · 2.x · Databricks CLI v0.290.0`.

### 6. Comprobá que todo te reconoce

```bash
python --version                         # Python 3.12.x
java -version                            # openjdk version "17..."
databricks current-user me --output json | grep userName     # tu mail de Databricks
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" -o none
az account show --query name -o tsv      # el nombre de tu suscripción
```

Si `databricks` responde con tu mail, la CLI encontró `DATABRICKS_HOST` y `DATABRICKS_TOKEN`. Si `az account show`
muestra tu suscripción, el service principal funciona.

## 🤔 Por qué así

- **¿Por qué Codespaces y no instalar todo en mi computadora?** Porque es igual para todos (mismo Linux, mismas
  versiones), no ensucia tu máquina, se rehace en minutos y se parece a cómo se trabaja en una empresa. La contra: tiene
  una cuota gratis por mes (alcanza si lo apagás).
- **¿Por qué Azure y no todo dentro de Databricks?** Porque un banco real no te deja sus datos adentro de tu
  plataforma: los deja en un storage, los sirve por una API, los tiene en otro sistema. Tenerlos afuera obliga a
  resolver la ingesta de verdad: credenciales, red, reintentos.
- **¿Por qué un service principal y no tu usuario?** Porque los scripts lo usan sin interacción (no hay navegador en un
  codespace ni en la CI) y porque se puede borrar o rotar sin tocar tu cuenta.

## 🧪 Probalo

| qué | comando | qué tenés que ver |
|---|---|---|
| Codespace | `echo $CODESPACES` | `true` |
| Secretos | `echo ${DATABRICKS_HOST:0:20}` | el principio de tu URL (nunca imprimas el token entero) |
| Databricks | `databricks current-user me --output json \| grep userName` | tu mail |
| Azure | `az account show --query name -o tsv` | tu suscripción |

## 🔥 Si falla

| síntoma | por qué | qué hacer |
|---|---|---|
| `databricks: command not found` | el codespace no se reconstruyó con tu receta | F1 → *Codespaces: Rebuild Container* y mirá el log de creación |
| `databricks` responde `401` | token vencido, mal copiado o de otro workspace | creá otro token y actualizá el secreto; reiniciá el codespace |
| `Error: default auth: cannot configure default credentials` | las variables no están en la terminal | creaste los secretos después de abrir el codespace: reinicialo |
| `az ad sp create-for-rbac` falla por permisos | tu usuario no es dueño (*Owner*) de la suscripción | entrá con el usuario dueño, o pedile a quien lo sea |
| más adelante, Databricks no llega a internet | la cuenta de Free Edition no está verificada | verificala y repetí el capítulo 03 |

## ✅ Punto de control

Tu repo `andes-bank` tiene `README.md` y `.devcontainer/` (dos archivos). Abrí en GitHub tu
`.devcontainer/devcontainer.json` y el de `andes-bank-referencia` uno al lado del otro: tienen que ser iguales (en el
capítulo 04 vas a aprender a compararlos con `git diff`). Las cuatro comprobaciones de "Probalo" responden bien.

## 🏋️ Ejercicio

Agregá a `post_create.sh` una línea que instale `jq` (un programa para leer JSON en la terminal) con
`sudo apt-get install -y jq`, reconstruí el codespace y probalo con
`databricks current-user me --output json | jq -r .userName`.
