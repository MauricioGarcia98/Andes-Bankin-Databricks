# 00 · Antes de empezar: las palabras y las herramientas

> **Para quién es este capítulo:** para alguien que nunca abrió una terminal, no sabe qué es Git ni qué hace un
> programa en Python. Si ya lo sabés, leé sólo la sección 6 (cómo se lee el curso) y pasá al capítulo 01.

## 🎯 Objetivo

Que al terminar sepas **qué es cada cosa** que vas a usar durante el curso y **por qué** está en el proyecto. No vas a
instalar nada todavía: eso es el capítulo 02. Acá se arma el mapa mental.

## 1. El proyecto en una frase (y por qué existe)

Vas a construir la **plataforma de datos de un banco argentino que no existe** (Andes Bank). El banco "funciona" solo:
un programa inventa sus clientes, sus cuentas, sus compras con tarjeta y sus movimientos, y los manda como lo haría un
banco real (archivos con errores, una API que falla a propósito). Tu trabajo es **recibir esos datos, limpiarlos,
ordenarlos y dejarlos listos** para que cualquier área del banco les haga preguntas en castellano con **Genie**.

¿Para qué? Para aprender ingeniería de datos con problemas de verdad, que en los tutoriales no aparecen. El detalle de
los objetivos está en el [manual técnico](../docs/manual/README.md#1-los-objetivos-reales-del-proyecto).

## 2. Las palabras básicas

| palabra | qué es | un ejemplo del proyecto |
|---|---|---|
| **archivo** | un conjunto de datos con nombre, guardado en un disco | `config.py`, `README.md` |
| **carpeta** (directorio) | una "caja" que agrupa archivos y otras carpetas | `src/`, `tests/` |
| **ruta** (*path*) | la dirección de un archivo: las carpetas separadas por `/` | `src/andes_bank/config.py` |
| **extensión** | lo que va después del punto en el nombre: dice qué tipo de archivo es | `.py` = Python, `.md` = texto con formato, `.yml` = configuración, `.sql` = consultas |
| **programa** / **código** | instrucciones escritas para que una computadora las ejecute | todo lo que está en `src/` |
| **Python** | el lenguaje de programación del proyecto | `def suma(a, b): return a + b` |
| **SQL** | el lenguaje para consultar tablas de datos | `SELECT count(*) FROM clientes` |
| **terminal** (consola) | una ventana donde le escribís **comandos** a la computadora en vez de hacer clic | `ls`, `cd src`, `pytest` |
| **comando** | una orden de una línea que se escribe en la terminal y se ejecuta con Enter | `mkdir src` crea la carpeta `src` |
| **la nube** | computadoras de otras empresas (Microsoft, GitHub, Databricks) que usás por internet | Azure, Codespaces, Databricks |
| **API** | una "ventanilla" por internet: un programa le pide datos a otro con un pedido HTTP y recibe una respuesta | la API de tarjetas del banco |
| **JSON** | un formato de texto para datos: llaves, nombres y valores | `{"cliente": "CUY-0001234", "saldo": 1500.50}` |
| **CSV** | un archivo de texto con una fila por línea y columnas separadas por coma (o `;`) | `cuenta_id,fecha,importe` |

### Leer una ruta

`src/andes_bank/utils/ids.py` se lee de izquierda a derecha: en la carpeta `src` hay una carpeta `andes_bank`, adentro
una carpeta `utils`, y adentro el archivo `ids.py`. Una ruta que empieza con `/` (por ejemplo `/workspaces/andes-bank`)
es **absoluta**: arranca desde la raíz del disco. Una que no empieza con `/` es **relativa**: arranca desde la carpeta
donde estás parado.

## 3. Las herramientas y por qué cada una

```
   vos (navegador)
        │
        ▼
 ┌──────────────────────────┐     git push      ┌──────────────┐
 │  GitHub Codespaces        │ ─────────────────► │   GitHub     │  el repositorio: la historia del código
 │  (una computadora en la   │                    └──────────────┘
 │   nube con todo instalado)│   az ...           ┌──────────────┐
 │  terminal + editor        │ ─────────────────► │   Azure      │  donde "viven" los sistemas del banco
 │  Python, Java, az,        │   databricks ...   ┌──────────────┐
 │  databricks               │ ─────────────────► │  Databricks  │  donde se procesan los datos y está Genie
 └──────────────────────────┘                    └──────────────┘
```

| herramienta | qué es | para qué la usa el proyecto | qué NO hace |
|---|---|---|---|
| **GitHub** | una página donde se guardan proyectos de código, con toda su historia | guardar el repo, revisar cambios, correr los tests automáticos (CI) | no ejecuta el proyecto ni guarda datos |
| **Git** | el programa que registra los cambios de los archivos ("versiones") | cada paso del curso termina en un *commit*: una foto del código con un mensaje | no sube nada solo: hay que hacer `git push` |
| **GitHub Codespaces** | una computadora Linux en la nube, que se abre en el navegador, con un editor (VS Code) y una terminal | es **tu lugar de trabajo**: no instalás nada en tu computadora; el repo ya está adentro | no es gratis infinito: tiene horas gratis por mes (alcanzan para el curso). Si la dejás prendida sin usar, se apaga sola |
| **Python** | el lenguaje | todo el código de datos | — |
| **Java** | otro lenguaje; lo necesita Spark para funcionar | correr los tests con Spark en el codespace | no se programa nada en Java |
| **Spark** | un motor para procesar muchísimos datos en paralelo | generar el banco (millones de filas) y transformarlo | no guarda datos: los procesa |
| **Delta Lake** | un formato de tablas sobre archivos, con historial y escrituras seguras | todas las tablas del proyecto | — |
| **Azure** | la nube de Microsoft | guardar los archivos del banco (Storage) y la API (Function) | no procesa los datos del lakehouse: eso es Databricks |
| **CLI de Azure** (`az`) | un programa para manejar Azure desde la terminal | crear el Storage y la API con un script, sin hacer clic | — |
| **Databricks** | una plataforma en la nube para datos: Spark, tablas, SQL, jobs, Genie | el lakehouse completo; la edición gratis (*Free Edition*) alcanza | — |
| **CLI de Databricks** (`databricks`) | un programa para manejar Databricks desde la terminal | desplegar los jobs, correrlos, cargar secretos | — |
| **Genie** | el asistente de Databricks que responde preguntas sobre tablas en castellano | el producto final: cada área le pregunta a sus datos | no inventa datos: escribe SQL sobre tus tablas |

### ¿Por qué todo en la nube?

- **No depende de tu computadora.** Da lo mismo Windows, Mac o una notebook vieja: el codespace es siempre el mismo
  Linux, con las mismas versiones. Si algo se rompe, se borra el codespace y se crea otro igual en dos minutos.
- **Es como se trabaja en una empresa.** El código vive en GitHub, se ejecuta en la nube y las credenciales se guardan
  en lugares seguros, no en tu disco.
- **El repo ES la carpeta del proyecto.** Cuando abrís un codespace, la carpeta `/workspaces/<nombre-del-repo>` es el
  repositorio. Todo archivo que creás ahí, después de `git commit` y `git push`, queda guardado en GitHub.

## 4. La terminal: los 10 comandos que vas a usar

En el codespace, la terminal está abajo (si no aparece: menú ☰ → *Terminal* → *New Terminal*). Escribís un comando y
apretás Enter. Lo que empieza con `#` es un comentario: no se escribe, explica.

```bash
pwd                      # "print working directory": en qué carpeta estoy parado
ls                       # lista lo que hay en la carpeta actual
ls -la                   # lo mismo, con archivos ocultos (los que empiezan con punto) y detalles
cd src                   # "change directory": entrar a la carpeta src
cd ..                    # subir una carpeta (.. = la carpeta de arriba)
mkdir -p src/andes_bank  # crear carpetas; -p crea también las intermedias y no falla si ya existen
touch archivo.py         # crear un archivo vacío
cat archivo.py           # mostrar el contenido de un archivo
code archivo.py          # abrir un archivo en el editor del codespace
python archivo.py        # ejecutar un programa de Python
```

Tres trucos:

- **Tab** completa nombres de archivos y carpetas: escribí `cd sr` y apretá Tab.
- **Flecha arriba** trae el comando anterior.
- **Ctrl+C** corta un comando que está tardando o que se colgó.

Cuando el curso muestra un bloque así:

```bash
pytest -q tests/test_ids.py
```

significa: **copialo en la terminal y apretá Enter**. Si el bloque dice `python`, `toml`, `yaml` u otro lenguaje, no es
para la terminal: es el **contenido de un archivo**, y el curso te dice en qué archivo va.

## 5. Git en cinco palabras

| palabra | qué es |
|---|---|
| **repositorio** (repo) | la carpeta del proyecto + toda su historia de cambios |
| **commit** | una "foto" de los archivos en un momento, con un mensaje que dice qué cambió |
| **rama** (*branch*) | una línea de trabajo; la principal se llama `main` |
| **push** | subir tus commits a GitHub |
| **tag** | un nombre fijo para un commit importante (el curso usa `etapa-02`, `etapa-03`…) |

El ciclo de todos los capítulos:

```bash
git status                       # qué archivos cambiaron
git add src/ tests/              # elegir qué cambios van en la foto
git commit -m "Etapa 02: configuración y CUIT/CBU"   # sacar la foto, con un mensaje
git push                         # subirla a GitHub
```

## 6. Cómo se lee este curso

Cada capítulo tiene siempre las mismas partes:

1. **🎯 Objetivo** — qué vas a tener al final y por qué este paso va en este lugar.
2. **📚 Conceptos** — lo que tenés que entender antes de escribir, explicado desde cero.
3. **🧱 Paso a paso** — cada carpeta y cada archivo, **en el orden en que se crean**. Cada archivo aparece **completo**,
   en bloques, y después de cada bloque: **qué hace**, **por qué así** y **qué NO hace**. Al terminar el paso, tu
   archivo es igual al del repo de referencia.
4. **🧪 Probalo** — el comando y lo que tenés que ver.
5. **🔥 Si falla** — los errores que **pasaron de verdad** al construirlo (la [bitácora](../docs/08_bitacora.md)).
6. **✅ Punto de control** — comparás tu código con el de referencia.
7. **🏋️ Ejercicio** — un cambio chico para hacer sin mirar.

**Escribí el código vos**, aunque sea copiándolo de a una línea: tipear obliga a leer. Lo que no entiendas, buscalo en
la explicación de abajo del bloque; si no está, es un error del curso.

## ✅ Punto de control

Podés explicar con tus palabras: qué es una ruta, qué hace `cd ..`, para qué sirve un commit, por qué el proyecto usa
Codespaces, Azure y Databricks, y qué diferencia hay entre un bloque `bash` y un bloque `python` del curso.

## 🏋️ Ejercicio

Dibujá en un papel el diagrama de la sección 3 de memoria, y agregale una flecha más: ¿desde dónde le pregunta un
gerente a Genie?
