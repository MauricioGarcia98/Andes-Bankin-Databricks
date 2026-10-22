# 11 · Scripts, docs, diagramas y asistentes

> Lo que no es pipeline pero hace que el proyecto se entienda, se reconstruya y se cuente: los diagramas como código,
> los tags de las etapas, la documentación, el curso `construir/` y las reglas para los asistentes de IA.

## 1. Diagramas como código (`scripts/diagramas/`)

**Por qué como código:** un diagrama dibujado a mano queda viejo en cuanto cambia la arquitectura. Acá cada diagrama es
una función de Python; si cambia algo se edita la función y se regenera. El resultado es un `.excalidraw` normal: se
puede abrir y retocar en excalidraw.com.

| archivo | qué hace |
|---|---|
| `excali.py` | un **mini-DSL** sin dependencias: `Diagram` con `heading`, `zone` (área de color con título e ícono), `box` (caja con texto, color, forma e ícono), `arrow` (flecha entre cajas con lado de salida/llegada, puntos intermedios y etiqueta), `note` (nota con altura automática según el texto), `text` e `icon`. Paleta fija (rellenos pastel y bordes), tipografía Helvetica y líneas limpias (nítidas en pantalla), grilla de 20 px. **Tamaño de letra:** GitHub muestra un lienzo de ~2.100 px en ~830 px, así que todo el texto se multiplica por `TEXT_SCALE` (1,5) y se mide con la tabla de anchos real de Helvetica; si un texto no entra en su caja, se achica lo justo (nunca por debajo del tamaño original). Así queda entre 11 y 16 px en pantalla. `save()` escribe un `.excalidraw` válido con ids determinísticos |
| `generar.py` | una función por diagrama (`panorama`, `fuentes`, `ingesta`, `silver`, `gold`, `genie`, `despliegue`, `api`, `linea_de_tiempo`, `generador`, `landing`, `incremental`, `dia_incompleto`, `scd2`, `estrella`, `piramide_genie`, `operacion`, `en_numeros`, `iconos`). `en_numeros` **lee `genie/benchmarks_dev.json`**: los porcentajes del diagrama son los de la última medición |
| `bajar_iconos.py` | baja de la biblioteca pública de Excalidraw (licencia MIT) los íconos que usan los diagramas y los guarda en `iconos.json` (sólo el dibujo, con la biblioteca de origen). Filtra marcos invisibles y calcula bien la caja de las líneas (si no, los íconos salían diminutos) |
| `iconos.json` | los íconos (Azure, Databricks, bases de datos, personas, candados…) |
| `exportar.mjs` + `package.json` | exporta cada `.excalidraw` a **SVG** (y a PNG con `--png`) usando la **misma librería del editor** (`@excalidraw/excalidraw` 0.17.6 con React) dentro de Chromium (Playwright). Incrusta la tipografía en base64: GitHub muestra los SVG como imagen y no deja cargar fuentes externas |

```bash
python scripts/diagramas/bajar_iconos.py        # sólo si cambian los íconos
python scripts/diagramas/generar.py             # → docs/diagramas/*.excalidraw
cd scripts/diagramas && npm install && node exportar.mjs   # → *.svg
```

**Lo que costó:** en la primera versión el texto se veía diminuto en GitHub (letra de 18 px en un lienzo de 2.100 px, unos 7 px en pantalla) y la letra "a mano" se emborronaba a ese tamaño: se pasó a Helvetica, texto ×1,5 con ajuste automático a la caja y trazos sin rugosidad. Antes de eso, la exportación usaba `@excalidraw/utils` 0.1.2 y el texto de varias líneas se
superponía; se cambió a la librería del editor. Los PNG no se versionan (`.gitignore`): el SVG es el que se muestra.

Los 19 diagramas están indexados en [docs/diagramas/README.md](../diagramas/README.md), con los créditos de los íconos.

## 2. `scripts/etapas.sh`: los tags del curso

Cada capítulo de `construir/` tiene un **punto de control**: un tag `etapa-NN` que apunta al estado del código al
terminar esa etapa. Así, quien rehace el proyecto compara su código con la referencia:

```bash
git diff etapa-10 -- src/andes_bank/ingestion/api_client.py
```

Cómo funciona: un diccionario etapa → archivos de esa etapa; para cada una busca **el último commit de main que tocó
esos archivos** (así el tag incluye los arreglos que vinieron después, como R14 o el CRM). Sin argumentos muestra qué
commit le toca a cada etapa; con `--push` crea los tags anotados y los sube. Se corre desde una máquina con permiso de
push de tags (`bash scripts/etapas.sh --push`).

## 3. `scripts/azure/` y los scripts de Genie

Ver [03 · Azure](03_azure.md) y [08 · Genie](08_genie.md).

## 4. `docs/`: la documentación

| documento | para qué sirve |
|---|---|
| [00 · Definición del proyecto](../00_definicion_del_proyecto.md) | el "contrato": qué es, por qué, alcance, decisiones, cómo se trabaja con la IA, **criterios de terminado del MVP** |
| [01 · La empresa](../01_la_empresa.md) | la historia del banco (1998 en Mendoza, compra del banco del NOA en 2008, Centro en 2016), áreas, personas, productos |
| [02 · Fuentes del MVP](../02_fuentes_mvp.md) | core, tarjetas, CRM y FX: formato, frecuencia y volumen |
| [03 · Catálogo de problemas](../03_catalogo_de_problemas.md) | los 37 problemas (12 de APIs, 18 de datos, 7 de ingesta): cómo aparecen, dónde se resuelven, si entran en el perfil `normal` |
| [04 · Arquitectura](../04_arquitectura.md) | Azure + Databricks, *pull* vs. *push*, Unity Catalog, costos |
| [05 · Genie](../05_genie.md) | los agents por área, Genie One, Genie Code y para qué serviría una App |
| [06 · Plan de construcción](../06_plan_de_construccion.md) | las etapas de `construir/` |
| [07 · Posteo de LinkedIn](../07_posteo_linkedin.md) | el posteo con los números reales y las 20 preguntas guía |
| [08 · Bitácora](../08_bitacora.md) | los 18 errores reales (R01–R18) con síntoma, causa y arreglo |
| [operacion.md](../operacion.md) | el runbook: qué mirar y qué hacer cuando falla cada tarea del job, y la prueba de volumen a SF 1 |
| [adr/0001](../adr/0001_free_edition_acceso_saliente.md) + `0001_spike_outbound.py` | el spike que comprobó que Free Edition verificado puede salir a internet (base del patrón *pull*) |
| [adr/0002](../adr/0002_landing_sync_blob_a_volume.md) | por qué Blob → Volume y no *external location* |
| `diagramas/` | los diagramas |
| `manual/` | este manual |

**ADR** (*Architecture Decision Record*): un documento corto por decisión importante, con el contexto, las opciones y
por qué se eligió una. Sirve para que dentro de seis meses nadie re-discuta lo mismo sin saber por qué se decidió.

## 5. `construir/`: el curso

28 capítulos para reconstruir todo **sin IA**, desde borrar todo (`01_borrar_todo.md`) hasta el examen final. Cada
capítulo tiene la misma forma: 🎯 objetivo, 📚 conceptos, 🧱 paso a paso, 🤔 por qué así, 🔥 si falla (sacado de la
bitácora), ✅ punto de control (el tag `etapa-NN`) y 🏋️ ejercicio. El índice está en
[construir/README.md](../../construir/README.md).

Diferencia con este manual: el curso es **secuencial** (cómo se construye, en orden); el manual es **de consulta**
(cómo es cada pieza terminada y por qué).

## 6. Asistentes de IA: `.assistant_instructions.md` y `.assistant/skills/`

**Genie Code** (el asistente de código dentro de Databricks) lee `.assistant_instructions.md` en cada pedido. Ahí están
las **reglas del proyecto** resumidas: capas del catálogo, que el código va en el paquete y no en notebooks, bronze tal
cual con linaje, una función por problema en silver, escrituras idempotentes (y nunca `saveAsTable` + `replaceWhere`,
R09), nombres en castellano, montos en tres monedas, horas UTC/local, secretos sólo por el scope, usar `ApiClient` y
fechas como STRING en funciones para Genie (R10).

Las **skills** son recetas para tareas que se repiten:

| skill | receta |
|---|---|
| `agregar-fuente` | los 8 pasos para sumar una fuente: generador → subida → bronze → silver → gold → calidad → tests → docs |
| `agregar-problema` | cómo inyectar un problema nuevo y cerrarlo: ID en el catálogo, inyección, arreglo en silver, regla, test |
| `benchmark-genie` | cómo escribir un benchmark útil y qué arreglar primero si Genie falla (la pirámide) |

Así, un asistente que trabaja en el repo sigue las mismas convenciones que una persona.
