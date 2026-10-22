# 24 · Etapa 21 · Genie Code: instrucciones y skills

## 🎯 Objetivo

Que el asistente de código de Databricks (**Genie Code**) trabaje como un integrante más del equipo: que conozca las
reglas del proyecto y sepa hacer las tareas repetidas (agregar una fuente, agregar un problema al catálogo, escribir
benchmarks) igual que las hiciste vos.

**Archivos:** `.assistant_instructions.md`, `.assistant/skills/{agregar-fuente,agregar-problema,benchmark-genie}/SKILL.md`.

## 📚 Conceptos

- **Instrucciones del asistente:** un archivo con las reglas del proyecto que el asistente lee siempre: capas,
  nombres en castellano, montos en `decimal(18,2)`, horas `*_utc` y `*_local`, secretos sólo por `dbutils.secrets`.
- **Skill:** una receta para una tarea concreta, con sus pasos y sus trampas. El asistente la usa cuando la tarea
  coincide con la descripción.

## 🧱 Paso a paso

### 1. Las reglas (`.assistant_instructions.md`)

```markdown
- **El código vive en el paquete `andes_bank`** (`src/`), testeado con pytest. Los notebooks son delgados.
- **Bronze** guarda lo que llegó tal cual (texto o JSON crudo) con linaje. Nunca limpiar en bronze.
- **Silver**: una función por problema del catálogo, con su ID en un comentario (DAT-02, API-06...).
- **Escrituras idempotentes**: MERGE insert-only para hechos, `utils.delta.replace_where` para reemplazar un rango.
- **Funciones para Genie**: parámetros de fecha como STRING 'yyyy-MM-dd' (con DATE, Genie rechaza el agent).
```

Cada regla salió de algo que pasó (la bitácora): por eso el asistente no repite los errores.

### 2. Las skills

- `agregar-fuente`: contrato de bronze, linaje, checkpoint, tabla silver, regla de calidad, test.
- `agregar-problema`: ID nuevo en `docs/03`, cómo se inyecta en el generador, cómo se arregla en silver, qué regla lo
  mide.
- `benchmark-genie`: cómo escribir un benchmark determinístico (sin `current_date()`) y qué arreglar primero si falla.

## 🤔 Por qué así

- **¿Por qué escribir reglas para una IA?** Por lo mismo que se escribe una guía para alguien nuevo: el contexto que
  no está en el código se pierde. La diferencia es que la IA la lee **siempre**.
- **¿Por qué skills chicas?** Una skill de 20 líneas se sigue; un manual de 20 páginas, no.

## 🧪 Probalo

En el workspace, abrí Genie Code en un notebook y pedile: *"agregá una fuente nueva: un CSV diario de cajeros
automáticos"*. Tiene que proponer el contrato de bronze, el linaje y la regla de calidad, sin limpiar en bronze.

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| El asistente limpia datos en bronze | no leyó las instrucciones | el archivo tiene que estar en la raíz del repo del workspace |
| Propone una UDF de Python | la regla no estaba escrita | agregala a las instrucciones con el **porqué** |

## ✅ Punto de control

`git diff etapa-21 -- .assistant_instructions.md .assistant/`

## 🏋️ Ejercicio

Escribí la skill `agregar-regla-de-calidad` y probala pidiéndole al asistente una regla para DAT-07 sobre CBU.
