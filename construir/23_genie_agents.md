# 23 · Etapa 19 · Genie Agents como código (los 4, con benchmarks)

## 🎯 Objetivo

Cuatro Genie Agents, uno por área, **versionados como código**, desplegados con un script y **medidos**: cada uno
tiene que acertar al menos el **80 %** de sus benchmarks.

| agent | área | sobre qué |
|---|---|---|
| Andes · Comercial y clientes | Comercial | clientes, productos, CRM |
| Andes · Tarjetas y fraude | Tarjetas | autorizaciones, rechazos, fraude |
| Andes · Finanzas y tesorería | Finanzas | depósitos, posición en dólares |
| Andes · Plataforma de datos | Datos | APIs, archivos, calidad |

**Archivos:** `genie/src/*.yml`, `scripts/{build_genie,deploy_genie,validate_genie_sql,genie_ask,genie_benchmark}.py`,
`genie/benchmarks_dev.json`.

![Capa semántica y Genie](../docs/diagramas/05_semantica_genie.svg)

## 📚 Conceptos

- **Un agent = tablas + instrucciones + ejemplos + funciones + benchmarks.** Todo en un YAML legible
  (`genie/src/01_comercial_clientes.yml`); `build_genie.py` lo convierte al JSON que entiende la API.
- **Benchmark:** una pregunta como la haría el usuario + el SQL con la respuesta oficial.
- **Medir dos cosas distintas:**
  1. que el SQL de cada benchmark **corre** (`validate_genie_sql.py`);
  2. que Genie **acierta** cuando le preguntás (`genie_benchmark.py`).

## 🧱 Paso a paso

### 1. El YAML de un agent

```yaml
title: "Andes · Comercial y clientes"
tables:
  - gold.mart_cliente_360
  - gold.dim_cuenta_crm
instructions: |
  - "Clientes" = filas de gold.mart_cliente_360 (un cliente vigente por fila). "Personas" = COUNT(DISTINCT persona_id) ...
  - CRM comercial (desde jun-2024): oportunidades en gold.fact_oportunidad ...
benchmarks:
  - q: ¿Cuántas oportunidades abiertas hay en el CRM?
    sql: SELECT count(*) FROM ${catalog}.gold.fact_oportunidad WHERE NOT cerrada
```

### 2. Construir, validar, desplegar

```bash
python scripts/build_genie.py                         # genie/src/*.yml → genie/agents/*.json
python scripts/validate_genie_sql.py                  # todo el SQL corre en el warehouse
python scripts/deploy_genie.py --target dev           # crea o actualiza (PATCH) cada space
```

### 3. Medir cuánto acierta (`genie_benchmark.py`)

Le hace cada benchmark al agent por la Conversation API y compara con el resultado del SQL esperado:

```python
def matches(expected_rows, genie_rows) -> bool:
    got = [v for row in genie_rows for v in row if v is not None]
    want = [v for row in expected_rows[:20] for v in row if v is not None]
    return all(any(_same(w, g) for g in got) for w in want)    # números con 0,5 % de tolerancia
```

Genie puede devolver columnas de más (el nombre al lado del número): no lo penaliza. Si devuelve otro número, sí.

```bash
python scripts/genie_benchmark.py                      # los 4 agents; detalle en genie/benchmarks_dev.json
python scripts/genie_benchmark.py --only 01_comercial_clientes
```

### 4. Si un agent no llega al 80 %

Se mira **la base de la pirámide primero**: ¿el dato está bien?, ¿el grano?, ¿el comentario?, ¿hay una métrica? Recién
después se agrega una instrucción o un ejemplo.

## 🤔 Por qué así

- **¿Por qué no configurar Genie en la interfaz?** Porque un agent configurado a mano no se revisa, no se versiona y
  se pierde al borrar el workspace. Acá se reconstruye con un comando.
- **¿Por qué medir con la API y no "a ojo"?** Porque "anda bien" no es un criterio. 80 % de los benchmarks sí.

## 🧪 Probalo

```bash
python scripts/genie_ask.py 01_comercial_clientes "¿Cuánto pipeline abierto hay por producto en el CRM?"
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Genie rechaza todas las preguntas de un agent | función certificada con parámetros `DATE` | `STRING 'yyyy-MM-dd'` (R10) |
| Un benchmark "falla" siempre | su SQL no corre | `validate_genie_sql.py` antes de desplegar |
| Genie mezcla "clientes" y "personas" | no está claro cuál es cuál | instrucción + las dos métricas en la metric view |

## ✅ Punto de control

`git diff etapa-19 -- genie/src/` y `genie/benchmarks_dev.json` con los 4 agents en 80 % o más.

## 🏋️ Ejercicio

Escribí 5 benchmarks nuevos para el agent de Finanzas con preguntas que **vos** le harías a tesorería, medilos y
arreglá lo que falle empezando por la base de la pirámide.
