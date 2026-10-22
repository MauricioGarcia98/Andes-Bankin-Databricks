# 22 · Etapa 18 · Capa semántica: metric views y funciones certificadas

## 🎯 Objetivo

Definir **una sola vez** las métricas oficiales del banco y la lógica que no se puede equivocar, para que Genie, los
dashboards y el SQL den el mismo número:

- **vistas** `v_*` para consultar cómodo (movimientos, autorizaciones, APIs, calidad, archivos);
- **metric views** `mv_*` con dimensiones y medidas en castellano, con sinónimos;
- **funciones certificadas** `fn_*` (alertas de fraude, posición en dólares, resumen de cliente, salud de ingesta).

**Archivos:** `sql/semantic/0[1-3]_*.sql`.

![Qué hace que Genie responda bien](../docs/diagramas/15_piramide_genie.svg)

## 📚 Conceptos

- **Metric view:** una vista con dimensiones y medidas declaradas en YAML. Se consulta con `MEASURE()`:

  ```sql
  SELECT `Segmento`, MEASURE(`Clientes`) FROM andes_bank.semantic.mv_clientes GROUP BY ALL
  ```

- **Sinónimos:** "segmento comercial", "tipo de cliente" → `Segmento`. Genie entiende cómo habla la gente.
- **Función certificada:** una función SQL con parámetros (`fn_posicion_dolares('2026-06-30')`). Genie la llama en vez
  de improvisar un SQL de 40 líneas.

## 🧱 Paso a paso

### 1. Una metric view

```sql
CREATE OR REPLACE VIEW ${semantic}.mv_clientes
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
source: ${gold}.mart_cliente_360
dimensions:
  - name: Segmento
    expr: segmento
    synonyms: [segmento comercial, tipo de cliente]
measures:
  - name: Clientes
    expr: COUNT(1)                                  # una fila por cliente vigente
    synonyms: [cantidad de clientes, número de clientes]
  - name: Personas
    expr: COUNT(DISTINCT persona_id)                # la fusión de 2008: dos clientes, una persona
$$;
```

### 2. Una función certificada

Los parámetros de fecha van como `STRING 'yyyy-MM-dd'`, no como `DATE` (R10: con `DATE`, Genie rechaza las preguntas
del agent).

### 3. La vista de llamadas a APIs

`v_api_llamadas` clasifica cada pedido en un `recurso` (autorizaciones, liquidaciones, cotizaciones, crm, token) y un
`resultado` (OK, RATE_LIMIT, TOKEN_VENCIDO, ERROR_SERVIDOR...). El agent de Plataforma pregunta sobre esto.

## 🤔 Por qué así

- **¿Por qué no dejar que Genie calcule las métricas?** Porque "clientes" y "personas" no son lo mismo (la fusión
  de 2008), y la diferencia son miles. La metric view define las dos, con nombre y sinónimos, una sola vez.
- **¿Por qué funciones y no más ejemplos SQL?** Un ejemplo se puede copiar mal; una función se llama igual siempre.

## 🧪 Probalo

```sql
SELECT `Región`, MEASURE(`Clientes`) FROM andes_bank.semantic.mv_clientes GROUP BY ALL;
SELECT * FROM andes_bank.semantic.fn_salud_ingesta(7);
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Genie rechaza TODAS las preguntas de un agent | una función certificada con parámetros `DATE` | parámetros `STRING 'yyyy-MM-dd'` (R10) |
| `UNRESOLVED_COLUMN al_dia` en `fn_posicion_dolares` | parámetro dentro de una subconsulta escalar | `dense_rank() OVER (ORDER BY fecha DESC)` (R13) |
| `PARSE_SYNTAX_ERROR` en una metric view | el `;` del YAML cortó la sentencia | `sql_runner` respeta los bloques `$$ ... $$` |

## ✅ Punto de control

`git diff etapa-18 -- sql/semantic/`

## 🏋️ Ejercicio

Agregá `mv_crm` (pipeline abierto, ganado, casos abiertos por producto y región) y usala en un benchmark nuevo del
agent Comercial.
