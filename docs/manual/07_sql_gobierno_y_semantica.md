# 07 · SQL: gobierno y capa semántica

> Carpeta `sql/`. Todo lo que se define mejor en SQL que en Python: el **gobierno** de Unity Catalog (comentarios,
> claves, tags, máscaras, filtros de fila) y la **capa semántica** (vistas, metric views y funciones certificadas)
> que usa Genie. Lo corre `sql_runner.run_folder` desde la tarea `governance_semantic`.

![Capa semántica y Genie](../diagramas/05_semantica_genie.svg)

## 1. Por qué se re-aplica en cada corrida

Gold se **reescribe** en cada corrida (`overwrite` con `overwriteSchema`): se pierden los comentarios de columna, las
claves, las máscaras y los filtros. Por eso la tarea `governance_semantic` corre **después de gold** y re-aplica todo,
y por eso todo es **idempotente** (`CREATE OR REPLACE`, `DROP … IF EXISTS`, `MERGE`).

Variables que reemplaza el runner: `${catalog}`, `${bronze}`, `${silver}`, `${gold}`, `${semantic}`, `${ops}`.

## 2. `sql/governance/`

### `01_comments.sql`: comentarios

Cada tabla y columna importante de gold explicada **en el idioma del negocio**. Es lo primero que lee Genie para
entender qué significa un dato: *un comentario vale más que diez instrucciones en el agent*. Ejemplos de lo que
explican: que `dim_cliente` tiene una fila por versión y hay que filtrar `is_current`; que `persona_id` sirve para contar
personas; qué significan `CUY-`, `NOA-` y `AB-`; que `tipo_persona = JURIDICA` es una empresa (este último se agregó por
R18, porque Genie no sabía que "empresa" era eso).

### `02_constraints.sql`: claves primarias y foráneas

Unity Catalog **no las hace cumplir**, pero son **informativas**: le dicen a Genie y a las herramientas de BI cómo se
unen las tablas. PK en las dimensiones y en `mart_cliente_360`; FK de los hechos a sus dimensiones.

**Bitácora R12:** la segunda corrida fallaba con `Cannot delete constraint pk_dim_fecha with non empty child
constraints`: las FK dependen de la PK. Se usa `DROP CONSTRAINT IF EXISTS … CASCADE` antes de crearla.

### `03_tags.sql`: tags

Dominio de cada tabla (`clientes`, `tarjetas`, `tesoreria`, `comercial`, `fraude`) y clasificación de cada columna
personal (`pii = documento | contacto | personal | financiero`). Sirven para buscar ("¿dónde hay PII?") y auditar.

### `04_masks_and_row_filters.sql`: máscaras y filtros de fila

El requisito (inventado, pero realista) es la "auditoría BCRA 2026":

| función | qué hace | quién ve el dato completo |
|---|---|---|
| `ops.puede_ver_pii()` | verdadero si el usuario es admin o pertenece a `andes_cumplimiento` o `andes_comercial` | — |
| `ops.mask_documento` | CUIT: `*****` + últimos 3 | Cumplimiento, Comercial, admins |
| `ops.mask_dni` | DNI numérico: nulo | ídem |
| `ops.mask_email` | `juan.perez@gmail.com` → `j*******@gmail.com` | ídem |
| `ops.mask_texto` | teléfono: `[RESERVADO]` | ídem |
| `ops.mask_fecha` | fecha de nacimiento: sólo el año | ídem |
| `ops.mask_cbu` | últimos 4 dígitos | Operaciones, Cumplimiento, admins |
| `ops.filtro_region(region)` | **row filter**: el usuario ve sólo las regiones asignadas en `ops.region_access` (o todas con `*`) | admins |

`ops.region_access (principal, region, area, granted_at)` se crea si no existe, y un `MERGE` le da `*` a quien corre el
job (si no, el propio job dejaría de ver los datos que escribe). Las máscaras se aplican a `dim_cliente` y `dim_cuenta`;
el filtro, a `dim_cliente` y `mart_cliente_360`.

Los grupos (`andes_cumplimiento`, etc.) no existen en un workspace recién creado: hasta que se creen, sólo los admins ven
la PII completa. Es el comportamiento seguro por defecto.

## 3. `sql/crm/01_governance.sql`

Comentarios, PK/FK, máscara del CUIT y tags de las tablas del CRM en gold. Corre sólo si existe `gold.dim_cuenta_crm`
(el notebook 35 lo chequea). El comentario de `dim_cuenta_crm` es clave para Genie: "para contar empresas usar
`COUNT(DISTINCT cuenta_principal_id)`" y "los prospectos también son empresas cargadas en el CRM".

## 4. `sql/semantic/`

### `01_views.sql`: vistas anchas

Unen un hecho con sus dimensiones **una sola vez**, acá, para que nadie tenga que adivinar los joins:

| vista | qué une |
|---|---|
| `v_movimientos` | movimientos + cliente vigente (segmento, tipo, región, banco de origen) + sucursal + calendario |
| `v_autorizaciones` | autorizaciones + comercio (rubro, exterior, riesgoso) + cliente + calendario |
| `v_saldos_fin_de_mes` | saldos de fin de mes + cliente |
| `v_api_llamadas` | `ops.api_calls` traducido: `api` (tarjetas, crm, espejo BCRA, token), `recurso`, **`resultado`** (OK, RATE_LIMIT, TOKEN_VENCIDO, ERROR_SERVIDOR, TIMEOUT_O_RED, RESPUESTA_INVALIDA, ERROR_CLIENTE) |
| `v_calidad` | `ops.dq_results` con columnas en castellano (`regla`, `severidad`, `problema`, `filas_que_fallan`, `paso`) |
| `v_archivos_recibidos` | `ops.landing_files` con sistema, fuente, fecha de entrega y estado |

La columna `api` de `v_api_llamadas` se agregó por R18: sin ella, "¿cuántos 429 dio la API de tarjetas?" contaba
también los del CRM.

### `02_metric_views.sql`: las métricas oficiales

Una **metric view** define dimensiones y medidas **una vez**, en YAML, y todos (Genie, dashboards, SQL) las usan igual:

```sql
SELECT `Segmento`, MEASURE(`Clientes`) FROM andes_bank.semantic.mv_clientes GROUP BY ALL
```

| metric view | fuente | ejemplos de medidas |
|---|---|---|
| `mv_clientes` | `mart_cliente_360` | Clientes, **Personas** (`COUNT(DISTINCT persona_id)`), Productos por cliente, Saldo total ARS/USD, Consumo con tarjetas, Porcentaje digital |
| `mv_movimientos` | `v_movimientos` | Movimientos, Créditos (ARS), Débitos (ARS), Flujo neto (ARS/USD), Clientes activos, Movimientos digitales (%) |
| `mv_tarjetas` | `v_autorizaciones` | Autorizaciones, Compras aprobadas, Tasa de aprobación, Rechazos por sospecha de fraude, Consumo aprobado (ARS), Ticket promedio, Tarjetas activas, Pendientes de liquidar |
| `mv_depositos` | `v_saldos_fin_de_mes` | Depósitos (ARS), Depósitos (USD), Depósitos en moneda original, Cuentas con saldo |
| `mv_plataforma_apis` | `v_api_llamadas` | Pedidos, Pedidos fallidos, Tasa de error, Rate limits (429), Latencia promedio y p95 (ms), Filas recibidas |

Los nombres son **en castellano de negocio** ("Cobra el sueldo en el banco") y llevan **sinónimos** ("plan sueldo",
"acredita haberes"): Genie los usa para entender cómo pregunta la gente. El YAML va entre `$$ … $$`, por eso el runner
no corta las sentencias dentro de esos bloques.

### `03_functions.sql`: funciones certificadas

Preguntas importantes con **una sola respuesta oficial**. Genie las usa en vez de inventar el SQL, así el resultado es
siempre el mismo:

| función | responde |
|---|---|
| `fn_resumen_cliente(id_cliente)` | la ficha de un cliente: productos, saldos, consumo, actividad |
| `fn_alertas_fraude(desde, hasta)` | tarjetas con 3 o más compras en un día en rubros riesgosos o en el exterior |
| `fn_posicion_dolares(al_dia)` | depósitos en USD al último fin de mes anterior o igual a la fecha, valuados al oficial |
| `fn_salud_ingesta(dias)` | pedidos y fallas por tipo de cada API en los últimos N días, con la latencia p95 |

Dos lecciones escritas en el archivo:

- **Fechas como `STRING 'yyyy-MM-dd'`, nunca `DATE`** (bitácora R10, heredada de Andes Viento Austral): con `DATE`,
  Genie acepta el deploy pero después rechaza **todas** las preguntas del agent. `deploy_genie.py` lo verifica antes de
  desplegar.
- **Bitácora R13:** en `fn_posicion_dolares`, el parámetro no se resolvía dentro de una subconsulta escalar del
  `WHERE` (`UNRESOLVED_COLUMN al_dia`). Se resolvió con una tabla derivada y `dense_rank() OVER (ORDER BY fecha DESC)`.
