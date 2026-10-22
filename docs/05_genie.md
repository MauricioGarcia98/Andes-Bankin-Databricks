# 05 · Genie en el centro

## La idea

Cada persona del banco, de cualquier área, le pregunta a los datos **en castellano** y recibe una respuesta correcta,
con el SQL a la vista y respetando sus permisos. Eso incluye a las áreas de negocio **y** al equipo de datos.

## Qué pieza de Genie sirve para qué

| pieza | quién la usa | para qué | en este proyecto |
|---|---|---|---|
| **Genie Agents** (ex Genie spaces) | cada área | un experto por dominio, con sus tablas, instrucciones, ejemplos de SQL y benchmarks | **el corazón**: 4 agents en el MVP |
| **Genie One** | cualquiera, desde la home | un chat general que usa los agents y los datos a los que la persona tiene acceso | puerta de entrada "no sé a quién preguntar" |
| **Agent mode** | analistas y gerentes | preguntas de "¿por qué?" que necesitan varios pasos | casos como *¿por qué subieron los rechazos en julio?* |
| **Genie Code** (ex Databricks Assistant) | data engineers y analistas **mientras construyen** | escribir y arreglar código, SQL y pipelines dentro del workspace, siguiendo las reglas del proyecto | instrucciones y skills del proyecto versionadas en el repo |

**Una aclaración importante:** Genie Code no es el asistente que contesta preguntas de negocio. Es el que te ayuda a
**construir** (como Claude Code, pero dentro de Databricks). El asistente que responde a cada área en lenguaje
natural es **Genie Agents + Genie One**. Para que también le responda al equipo de datos, armamos un agent específico,
**Plataforma de datos**, sobre las tablas `ops.*`.

## Los Genie Agents del MVP

| agent | área | tablas principales | ejemplos de preguntas |
|---|---|---|---|
| **Andes · Comercial y clientes** | Comercial | `dim_cliente`, `dim_cuenta`, `fact_oportunidad`, `fact_lead` (CRM + core) | *¿Qué sucursal convierte más leads de tarjeta?* · *¿Cuántos clientes de Cuyo tienen sólo caja de ahorro?* |
| **Andes · Tarjetas y fraude** | Prevención de fraude | `fact_autorizacion`, `dim_comercio`, `dim_tarjeta` | *¿Qué comercios tuvieron más rechazos por sospecha la semana pasada?* · *Compras de más de 5 veces el promedio del cliente* |
| **Andes · Finanzas y tesorería** | Tesorería | `fact_saldo_diario`, `fact_movimiento`, `dim_date` (con FX) | *¿Cuántos depósitos en USD tenemos, valuados al oficial de hoy?* · *¿Cómo afectó la devaluación de dic-2023 a los saldos?* |
| **Andes · Plataforma de datos** | Data Engineering / Analytics | `ops.ingestion_runs`, `ops.api_calls`, `ops.quality_results`, `ops.conciliaciones` | *¿Qué ingestas fallaron ayer y por qué?* · *¿Cuántos 429 dio la API de tarjetas?* · *¿Cuántas filas fueron a cuarentena por CUIT inválido?* |

Post-MVP: **Cumplimiento y AML** y **Riesgo crediticio**.

Cada agent:
- se define como código en `genie/src/*.yml` y se despliega con un script (como en Viento Austral);
- tiene **benchmarks** (preguntas con la respuesta esperada) y una evaluación automática;
- se apoya en **metric views y funciones certificadas**, no en instrucciones largas.

## La App: ¿para qué sirve?

**Para usar Genie no hace falta la App.** Genie ya se usa desde el workspace (menú Genie o Genie One).

La App (una Databricks App con Streamlit que habla con la Conversation API de Genie) sirve para tres cosas, y por
eso propongo dejarla como **extra después del MVP**:

1. **Aprender a meter Genie en una herramienta propia**, que es lo que piden las empresas: Genie dentro de la intranet
   o de un bot de Teams/Slack.
2. **Una sola puerta para todas las áreas**: la App elige el agent según el área de la persona (o según la
   pregunta) y la persona no tiene que saber cuál usar.
3. **Mostrar el estado de la plataforma** al lado del chat: últimas corridas, errores de API y calidad.

Si Genie One ya cubre bien la puerta única cuando lleguemos ahí, la App se reduce al punto 1 y se dice así en el
posteo.

## Genie Code en el proyecto

Para que Genie Code ayude a construir con las reglas del proyecto:
- `.assistant_instructions.md`: las convenciones (catálogo `andes_bank`, nombres en castellano, cómo se escribe un
  cliente de API, que nunca se hardcodean secretos).
- `.assistant/skills/`: skills como "agregar una fuente nueva", "agregar un problema al catálogo" y "crear un
  benchmark de Genie".
