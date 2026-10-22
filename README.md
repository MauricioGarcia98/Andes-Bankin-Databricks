# 🏦 Andes Bank · Lakehouse bancario E2E en Databricks Free Edition

> **Estado: MVP terminado (22-oct-2026).** Las 4 fuentes llegan a bronze con el caos activado, el quality gate pasa
> (22 de 22 reglas), los 4 Genie Agents aciertan entre el 80 y el 100 % de sus benchmarks, un job simula a
> pedido días nuevos del banco y corre el lakehouse hasta ahí, y Azure costó USD 0,02 en el mes. Ver [Operación](docs/operacion.md) y
> [el curso para reconstruirlo](construir/README.md).

Andes Bank es un banco argentino **ficticio**, regulado por el BCRA. Este repo construye su plataforma de datos de
punta a punta en **Databricks Free Edition**, con fuentes reales que sirven datos sintéticos (APIs en Azure,
Azure Blob Storage, un CRM comercial servido por API y la API pública del BCRA), **problemas inyectados a propósito** en las APIs, en los
datos y en la ingesta, y **Genie** como la forma en que cada área le pregunta a los datos.

Es el proyecto hermano de [Andes Viento Austral](https://github.com/MauricioGarcia98/andes-viento-austral) y sigue
su misma estructura.

## Empezá por acá

| doc | de qué trata |
|---|---|
| [00 · Definición del proyecto](docs/00_definicion_del_proyecto.md) | qué es, por qué, alcance del MVP, decisiones tomadas |
| [01 · La empresa](docs/01_la_empresa.md) | historia, áreas, personas, sucursales y productos |
| [02 · Fuentes del MVP](docs/02_fuentes_mvp.md) | core, tarjetas, CRM y FX: formato y volumen |
| [03 · Catálogo de problemas](docs/03_catalogo_de_problemas.md) | qué se rompe en las APIs, en los datos y en la ingesta, y dónde se arregla |
| [04 · Arquitectura](docs/04_arquitectura.md) | Azure + Databricks, pull vs. push, Unity Catalog, costos |
| [05 · Genie](docs/05_genie.md) | Genie Agents por área, Genie One, Genie Code y para qué sirve la App |
| [06 · Plan de construcción](docs/06_plan_de_construccion.md) | las etapas de `construir/` |
| [07 · Posteo de LinkedIn](docs/07_posteo_linkedin.md) | las preguntas para contar el proyecto |
| [Diagramas](docs/diagramas/README.md) | el proyecto en Excalidraw, fase por fase |
| [Operación](docs/operacion.md) | qué hacer cuando falla un job, y la prueba de volumen a SF 1 |
| [Manual técnico](docs/manual/README.md) | los objetivos reales y cada carpeta, archivo y pieza de código explicados: qué es, cómo funciona y por qué |

![Andes Bank: el proyecto completo](docs/diagramas/00_panorama.svg)

## MVP

Core bancario · Tarjetas (API con caos) · CRM comercial (API) · FX (API real del BCRA + espejo con caos) · 4 Genie Agents
(Comercial, Tarjetas y fraude, Finanzas y tesorería, Plataforma de datos).

**Tope de Azure:** USD 10/mes.
