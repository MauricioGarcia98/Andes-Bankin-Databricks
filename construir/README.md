# 🛠️ Construilo vos: Andes Bank desde un repo vacío

> **Qué es esto:** el curso para **borrar todo** (Databricks, Azure y el repo) y **volver a construir el
> proyecto vos solo**, archivo por archivo y en el orden en que se construyó. Cada bloque de código se explica: **qué
> hace, por qué se hizo así y no de otra forma, qué va a fallar y cómo se arregla**. Los "🔥 Si falla" salen de la
> [bitácora](../docs/08_bitacora.md): son errores que **pasaron de verdad** mientras se construía.

## Para quién es

Para alguien que programa un poco (funciones de Python, un `SELECT` con `GROUP BY`) y quiere aprender en serio
**Databricks Free Edition**, **Azure** y **Genie**, con un problema que se parece al de un banco real.

## Qué vas a saber hacer al terminar

- Simular una empresa completa con datos sintéticos **reproducibles** (Spark y una semilla) y ensuciarlos a propósito.
- Montar fuentes reales en la nube:
  - Blob Storage con los archivos del core;
  - una API en Azure Functions con OAuth, paginación por cursor y **caos**;
  - la API pública del BCRA;
  - un CRM servido por la misma API, con incremental y borrados.
- Escribir un cliente de API que no se rompe: reintentos, *backoff*, renovación de token y conciliación contra totales.
- Construir el medallion completo:
  - Auto Loader con dos contratos y *schema drift*;
  - SCD2 desde CDC;
  - resolución de identidad;
  - un modelo dimensional;
  - un *quality gate*.
- Gobernar con Unity Catalog: comentarios, claves, tags, máscaras de PII y filtros por región.
- Preparar los datos para Genie (metric views, funciones certificadas) y versionar **4 Genie Agents como código**, con
  benchmarks que se validan contra los datos.
- Llevarlo todo a producción como código: Asset Bundles, jobs, CI en GitHub Actions y operación diaria.

## Cómo está armado

Antes de empezar, mirá los [diagramas](../docs/diagramas/README.md): uno por fase, para saber dónde estás parado en cada capítulo.

| # | capítulo | tag al terminar | tiempo |
|---|---|---|---|
| 00 | este documento | — | 15 min |
| **Parte 0 · Accesos** | | | |
| 01 | [Borrar todo (y guardar la referencia)](01_borrar_todo.md) | — | 45 min |
| 02 | [Cuentas y herramientas: Free Edition, Azure con presupuesto, CLIs](02_cuentas_y_herramientas.md) | — | 2 h |
| 03 | [*Spike* de Free Edition: qué se puede y qué no](03_spike_free_edition.md) ([ADR 0001](../docs/adr/0001_free_edition_acceso_saliente.md)) | — | 1 h 30 |
| **Parte 1 · Fuentes (la empresa simulada)** | | | |
| 04 | [Cimientos del repo: paquete, configuración, CUIT/CBU](04_cimientos_del_repo.md) | `etapa-02` | 2 h |
| 05 | [La empresa en código: sucursales, clientes, cuentas, tarjetas](05_la_empresa_en_codigo.md) | `etapa-03` | 3 h |
| 06 | [Movimientos, tarjetas y saldos (y por qué crecían sin techo)](06_movimientos_tarjetas_y_saldos.md) | `etapa-03` | 3 h |
| 07 | [Exportes sucios del core y del procesador](07_exportes_sucios.md) | `etapa-04` | 3 h |
| 08 | [Azure con `az`: Storage, Function App, SAS y teardown](08_azure_con_az.md) | `etapa-05` | 2 h |
| 09 | [La API de tarjetas: OAuth, cursor, v1/v2 y caos](09_api_de_tarjetas.md) | `etapa-06` | 4 h |
| 10 | [El espejo de FX y la API real del BCRA](10_fx_bcra.md) | `etapa-07` | 1 h 30 |
| 11 | [El CRM comercial: generarlo y publicarlo en la API](11_crm.md) | `etapa-08` | 2 h |
| **Parte 2 · Ingesta** | | | |
| 12 | [Landing: de Blob al Volume, con validación en el borde](12_landing.md) ([ADR 0002](../docs/adr/0002_landing_sync_blob_a_volume.md)) | `etapa-09` | 2 h |
| 13 | [Bronze de archivos con Auto Loader (dos contratos)](13_bronze_auto_loader.md) | `etapa-09` | 2 h |
| 14 | [El cliente de API robusto](14_cliente_de_api.md) | `etapa-10` | 4 h |
| 15 | [Bronze de tarjetas, FX y CRM](15_bronze_apis.md) | `etapa-11` | 3 h |
| **Parte 3 · Transformación** | | | |
| 16 | [Silver I: limpiar (números argentinos, monedas, husos, PII)](16_silver_limpiar.md) | `etapa-12` | 3 h |
| 17 | [Silver II: identidad, SCD2, conciliación y CRM](17_silver_identidad_scd2.md) | `etapa-13` | 3 h |
| 18 | [Gold: modelo dimensional y marts](18_gold.md) | `etapa-14` | 3 h |
| 19 | [Quality gate y el pipeline completo en local](19_quality_gate.md) | `etapa-15` | 2 h |
| **Parte 4 · Plataforma** | | | |
| 20 | [Asset Bundle y jobs](20_asset_bundle_y_jobs.md) | `etapa-16` | 3 h |
| 21 | [Gobierno en Unity Catalog](21_gobierno_unity_catalog.md) | `etapa-17` | 2 h |
| 22 | [Capa semántica: metric views y funciones certificadas](22_capa_semantica.md) | `etapa-18` | 2 h |
| **Parte 5 · Genie** | | | |
| 23 | [Genie Agents como código (los 4, con benchmarks)](23_genie_agents.md) | `etapa-19` | 4 h |
| 24 | [Genie Code: instrucciones y skills](24_genie_code.md) | `etapa-21` | 1 h |
| **Parte 6 · Operación** | | | |
| 25 | [CI/CD con GitHub Actions](25_ci_cd.md) | `etapa-22` | 2 h |
| 26 | [Operar con el caos: corrida diaria y diagnóstico](26_operar_con_el_caos.md) | `etapa-23` | 2 h |
| 27 | [Contarlo: el posteo de LinkedIn](27_contarlo.md) ([docs/07](../docs/07_posteo_linkedin.md)) | `etapa-24` | 1 h |
| 28 | [Examen final](28_examen_final.md) | — | 1-2 días |

Total: unas **55-60 horas**.

## Cómo es cada capítulo

1. **🎯 Objetivo**: qué vas a tener al final y por qué esta etapa va en este lugar.
2. **📚 Conceptos**: lo mínimo que tenés que entender antes de escribir código.
3. **🧱 Paso a paso**: los archivos, en orden, con el código y su explicación.
4. **🤔 Por qué así**: las alternativas que se descartaron y por qué.
5. **🧪 Probalo**: el comando y lo que tenés que ver.
6. **🔥 Si falla**: los errores reales de la bitácora, con el síntoma exacto.
7. **✅ Punto de control**: comparás tu repo con el tag (`git diff etapa-NN -- src/`).
8. **🏋️ Ejercicio**: un cambio chico para hacer sin mirar.

## Reglas de juego

- **Escribí el código vos.** Copiar y pegar no enseña; usá el tag para comparar, no para copiar.
- **Nada de secretos en el repo ni en un chat.** Las credenciales van en variables de entorno, en el secret scope
  `andes_bank` y en la configuración de la Function App.
- **Cuidá el presupuesto.** Todo el proyecto cuesta centavos por mes en Azure y USD 0 en Databricks. Si
  ves otra cosa, pará y revisá (capítulo 08).
- **Los tests primero.** Cada etapa se prueba en local (Spark + Delta) antes de subirla a Databricks.
