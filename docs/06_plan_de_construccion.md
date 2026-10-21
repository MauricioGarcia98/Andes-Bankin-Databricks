# 06 · Plan de construcción (borrador del índice de `construir/`)

Mismo formato que Andes Viento Austral: cada capítulo tiene Objetivo, Conceptos, Paso a paso, Por qué así, Probalo,
Si falla, Punto de control (tag `etapa-NN`) y Ejercicio. Los tiempos son estimados.

| # | etapa | qué queda andando | tiempo |
|---|---|---|---|
| **Parte 0 · Accesos** | | | |
| 00 | Cuentas y herramientas | Databricks Free Edition verificada con LinkedIn, Azure con presupuesto de USD 10 y alertas, CLIs `databricks`, `az`, `sf` | 2 h |
| 01 | *Spike* de Free Edition | ADR con lo que comprobaste: acceso saliente, lectura de Blob, secret scopes, cuotas | 1-2 h |
| **Parte 1 · Fuentes (la empresa simulada)** | | | |
| 02 | Cimientos del repo | paquete Python, lint, tests, configuración, `scale_factor` | 2 h |
| 03 | La empresa en código | sucursales, clientes, cuentas, con semilla fija | 3 h |
| 04 | Movimientos y tarjetas | el generador de eventos a SF 0,1 y SF 1 | 3 h |
| 05 | Infraestructura Azure con `az` | resource group, Storage, Function App, Application Insights, presupuesto, teardown | 2 h |
| 06 | La API de tarjetas | Functions v1 y v2, OAuth simulado, paginación, perfiles de caos | 4 h |
| 07 | El espejo de FX y la API real del BCRA | Function espejo + prueba contra la API real | 1 h 30 |
| 08 | Salesforce | crear la org, el campo custom y la carga con `sf` | 2 h |
| **Parte 2 · Ingesta** | | | |
| 09 | Bronze de archivos | Auto Loader sobre el core, con los dos formatos | 2 h |
| 10 | El cliente de API robusto | reintentos, backoff, token, cursor, checkpoint, `ops.api_calls` | 4 h |
| 11 | Bronze de tarjetas, FX y Salesforce | las tres ingestas por API, idempotentes | 3 h |
| **Parte 3 · Transformación** | | | |
| 12 | Silver I: limpiar | parseos argentinos, monedas, husos, identificadores, cuarentena | 3 h |
| 13 | Silver II: identidad, SCD2 y conciliación | `customer_xref`, SCD2, saldos vs. movimientos | 3 h |
| 14 | Gold | modelo dimensional | 3 h |
| 15 | Quality gate | reglas, gate y pipeline completo | 2 h |
| **Parte 4 · Plataforma** | | | |
| 16 | Asset Bundle y jobs | job histórico, job diario y mantenimiento | 3 h |
| 17 | Gobierno | comentarios, PK/FK, tags, máscaras de PII, row filters por región | 2 h |
| 18 | Capa semántica | metric views y funciones certificadas | 2 h |
| **Parte 5 · Genie** | | | |
| 19 | Genie Agents de negocio | Comercial, Tarjetas y fraude, Finanzas, con benchmarks | 4 h |
| 20 | Genie Agent Plataforma de datos | Genie para el equipo de datos sobre `ops.*` | 2 h |
| 21 | Genie Code (y la App, opcional) | instrucciones y skills; App como extra | 2 h |
| **Parte 6 · Operación** | | | |
| 22 | CI/CD | GitHub Actions | 2 h |
| 23 | Operar con el caos | qué hacer cuando falla cada cosa; prueba de volumen a SF 1 | 2 h |
| 24 | Contarlo | el posteo de LinkedIn con números reales ([07](07_posteo_linkedin.md)) | 1 h |

Total aproximado: **55-60 horas**.

## En qué orden trabajamos (vos y Claude Code)

1. Aprobás esta definición (docs 00 a 07).
2. Se construye **etapa por etapa**. Al final de cada una: tests verdes, commit con tag y el capítulo de `construir/`
   escrito con los errores **reales** que aparecieron.
3. Salesforce se crea recién en la etapa 08.
