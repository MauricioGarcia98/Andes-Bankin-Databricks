# ADR 0001 · Free Edition con acceso saliente: ingesta *pull*

- **Fecha:** 2026-10-21
- **Estado:** aceptado

## Contexto

La doc oficial de Free Edition dice que el acceso saliente a internet está restringido a dominios de confianza y que
se desbloquea verificando la cuenta con LinkedIn. Necesitábamos saber si Databricks puede llamar directamente a las
APIs del proyecto (Azure Functions, Blob, Salesforce, BCRA).

## Prueba

Un notebook (`0001_spike_outbound.py`) corrido como job serverless con `databricks jobs submit`, que hace un `GET` a
cada dominio. Resultado con la cuenta verificada:

| destino | resultado |
|---|---|
| `api.bcra.gob.ar` (API real de cotizaciones) | OK 200 |
| Azure (`azure.microsoft.com`, `functions.azure.com`) | OK 200 |
| `login.salesforce.com` | OK 200 |
| `api.github.com` | OK 200 |

## Decisión

**Patrón *pull*:** los jobs de Databricks llaman a las APIs y leen Blob directamente. El plan B (*push* con una Azure
Function que deja archivos) queda documentado pero no se construye.

## Consecuencias

- El cliente de API corre dentro de Databricks: los reintentos, el backoff y los checkpoints se escriben en Python y
  quedan registrados en `ops.api_calls`.
- Hay que repetir la prueba en tu cuenta nueva cuando reconstruyas el proyecto: si no verificaste la cuenta, el job
  falla con errores de conexión, no con códigos HTTP.
