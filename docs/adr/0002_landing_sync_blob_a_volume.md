# ADR 0002 · Blob → Volume con una SAS de sólo lectura (y validación en el borde)

- **Fecha:** 2026-10-21
- **Estado:** aceptado

## Contexto

Los sistemas origen dejan sus archivos en Azure Blob. Lo ideal sería que Auto Loader los lea directo
(`abfss://...`), pero en Databricks Free Edition:

- no se pueden crear *external locations* hacia un storage propio (la credencial de Unity Catalog necesitaría
  además guardar el secreto de un service principal de Azure en el workspace);
- el cómputo serverless no acepta credenciales de Azure en la configuración de Spark (`fs.azure.*`).

## Decisión

Un paso de **sincronización** (`andes_bank.ingestion.landing_sync`) lista los contenedores con una SAS de **sólo
lectura** (secret scope `andes_bank/blob_sas_read`) y copia lo nuevo al Volume `andes_bank.landing.raw`. Auto Loader lee
el Volume.

La copia es el borde del lakehouse, y ahí se valida cada archivo:

| estado | qué pasó | qué se hace |
|---|---|---|
| `LANDED` | archivo sano | se copia al Volume |
| `EMPTY` | sólo encabezado | se copia y queda la alerta (ING-02) |
| `CORRUPT` | el gzip no descomprime | va a `_errors/`, no al Volume (ING-03) |
| `DUPLICATE_CONTENT` | mismo contenido que otro ya aterrizado | no se copia (ING-01) |
| `CHANGED` | el blob cambió después de copiarlo | alerta; no se re-copia |

## Consecuencias

- Es un patrón real (ADF / Fivetran "copy to landing"), no sólo un parche de Free Edition.
- Un archivo corrupto ya no tumba el stream de Auto Loader: se queda en el borde con su motivo en `ops.landing_files`.
- En un workspace pago con Unity Catalog sobre Azure, se reemplaza por una *external location* y Auto Loader directo;
  la validación de contenido pasaría a un paso de calidad sobre bronze.
