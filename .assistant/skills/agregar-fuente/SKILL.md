---
name: agregar-fuente
description: Cómo sumar una fuente nueva al lakehouse de Andes Bank (de un sistema origen a gold) siguiendo las convenciones del proyecto.
---
# Agregar una fuente nueva

1. **Generador** (`src/andes_bank/generator/`): la verdad en `sim.<entidad>` y el exporte sucio en `exports.py`.
2. **Subida** (`generator/upload.py`): nombre del archivo como lo entregaría el sistema real.
3. **Bronze**: archivos → un `FileSource` en `ingestion/bronze_files.py` (un contrato por formato); API → un recurso en
   `ingestion/bronze_api.py` usando `ApiClient`.
4. **Silver**: una función por problema en `silver/`, cargada desde `silver/load.py`. Maestros: SCD2 con
   `scd2_from_events`. Hechos: MERGE insert-only con watermark por `_ingested_at`.
5. **Gold**: dimensión/hecho en `gold/model.py`; comentarios en `sql/governance/01_comments.sql`.
6. **Calidad**: una `Rule` por problema en `quality/gate.py` (fail si rompe, warn si es suciedad tratada).
7. **Tests**: unitario del parseo + el caso en `tests/test_e2e.py` contra la verdad.
8. **Docs**: fila en `docs/02_fuentes_mvp.md` y en el catálogo `docs/03`.
