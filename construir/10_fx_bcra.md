# 10 · Etapa 07 · El espejo de FX y la API real del BCRA (y el job de fuentes)

## 🎯 Objetivo

- Traer la cotización oficial del dólar, el euro y el real desde la **API real del BCRA**, la única fuente 100 % real
  del proyecto.
- Montar un **espejo** de esa API en la Function, con su propia suciedad, como respaldo.
- Armar el **job de fuentes** (`andes-bank-fuentes`): generar la verdad, exportarla sucia, subirla a Azure y copiar
  el BCRA al espejo.

**Archivos:** `src/andes_bank/generator/upload.py`, `azure/card_api/api.py` (`_fx`), `notebooks/0[0-4]_*.py`,
`resources/andes_fuentes.job.yml`.

![Fuentes: el banco simulado](../docs/diagramas/01_fuentes.svg)

## 📚 Conceptos

- **API del BCRA:** `GET /estadisticascambiarias/v1.0/Cotizaciones/USD?fechadesde=&fechahasta=&offset=&limit=`.
  Devuelve de la fecha más nueva a la más vieja, paginada de a 1.000, y **no publica fines de semana ni feriados**
  (DAT-13).
- **Espejo con suciedad:** la misma forma de respuesta, pero en sep-2024 el valor viene como texto con coma
  (`"960,5000"`) y en feb-2025 el código de moneda cambia a `U$S`. Silver tiene que bancarse las dos cosas.
- **Job de fuentes:** no es parte del lakehouse: es el "mundo exterior". Lo corrés cuando querés regenerar el banco.

## 🧱 Paso a paso

### 1. Copiar el BCRA al espejo (`upload.py`)

```python
def fetch_bcra_series(currency, start, end, session=None) -> list[dict]:
    while True:
        r = s.get(BCRA_URL.format(currency=currency), timeout=30,
                  params={"fechadesde": start.isoformat(), "fechahasta": end.isoformat(), "offset": offset, "limit": 1000})
        ...
        offset += 1000
        if offset >= count:
            return out
```

La serie se guarda en `fx-mirror/cotizaciones/USD.json` y la Function la sirve en `/fx/...`.

### 2. El espejo, ensuciado (`api._dirty_fx`)

```python
if r["fecha"].startswith("2024-09"):
    value = f"{value:.4f}".replace(".", ",")                      # texto con coma
code = "U$S" if (currency == "USD" and r["fecha"].startswith("2025-02")) else currency
```

### 3. Subir a Azure sin pisar (`upload_all`)

```python
existing = {(c, b.name) for c, client in clients.items() for b in client.list_blobs()}
...
if key in existing:
    continue                     # re-subir un archivo idéntico le cambia el etag: para el lakehouse sería "CHANGED"
```

La clase `Upload` lee el contenido **recién** cuando hace falta: si el blob ya está en Azure, ni se lee del Volume
(con miles de archivos, segundos en vez de media hora).

### 4. El job de fuentes (`resources/andes_fuentes.job.yml`)

```
setup → generate_truth → export_sources → upload_to_azure
setup → fx_mirror                (en paralelo)
generate_truth → crm_publish     (capítulo 11)
```

## 🤔 Por qué así

- **¿Por qué un espejo si el BCRA es gratis?** Porque una API pública a veces no responde, y porque la suciedad del
  espejo enseña a no confiar en el formato. Silver usa la real cuando está y el espejo de respaldo, y lo registra en
  `fx_source`.
- **¿Por qué no re-subir todo?** R07: un blob re-subido idéntico cambia de etag, y landing lo trataría como "el
  sistema origen modificó un archivo ya entregado".

## 🧪 Probalo

```bash
databricks bundle deploy -t dev
databricks bundle run andes_fuentes -t dev       # la primera vez tarda (genera ~17 M de movimientos)
curl -s "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/USD?fechadesde=2025-07-01&fechahasta=2025-07-05"
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Todos los archivos aparecen `CHANGED` en landing | se re-subió todo | `upload_all(skip_existing=True)` (R07) |
| El BCRA no responde (timeout) | la API pública a veces falla | reintentos; el job tiene `max_retries: 2` en `fx_mirror` |
| `upload_to_azure` tarda media hora | lee del Volume archivos que ya están en Azure | `Upload` perezoso: sólo lee lo que va a subir |

## ✅ Punto de control

`git diff etapa-07 -- src/andes_bank/generator/upload.py resources/andes_fuentes.job.yml`

## 🏋️ Ejercicio

Agregá la libra esterlina (`GBP`) al espejo y a `ingest_fx`. ¿Qué tabla de gold tenés que tocar para que Genie la vea?
