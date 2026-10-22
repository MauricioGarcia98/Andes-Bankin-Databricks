# 03 · *Spike* de Free Edition: qué se puede y qué no

## 🎯 Objetivo

Antes de diseñar nada, **probar** las dos cosas de las que depende todo el proyecto:

1. ¿Databricks puede llamar a una API de internet (tu Azure Function, el BCRA)?
2. ¿Puede leer tu Azure Blob Storage?

El resultado queda escrito en un ADR (*Architecture Decision Record*): [ADR 0001](../docs/adr/0001_free_edition_acceso_saliente.md)
y [ADR 0002](../docs/adr/0002_landing_sync_blob_a_volume.md).

**Archivos:** `docs/adr/0001_spike_outbound.py`, `docs/adr/0001_*.md`, `docs/adr/0002_*.md`.

## 📚 Conceptos

- **Spike:** un experimento corto y descartable para sacarse una duda antes de construir. Lo que queda es la
  **decisión**, no el código.
- **ADR:** un documento cortito con contexto, decisión y consecuencias. Dentro de seis meses, cuando alguien pregunte
  "¿por qué copian a un Volume en vez de leer Blob directo?", la respuesta está escrita.
- ***Pull* vs. *push*:** en *pull*, Databricks llama a las fuentes; en *push*, alguien deja los datos donde
  Databricks los lee. *Pull* es lo normal en un equipo de datos, pero necesita acceso saliente.

## 🧱 Paso a paso

### 1. La prueba de acceso saliente

`docs/adr/0001_spike_outbound.py` es un notebook que hace un `GET` a cada dominio y muestra el código:

```python
hosts = {"bcra": "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones",
         "azure_functions": "https://functions.azure.com", "github": "https://api.github.com", ...}
for k, u in hosts.items():
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "andes-spike"}), timeout=15)
        res[k] = f"OK {r.status}"
    except urllib.error.HTTPError as e:
        res[k] = f"HTTP {e.code} (llega)"          # un 404 también prueba que HAY salida a internet
    except Exception as e:
        res[k] = f"FALLA {type(e).__name__}: {str(e)[:120]}"
```

Fijate en el segundo `except`: un error HTTP (404, 401) **también** es éxito para el spike, porque significa que la
conexión salió y el servidor contestó. Lo que no queremos ver es `FALLA ConnectionError`.

Correlo como job serverless (no hace falta un cluster):

```bash
databricks workspace import /Users/$USER/spike --file docs/adr/0001_spike_outbound.py --language PYTHON --format SOURCE
databricks jobs submit --json '{"run_name": "spike", "tasks": [{"task_key": "s",
  "notebook_task": {"notebook_path": "/Users/'$USER'/spike"}}]}'
```

### 2. La prueba de lectura de Blob

Probá las dos formas de leer Blob desde serverless:

- una *external location* con una credencial de Unity Catalog → Free Edition **no** la permite;
- `spark.conf.set("fs.azure.sas...", ...)` → serverless **no** acepta credenciales de Azure en la configuración.

Lo que **sí** anda: el SDK de Azure (`azure-storage-blob`) con una SAS, desde Python. Esa es la base del
capítulo 12: copiar de Blob a un Volume y que Auto Loader lea el Volume.

### 3. Escribí los ADR

Copiá la estructura de los ADR del repo: **Contexto → Prueba → Decisión → Consecuencias**.

## 🤔 Por qué así

- **¿Por qué no asumir que anda?** Porque la documentación de Free Edition cambia, y si el acceso saliente no
  funciona, la mitad del proyecto (las APIs) cambia de diseño. Media hora de prueba ahorra días.
- **¿Por qué copiar a un Volume y no leer Blob directo?** Porque Free Edition no deja otra. Pero además es un patrón
  real ("copy to landing" de ADF o Fivetran) y el borde es el mejor lugar para validar archivos (ADR 0002).

## 🧪 Probalo

La salida del job tiene que mostrar `200` para el BCRA y para Azure. Si muestra `FALLA ConnectionError`, la cuenta
no está verificada.

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `ConnectionError` / `ProxyError` en todos los dominios | cuenta de Free Edition sin verificar | verificala con LinkedIn (capítulo 02) |
| `[UNSUPPORTED_FEATURE] external location` | Free Edition no admite storage propio | es esperado: por eso existe el capítulo 12 (bitácora R05) |
| `fs.azure...` ignorado o `CONFIG_NOT_AVAILABLE` | serverless no acepta credenciales en `spark.conf` | igual que arriba: SAS desde Python y copia a un Volume |

## ✅ Punto de control

Tus dos ADR dicen lo mismo que los del repo, con **tu** fecha y **tus** resultados.

## 🏋️ Ejercicio

Agregá al spike la URL de tu Azure Function (`https://<func>.azurewebsites.net/api/health`) cuando la tengas
desplegada (capítulo 09) y confirmá que también da 200.
