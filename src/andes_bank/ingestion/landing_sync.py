"""Sincronización Azure Blob → Volume (la "zona de aterrizaje" del lakehouse).

¿Por qué copiar y no leer Blob directo con Auto Loader? En Free Edition no hay *external locations* hacia tu propio
storage y el cómputo serverless no acepta credenciales de Azure en la configuración de Spark (ADR 0002). Copiar a un
Volume con una SAS de sólo lectura es además un patrón real (ADF / Fivetran "copy to landing").

La copia es el BORDE del lakehouse, y en el borde se valida cada archivo antes de dejarlo pasar:

  LANDED              se copió al Volume (Auto Loader lo va a levantar)
  EMPTY               se copió, pero no trae filas (alerta: ING-02)
  CORRUPT             no se pudo descomprimir: va a `_errors/`, NO al Volume (ING-03)
  DUPLICATE_CONTENT   mismo contenido que otro archivo ya aterrizado con otro nombre: no se copia (ING-01)
  CHANGED             el blob cambió después de aterrizarlo (mismo nombre, otro etag): alerta, no se re-copia

Todo queda en `ops.landing_files` (una fila por blob y versión).
"""

from __future__ import annotations

import gzip
import hashlib
import os
import zlib
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from andes_bank.config import ProjectConfig

LANDING_FILES_SCHEMA = T.StructType([
    T.StructField("container", T.StringType()), T.StructField("blob_name", T.StringType()),
    T.StructField("etag", T.StringType()), T.StructField("size_bytes", T.LongType()),
    T.StructField("md5", T.StringType()), T.StructField("status", T.StringType()),
    T.StructField("data_rows", T.LongType()), T.StructField("landed_path", T.StringType()),
    T.StructField("detail", T.StringType()), T.StructField("synced_at", T.TimestampType()),
])


@dataclass(frozen=True)
class RemoteFile:
    container: str
    name: str
    etag: str
    size: int
    fetch: Callable[[], bytes]


@dataclass
class SyncResult:
    container: str
    blob_name: str
    etag: str
    size_bytes: int
    md5: str | None
    status: str
    data_rows: int | None
    landed_path: str | None
    detail: str | None
    synced_at: datetime


def inspect(name: str, data: bytes) -> tuple[str, int | None, str | None]:
    """(status, filas de datos, detalle) de un archivo, sin Spark."""
    raw = data
    if name.endswith(".gz"):
        try:
            raw = gzip.decompress(data)
        except (EOFError, OSError, zlib.error) as exc:
            return "CORRUPT", None, f"{type(exc).__name__}: {str(exc)[:200]}"
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    rows = len(lines) - 1 if name.endswith((".csv", ".csv.gz")) else len(lines)
    return ("EMPTY" if rows <= 0 else "LANDED"), max(rows, 0), None


def sync(cfg: ProjectConfig, files: Iterable[RemoteFile], known: dict[tuple[str, str], str],
         known_md5: dict[str, str], workers: int = 16) -> list[SyncResult]:
    """Copia lo nuevo. `known`: (container, blob) → etag ya procesado. `known_md5`: md5 → blob ya aterrizado."""
    todo = [f for f in files if known.get((f.container, f.name)) != f.etag]
    now = datetime.now(timezone.utc)

    def fetch(f: RemoteFile) -> tuple[RemoteFile, bytes]:
        return f, f.fetch()

    results: list[SyncResult] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        downloaded = list(pool.map(fetch, sorted(todo, key=lambda f: (f.container, f.name))))
    for f, data in downloaded:
        md5 = hashlib.md5(data).hexdigest()
        target = os.path.join(cfg.root_path, f.container, f.name)
        if (f.container, f.name) in known:
            results.append(SyncResult(f.container, f.name, f.etag, f.size, md5, "CHANGED", None, None,
                                      "el blob cambió después de aterrizarlo; no se vuelve a copiar", now))
            continue
        status, rows, detail = inspect(f.name, data)
        if status != "CORRUPT" and md5 in known_md5:
            status, detail, target = "DUPLICATE_CONTENT", f"mismo contenido que {known_md5[md5]}", None
        elif status == "CORRUPT":
            target = os.path.join(cfg.errors_path("landing"), f.container, f.name)
        if target:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "wb") as fh:
                fh.write(data)
        if status in ("LANDED", "EMPTY"):
            known_md5[md5] = f"{f.container}/{f.name}"
        results.append(SyncResult(f.container, f.name, f.etag, f.size, md5, status, rows, target, detail, now))
    return results


def azure_files(account: str, sas: str, containers: Iterable[str]) -> Iterable[RemoteFile]:  # pragma: no cover
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=sas)
    for c in containers:
        client = service.get_container_client(c)
        for b in client.list_blobs():
            yield RemoteFile(c, b.name, b.etag, b.size,
                             lambda client=client, name=b.name: client.download_blob(name).readall())


def run_sync(spark: SparkSession, cfg: ProjectConfig, files: Iterable[RemoteFile]) -> dict[str, int]:
    """Sincroniza y registra el resultado en ops.landing_files."""
    table = cfg.table("ops", "landing_files")
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], LANDING_FILES_SCHEMA).write.saveAsTable(table)
    prev = spark.table(table).select("container", "blob_name", "etag", "md5", "status").collect()
    known = {(r.container, r.blob_name): r.etag for r in prev}
    known_md5 = {r.md5: f"{r.container}/{r.blob_name}" for r in prev if r.status in ("LANDED", "EMPTY") and r.md5}
    results = sync(cfg, files, known, known_md5)
    if results:
        spark.createDataFrame([asdict(r) for r in results], LANDING_FILES_SCHEMA).write.mode("append").saveAsTable(table)
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts
