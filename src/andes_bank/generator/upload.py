"""Sube los exportes del Volume (`_staging/`) a Azure Blob, con el nombre que usaría cada sistema origen.

    core          clientes/2025/05/03/CLIENTES_20250503.csv.gz        (antes de mar-2024: .csv sin comprimir)
                  movimientos/2025/08/15/MOVIMIENTOS_20250812.csv.gz  (el archivo del 12 que llegó el 15)
    cards-legacy  autorizaciones/2024/01/10/autorizaciones_20240110.json
    api-data      authorizations/2025/07/15/page-00001.json + _manifest.json   (lo que sirve la Function)

Acá también se "rompen" los archivos de los incidentes de entrega que no se pueden armar con Spark:
  ING-02  el archivo vacío (sólo encabezado) y su _REENVIO del día siguiente
  ING-03  el archivo truncado (gzip cortado a la mitad) y su _REENVIO
  ING-01  el mismo archivo subido dos veces con otro nombre
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta

from andes_bank.config import CONTAINERS, ProjectConfig
from andes_bank.generator.exports import CORRUPT_FILE, DUPLICATE_FILE, EMPTY_FILE

log = logging.getLogger(__name__)
PAGE_SIZE = 1000
CORE_PREFIX = {"clientes": "CLIENTES", "cuentas": "CUENTAS", "tarjetas": "TARJETAS", "movimientos": "MOVIMIENTOS",
               "saldos": "SALDOS"}
_PART = re.compile(r"^part-.*\.(csv|csv\.gz|json)$")


@dataclass(frozen=True)
class Upload:
    container: str
    name: str
    data: bytes


def _parts(folder: str) -> list[str]:
    if not os.path.isdir(folder):
        return []
    return sorted(os.path.join(folder, f) for f in os.listdir(folder) if _PART.match(f))


def _dated_dirs(root: str, key: str) -> Iterator[tuple[date, str]]:
    if not os.path.isdir(root):
        return
    for d in sorted(os.listdir(root)):
        if d.startswith(f"{key}="):
            yield date.fromisoformat(d.split("=", 1)[1]), os.path.join(root, d)


def _concat(files: list[str]) -> bytes:
    """Une los part-files de una carpeta en un solo archivo (los .gz se pueden concatenar tal cual)."""
    out = io.BytesIO()
    for f in files:
        with open(f, "rb") as fh:
            out.write(fh.read())
    return out.getvalue()


def _in_range(d: date, start: date | None, end: date | None) -> bool:
    return (start is None or d >= start) and (end is None or d <= end)


# ------------------------------------------------------------------------------------------------- core
def core_uploads(cfg: ProjectConfig, start: date | None = None, end: date | None = None) -> Iterator[Upload]:
    container = CONTAINERS["core"]
    for source, prefix in CORE_PREFIX.items():
        root = cfg.staging_path(f"core/{source}")
        for delivery, ddir in _dated_dirs(root, "delivery_date"):
            if not _in_range(delivery, start, end):
                continue
            for file_date, fdir in _dated_dirs(ddir, "file_date"):
                files = _parts(fdir)
                if not files:
                    continue
                ext = "csv.gz" if files[0].endswith(".gz") else "csv"
                data = _concat(files)
                folder = f"{source}/{delivery:%Y/%m/%d}"
                resend = source == "movimientos" and file_date in (EMPTY_FILE, CORRUPT_FILE) and delivery != file_date
                name = f"{folder}/{prefix}_{file_date:%Y%m%d}{'_REENVIO' if resend else ''}.{ext}"
                yield Upload(container, name, data)
                if source != "movimientos":
                    continue
                if file_date == EMPTY_FILE and resend:                                        # ING-02
                    header = gzip.decompress(data).split(b"\n", 1)[0] + b"\n"
                    yield Upload(container, f"movimientos/{EMPTY_FILE:%Y/%m/%d}/{prefix}_{EMPTY_FILE:%Y%m%d}.{ext}",
                                 gzip.compress(header) if ext.endswith("gz") else header)
                if delivery == CORRUPT_FILE == file_date:                                     # ING-03
                    yield Upload(container, name, data[: len(data) // 2])                     # pisa con la versión cortada
                    nxt = CORRUPT_FILE + timedelta(days=1)
                    yield Upload(container, f"movimientos/{nxt:%Y/%m/%d}/{prefix}_{CORRUPT_FILE:%Y%m%d}_REENVIO.{ext}", data)
                if delivery == DUPLICATE_FILE == file_date:                                   # ING-01
                    yield Upload(container, f"{folder}/{prefix}_{file_date:%Y%m%d}_copia.{ext}", data)


# ----------------------------------------------------------------------------------------- procesador
def cards_legacy_uploads(cfg: ProjectConfig, start: date | None = None, end: date | None = None) -> Iterator[Upload]:
    container = CONTAINERS["cards_legacy"]
    for source in ("autorizaciones", "liquidaciones"):
        for delivery, ddir in _dated_dirs(cfg.staging_path(f"cards_legacy/{source}"), "delivery_date"):
            if _in_range(delivery, start, end) and (files := _parts(ddir)):
                yield Upload(container, f"{source}/{delivery:%Y/%m/%d}/{source}_{delivery:%Y%m%d}.json", _concat(files))


def api_uploads(cfg: ProjectConfig, start: date | None = None, end: date | None = None) -> Iterator[Upload]:
    """Parte cada día en páginas de 1.000 registros + un _manifest.json con los totales (para conciliar)."""
    container = CONTAINERS["api_data"]
    amount_field = {"authorizations": "billing_amount_ars", "settlements": "settled_billing_ars"}
    for resource, amount in amount_field.items():
        for day, ddir in _dated_dirs(cfg.staging_path(f"api_data/{resource}"), "delivery_date"):
            if not _in_range(day, start, end):
                continue
            rows = [json.loads(line) for line in _concat(_parts(ddir)).decode("utf-8").splitlines() if line.strip()]
            folder = f"{resource}/{day:%Y/%m/%d}"
            pages = 0
            for i in range(0, len(rows), PAGE_SIZE):
                pages += 1
                yield Upload(container, f"{folder}/page-{pages:05d}.json",
                             json.dumps(rows[i: i + PAGE_SIZE], ensure_ascii=False, separators=(",", ":")).encode())
            manifest = {"pages": pages, "records": len(rows), "amount_ars": round(sum(r.get(amount, 0) for r in rows), 2)}
            yield Upload(container, f"{folder}/_manifest.json", json.dumps(manifest).encode())


# --------------------------------------------------------------------------------------------- subida
def upload_all(cfg: ProjectConfig, sas_token: str, start: date | None = None, end: date | None = None,
               parts: tuple[str, ...] = ("core", "cards_legacy", "api_data"), workers: int = 32,
               skip_existing: bool = True) -> dict[str, int]:
    """Sube todo en paralelo.

    `skip_existing`: no vuelve a subir lo que ya está. Importa: re-subir un archivo idéntico le cambia el etag, y
    para el lakehouse eso es "el sistema origen cambió un archivo ya entregado" (estado CHANGED en landing_sync).
    """
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(account_url=f"https://{cfg.storage_account}.blob.core.windows.net", credential=sas_token)
    clients = {c: service.get_container_client(c) for c in CONTAINERS.values()}
    gens = {"core": core_uploads, "cards_legacy": cards_legacy_uploads, "api_data": api_uploads}
    counts: dict[str, int] = {}

    def put(u: Upload) -> None:
        clients[u.container].upload_blob(u.name, u.data, overwrite=True)

    existing: set[tuple[str, str]] = set()
    if skip_existing:
        for c, client in clients.items():
            existing |= {(c, b.name) for b in client.list_blobs()}

    for part in parts:
        n = 0
        pending: dict[tuple[str, str], object] = {}
        gate = threading.BoundedSemaphore(workers * 2)        # como mucho 2x workers archivos en memoria
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for u in gens[part](cfg, start, end):
                key = (u.container, u.name)
                if key in existing:
                    continue
                if key in pending:            # ING-03: el truncado pisa al completo, tiene que subirse después
                    pending[key].result()
                gate.acquire()
                fut = pool.submit(put, u)
                fut.add_done_callback(lambda _f, g=gate: g.release())
                pending[key] = fut
                n += 1
            for fut in pending.values():
                fut.result()                   # propaga errores
        counts[part] = n
        log.info("%s: %d blobs subidos", part, n)
    return counts


# ------------------------------------------------------------------------------------------- espejo FX
BCRA_URL = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{currency}"


def fetch_bcra_series(currency: str, start: date, end: date, session=None) -> list[dict]:
    """Serie diaria real del BCRA, paginada de a 1.000 (el BCRA devuelve de la fecha más nueva a la más vieja)."""
    import requests

    s = session or requests.Session()
    out, offset = [], 0
    while True:
        r = s.get(BCRA_URL.format(currency=currency), timeout=30,
                  params={"fechadesde": start.isoformat(), "fechahasta": end.isoformat(), "offset": offset, "limit": 1000})
        r.raise_for_status()
        body = r.json()
        for day in body.get("results", []):
            for d in day.get("detalle", []):
                out.append({"fecha": day["fecha"], "descripcion": d.get("descripcion"), "tipoPase": d.get("tipoPase"),
                            "tipoCotizacion": d.get("tipoCotizacion")})
        count = body.get("metadata", {}).get("resultset", {}).get("count", 0)
        offset += 1000
        if offset >= count:
            return out


def upload_fx_mirror(cfg: ProjectConfig, sas_token: str, currencies: tuple[str, ...] = ("USD", "EUR", "BRL")) -> dict[str, int]:
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(account_url=f"https://{cfg.storage_account}.blob.core.windows.net", credential=sas_token)
    container = service.get_container_client("fx-mirror")
    counts = {}
    for cur in currencies:
        series = fetch_bcra_series(cur, cfg.start_date, cfg.end_date)
        container.upload_blob(f"cotizaciones/{cur}.json", json.dumps(series, ensure_ascii=False).encode(), overwrite=True)
        counts[cur] = len(series)
    return counts
