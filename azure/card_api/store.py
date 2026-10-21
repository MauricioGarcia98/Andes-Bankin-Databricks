"""Dónde lee la API sus datos: páginas pre-armadas en Blob Storage.

    api-data/<recurso>/<yyyy>/<mm>/<dd>/_manifest.json     {"pages": 7, "records": 6412, "amount_ars": ...}
    api-data/<recurso>/<yyyy>/<mm>/<dd>/page-00001.json    [ {...}, ... ]   (1.000 registros por página)

La API no tiene base de datos: cada pedido lee UN blob. Por eso sirve millones de filas por centavos.
"""

from __future__ import annotations

import json
from functools import lru_cache

PAGE_SIZE = 1000


def day_prefix(resource: str, day: str) -> str:
    y, m, d = day.split("-")
    return f"{resource}/{y}/{m}/{d}"


class BlobStore:  # pragma: no cover - se prueba contra Azure
    def __init__(self, connection_string: str, container: str = "api-data"):
        from azure.storage.blob import BlobServiceClient

        self.client = BlobServiceClient.from_connection_string(connection_string).get_container_client(container)
        self._manifest = lru_cache(maxsize=4096)(self._read_manifest)

    def _read(self, name: str):
        try:
            return json.loads(self.client.download_blob(name).readall())
        except Exception as exc:  # BlobNotFound
            if "BlobNotFound" in type(exc).__name__ or "BlobNotFound" in str(exc):
                return None
            raise

    def _read_manifest(self, resource: str, day: str):
        return self._read(f"{day_prefix(resource, day)}/_manifest.json")

    def manifest(self, resource: str, day: str):
        return self._manifest(resource, day)

    def page(self, resource: str, day: str, page: int):
        return self._read(f"{day_prefix(resource, day)}/page-{page:05d}.json")


class MemoryStore:
    """Store en memoria para los tests: {(recurso, día): [registros]}."""

    def __init__(self, data: dict[tuple[str, str], list[dict]]):
        self.data = data

    def manifest(self, resource: str, day: str):
        rows = self.data.get((resource, day))
        if rows is None:
            return None
        pages = max(1, -(-len(rows) // PAGE_SIZE)) if rows else 0
        amount = "billing_amount_ars" if resource == "authorizations" else "settled_billing_ars"
        return {"pages": pages, "records": len(rows), "amount_ars": round(sum(r.get(amount, 0) for r in rows), 2)}

    def page(self, resource: str, day: str, page: int):
        rows = self.data.get((resource, day))
        if rows is None or page < 1:
            return None
        chunk = rows[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
        return chunk or None


class BlobFxStore:  # pragma: no cover - se prueba contra Azure
    """Serie de cotizaciones del espejo: fx-mirror/cotizaciones/<MONEDA>.json (la copia un job desde el BCRA real)."""

    def __init__(self, connection_string: str, container: str = "fx-mirror"):
        from azure.storage.blob import BlobServiceClient

        self.client = BlobServiceClient.from_connection_string(connection_string).get_container_client(container)
        self._cache: dict[str, tuple[float, list]] = {}

    def series(self, currency: str):
        import time

        cached = self._cache.get(currency)
        if cached and time.time() - cached[0] < 600:
            return cached[1]
        try:
            data = json.loads(self.client.download_blob(f"cotizaciones/{currency}.json").readall())
        except Exception:
            data = None
        self._cache[currency] = (time.time(), data)
        return data


class MemoryFxStore:
    def __init__(self, data: dict[str, list[dict]]):
        self.data = data

    def series(self, currency: str):
        return self.data.get(currency)
