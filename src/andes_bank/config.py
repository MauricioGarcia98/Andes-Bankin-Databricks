"""Configuración central de Andes Bank.

Un único objeto `ProjectConfig` resuelve:
  * los nombres de 3 niveles (catalog.schema.table) de Unity Catalog,
  * las rutas de los Volumes (lo que se copia desde Azure, checkpoints, errores de API),
  * los contenedores de Azure Blob donde "viven" los sistemas origen,
  * el `scale_factor`, que escala la CANTIDAD DE CLIENTES (y con ellos cuentas, tarjetas y movimientos).

¿Por qué escalar clientes y no días? Porque la historia del banco (devaluación de dic-2023, migración del
core en mar-2024, Salesforce en 2024, API v2 en 2025) tiene que estar completa a cualquier escala.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

# sim = "los sistemas origen" (la verdad que genera el generador; el lakehouse NO la lee, sólo los tests y
# las conciliaciones de control). El resto son las capas del lakehouse.
LAYERS = ("sim", "landing", "bronze", "silver", "gold", "semantic", "ops")

# Contenedores de Azure Blob: cada uno es un sistema origen distinto.
CONTAINERS = {
    "core": "core",                    # exportes nocturnos del core bancario (CSV / Parquet)
    "cards_legacy": "cards-legacy",    # exportes diarios de la integración vieja con el procesador (API v1)
    "api_data": "api-data",            # lo que sirve la API de tarjetas v2 (páginas pre-armadas por día)
}

# Fuentes del MVP (una carpeta por fuente dentro de su contenedor o endpoint).
CORE_SOURCES = ("customers", "accounts", "transactions", "balances")
CARD_SOURCES = ("authorizations", "settlements")


def is_databricks() -> bool:
    """True cuando el código corre dentro de un runtime de Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class ScaleProfile:
    """El banco a SF=1.

    500 mil clientes → ~800 mil cuentas → ~150 M movimientos y ~60 M autorizaciones de tarjeta entre 2023 y
    oct-2026. A SF 0,1 (el default en Free Edition) todo es 10 veces menor.
    """

    customers: int = 500_000
    merchants: int = 20_000
    usd_account_share: float = 0.40       # clientes con caja de ahorro en dólares
    checking_share: float = 0.15          # clientes con cuenta corriente (sobre todo pymes y premium)
    salary_share: float = 0.60            # clientes que cobran el sueldo en el banco
    credit_card_share: float = 0.35       # clientes con tarjeta de crédito
    debit_card_share: float = 0.85        # cajas de ahorro en pesos con tarjeta de débito
    txn_per_account_month: float = 4.5    # movimientos "propios" (sin sueldo ni compras con débito)
    card_purchases_month: float = 3.0     # compras por tarjeta por mes
    legacy_duplicate_share: float = 0.015 # clientes del NOA que también están como clientes de Cuyo (fusión 2008)


@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = "andes_bank"
    schema_prefix: str = ""          # en local: "andes_" para no chocar con otras bases
    volume: str = "raw"
    base_path: str | None = None     # override local (por defecto /Volumes/...)
    scale_factor: float = 0.1
    start_date: date = date(2023, 1, 1)
    end_date: date = date(2026, 10, 20)
    seed: int = 1998                 # año de fundación del banco
    storage_account: str = ""        # cuenta de Azure Storage (se resuelve por variable o widget)
    profile: ScaleProfile = field(default_factory=ScaleProfile)

    # ---------------------------------------------------------------- nombres
    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}. Válidas: {LAYERS}")
        if self.catalog == "spark_catalog":  # local: Delta OSS sin Unity Catalog
            return f"{self.schema_prefix}{layer}"
        return f"{self.catalog}.{self.schema_prefix}{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    # ------------------------------------------------------------------ rutas
    @property
    def root_path(self) -> str:
        if self.base_path:
            return self.base_path.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.schema_prefix}landing/{self.volume}"

    def landing_path(self, system: str, source: str) -> str:
        """Copia local (en el Volume) de lo que llegó de un sistema origen: core/transactions, cards/..."""
        return f"{self.root_path}/{system}/{source}"

    def staging_path(self, name: str) -> str:
        """Donde el generador deja los archivos antes de "subirlos" al sistema origen (Azure)."""
        return f"{self.root_path}/_staging/{name}"

    def checkpoint_path(self, name: str) -> str:
        return f"{self.root_path}/_checkpoints/{name}"

    def schema_location(self, name: str) -> str:
        return f"{self.root_path}/_schemas/{name}"

    def errors_path(self, name: str) -> str:
        return f"{self.root_path}/_errors/{name}"

    def blob_url(self, container_key: str) -> str:
        return f"https://{self.storage_account}.blob.core.windows.net/{CONTAINERS[container_key]}"

    # ---------------------------------------------------------------- volumen
    def scaled(self, n_real: int) -> int:
        """Cantidad a esta escala (mínimo 1)."""
        return max(1, int(round(n_real * self.scale_factor)))

    @property
    def n_customers(self) -> int:
        return self.scaled(self.profile.customers)

    @property
    def n_merchants(self) -> int:
        return max(200, self.scaled(self.profile.merchants))

    @property
    def n_days(self) -> int:
        return (self.end_date - self.start_date).days + 1


def local_config(base_path: str, scale_factor: float = 0.002, **kwargs) -> ProjectConfig:
    """Config para tests / ejecución local con Spark + Delta OSS (1.000 clientes)."""
    return ProjectConfig(
        catalog="spark_catalog",
        schema_prefix="andes_",
        base_path=base_path,
        scale_factor=scale_factor,
        **kwargs,
    )


def config_from_widgets(dbutils) -> ProjectConfig:  # pragma: no cover - sólo Databricks
    """Construye la config a partir de los widgets/parámetros del job."""

    def get(name: str, default: str) -> str:
        try:
            value = dbutils.widgets.get(name)
            return value if value != "" else default
        except Exception:
            return default

    return ProjectConfig(
        catalog=get("catalog", "andes_bank"),
        scale_factor=float(get("scale_factor", "0.1")),
        start_date=date.fromisoformat(get("start_date", "2023-01-01")),
        end_date=date.fromisoformat(get("end_date", "2026-10-20")),
        storage_account=get("storage_account", os.environ.get("ANDES_STORAGE_ACCOUNT", "")),
    )
