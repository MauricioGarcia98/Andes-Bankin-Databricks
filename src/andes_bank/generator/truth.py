"""Genera la "verdad" del banco y la guarda en el esquema `sim` (los sistemas origen por dentro).

    sim.branches · sim.customers · sim.accounts · sim.cards · sim.merchants · sim.months
    sim.authorizations · sim.settlements · sim.transactions · sim.balances

El lakehouse nunca lee `sim`: llega a los datos sólo por los exportes sucios (core), la API (procesador) y
Salesforce. `sim` sirve para los tests y para medir si el pipeline recuperó la verdad.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from andes_bank.config import ProjectConfig
from andes_bank.generator import entities, events

log = logging.getLogger(__name__)
TRUTH_TABLES = ("branches", "months", "customers", "accounts", "cards", "merchants", "authorizations", "settlements",
                "transactions", "balances")


def _save(df, cfg: ProjectConfig, name: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("sim", name))
    log.info("sim.%s escrita", name)


def generate_truth(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    """Genera todo el banco. Cada tabla se escribe y se vuelve a leer: así cada paso usa datos materializados
    (y no recalcula todo el linaje, que con 150 M de filas sería carísimo)."""
    t = lambda name: spark.table(cfg.table("sim", name))  # noqa: E731
    _save(entities.branches_df(spark), cfg, "branches")
    _save(entities.monthly_index_df(spark, cfg), cfg, "months")
    _save(entities.customers_df(spark, cfg), cfg, "customers")
    _save(entities.accounts_df(t("customers"), cfg), cfg, "accounts")
    _save(entities.cards_df(t("customers"), t("accounts"), cfg), cfg, "cards")
    _save(entities.merchants_df(spark, cfg), cfg, "merchants")
    _save(events.authorizations_df(t("cards"), t("merchants"), t("months"), cfg), cfg, "authorizations")
    _save(events.settlements_df(t("authorizations"), t("months"), cfg), cfg, "settlements")
    _save(events.transactions_df(t("customers"), t("accounts"), t("authorizations"), t("months"), cfg), cfg, "transactions")
    _save(events.balances_df(t("transactions"), t("accounts"), cfg), cfg, "balances")
    return {name: t(name).count() for name in TRUTH_TABLES}
