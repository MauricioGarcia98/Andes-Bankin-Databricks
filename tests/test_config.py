from andes_bank.config import ProjectConfig


def test_nombres_y_rutas():
    c = ProjectConfig(storage_account="andesbankdev")
    assert c.table("bronze", "core_transactions") == "andes_bank.bronze.core_transactions"
    assert c.landing_path("core", "transactions") == "/Volumes/andes_bank/landing/raw/core/transactions"
    assert c.blob_url("core") == "https://andesbankdev.blob.core.windows.net/core"


def test_escala():
    assert ProjectConfig(scale_factor=0.1).n_customers == 50_000
    assert ProjectConfig(scale_factor=1).n_customers == 500_000
    assert ProjectConfig(scale_factor=0.0001).n_merchants == 200


def test_setup_crea_las_tablas_de_ops(spark, tmp_path):
    from dataclasses import replace

    from andes_bank.config import local_config
    from andes_bank.setup import create_namespace, create_ops_tables

    cfg = replace(local_config(str(tmp_path)), schema_prefix="ops_t_")
    create_namespace(spark, cfg)
    created = create_ops_tables(spark, cfg)
    assert cfg.table("ops", "dq_results") in created and cfg.table("ops", "api_reconciliation") in created
    assert create_ops_tables(spark, cfg) == []          # idempotente
