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
