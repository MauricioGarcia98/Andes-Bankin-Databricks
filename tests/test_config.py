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


def test_ayer_en_hora_argentina_y_meses_despues_de_2026():
    from datetime import date, datetime, timezone

    from andes_bank.config import parse_end_date, yesterday_ar
    from andes_bank.generator.reference import monthly_index_rows

    # 22-oct 01:30 UTC todavía es 21-oct en Buenos Aires (UTC-3): ayer es el 20
    assert yesterday_ar(datetime(2026, 10, 22, 1, 30, tzinfo=timezone.utc)) == date(2026, 10, 20)
    assert parse_end_date("2026-10-20") == date(2026, 10, 20)
    assert parse_end_date("ayer") == yesterday_ar()
    rows = monthly_index_rows(date(2026, 11, 1), date(2027, 2, 1))     # el banco sigue vivo en 2027
    assert [r[0] for r in rows] == ["2026-11", "2026-12", "2027-01", "2027-02"] and rows[-1][2] == rows[-2][2]
