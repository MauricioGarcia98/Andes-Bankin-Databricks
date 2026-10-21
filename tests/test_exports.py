"""Los exportes tienen que estar sucios exactamente como dice el catálogo (docs/03)."""

import glob
import gzip
import os

import pytest

from andes_bank.generator import reference as ref
from andes_bank.generator.exports import EMPTY_FILE, LATE_FILE, export_all

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def exported(spark, truth):
    cfg, _ = truth
    return cfg, export_all(spark, cfg)


def _files(path, delivery):
    return sorted(glob.glob(os.path.join(path, f"delivery_date={delivery}", "*", "*.csv*")))


def test_formato_viejo_y_nuevo_del_core(exported):
    cfg, paths = exported
    old = _files(paths["core/movimientos"], "2023-06-15")[0]
    with open(old, encoding="latin1") as f:
        header = f.readline().strip()
    assert header.startswith("NRO_MOVIMIENTO;NRO_CUENTA;FECHA_HORA") and old.endswith(".csv")
    new = _files(paths["core/movimientos"], "2024-06-10")[0]
    with gzip.open(new, "rt") as f:
        header = f.readline().strip()
    assert header.startswith("movimiento_id,cuenta_id") and "canal_digital" not in header
    drift = _files(paths["core/movimientos"], "2025-06-10")[0]
    with gzip.open(drift, "rt") as f:
        assert f.readline().strip().endswith(",canal_digital")


def test_incidentes_de_entrega(exported):
    _, paths = exported
    late_from, late_to = LATE_FILE
    assert not os.path.exists(os.path.join(paths["core/movimientos"], f"delivery_date={late_from}"))
    assert os.path.isdir(os.path.join(paths["core/movimientos"], f"delivery_date={late_to}", f"file_date={late_from}"))
    assert not os.path.exists(os.path.join(paths["core/movimientos"], f"delivery_date={EMPTY_FILE}"))


def test_la_migracion_reenvia_todos_los_clientes(exported, spark):
    cfg, paths = exported
    mig = ref.CORE_MIGRATION_DATE.isoformat()
    df = spark.read.option("header", True).csv(os.path.join(paths["core/clientes"], f"delivery_date={mig}"))
    assert df.where("op = 'I'").count() > 0.8 * spark.table(cfg.table("sim", "customers")).count()


def test_api_v2_y_v1_se_solapan(exported, spark):
    _, paths = exported
    v2 = spark.read.json(paths["api_data/authorizations"])
    v1 = spark.read.json(paths["cards_legacy/autorizaciones"])
    overlap = v2.join(v1, v2.auth_id == v1.id_autorizacion)
    assert overlap.count() > 0                                     # abr-jun 2025 llegan por los dos caminos
    assert "wallet_type" in v2.columns and "monto_centavos" in v1.columns
