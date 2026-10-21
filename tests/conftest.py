import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from andes_bank.config import local_config  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from andes_bank.utils.spark import build_local_spark

    wh = tmp_path_factory.mktemp("warehouse")
    session = build_local_spark("andes-bank-tests", warehouse_dir=str(wh))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def cfg(tmp_path, spark):
    """Config aislada por test: esquemas con prefijo único para no pisarse."""
    from andes_bank.setup import create_namespace

    prefix = f"t{abs(hash(str(tmp_path))) % 10**8}_"
    c = replace(local_config(str(tmp_path / "volume")), schema_prefix=prefix)
    create_namespace(spark, c)
    return c
