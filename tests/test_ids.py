import pytest

from andes_bank.utils.ids import (
    build_cbu,
    build_cuit,
    cbu_col,
    cbu_valid_col,
    cuit_col,
    cuit_valid_col,
    is_valid_cbu,
    is_valid_cuit,
)


def test_cuit_conocido():
    # ejemplo de manual: 20-12345678-6
    assert build_cuit(12345678, female=False) == "20123456786"
    assert is_valid_cuit("20-12345678-6")
    assert not is_valid_cuit("20-12345678-5")
    assert not is_valid_cuit("2012345678")


@pytest.mark.parametrize("dni", [1, 9_999_999, 23_456_789, 40_111_222, 45_999_999])
def test_cuit_generado_siempre_valido(dni):
    assert is_valid_cuit(build_cuit(dni, female=True))
    assert is_valid_cuit(build_cuit(dni, female=False))


def test_cbu():
    cbu = build_cbu(12, 345678)
    assert len(cbu) == 22 and is_valid_cbu(cbu)
    assert not is_valid_cbu(cbu[:-1] + str((int(cbu[-1]) + 1) % 10))
    assert not is_valid_cbu(cbu[:21])


@pytest.mark.spark
def test_columnas_spark_igual_que_python(spark):
    from pyspark.sql import functions as F

    rows = [(d, f, b, a) for d, f, b, a in [(12345678, False, 12, 345678), (30111222, True, 7, 1), (5, True, 9999, 99)]]
    df = spark.createDataFrame(rows, "dni long, female boolean, branch int, acct long")
    out = df.select(
        cuit_col(F.col("dni"), F.col("female")).alias("cuit"),
        cbu_col(F.col("branch"), F.col("acct")).alias("cbu"),
        "dni", "female", "branch", "acct",
    ).withColumn("cuit_ok", cuit_valid_col(F.col("cuit"))).withColumn("cbu_ok", cbu_valid_col(F.col("cbu"))).collect()
    for r in out:
        assert r.cuit == build_cuit(r.dni, r.female)
        assert r.cbu == build_cbu(r.branch, r.acct)
        assert r.cuit_ok and r.cbu_ok
