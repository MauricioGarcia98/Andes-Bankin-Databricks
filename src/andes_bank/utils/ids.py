"""Identificadores argentinos con su dígito verificador real: CUIT/CUIL y CBU.

Hay dos versiones de cada función:
  * Python puro (`cuit_check_digit`, `is_valid_cbu`...): para tests y para entender el algoritmo.
  * Columnas de Spark (`cuit_col`, `cbu_valid_col`...): para generar y validar millones de filas sin UDFs
    (una UDF de Python saca los datos de la JVM fila por fila y es 10-100 veces más lenta).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

CUIT_WEIGHTS = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)
CBU_BLOCK1_WEIGHTS = (7, 1, 3, 9, 7, 1, 3)
CBU_BLOCK2_WEIGHTS = (3, 9, 7, 1, 3, 9, 7, 1, 3, 9, 7, 1, 3)
BANK_CODE = "998"  # código BCRA ficticio de Andes Bank (no existe en el sistema real)


# ------------------------------------------------------------------ CUIT/CUIL
def cuit_check_digit(first10: str) -> int:
    """Dígito verificador (módulo 11) de los primeros 10 dígitos. Devuelve 10 si no hay dígito válido."""
    total = sum(int(d) * w for d, w in zip(first10, CUIT_WEIGHTS, strict=True))
    dv = 11 - total % 11
    return 0 if dv == 11 else dv


def build_cuit(dni: int, female: bool) -> str:
    """CUIL de una persona humana: 20 (varón) / 27 (mujer); si el dígito da 10, AFIP usa prefijo 23."""
    prefix = "27" if female else "20"
    body = f"{prefix}{dni:08d}"
    dv = cuit_check_digit(body)
    if dv == 10:
        body = f"23{dni:08d}"
        dv = 4 if female else 9
    return f"{body}{dv}"


def is_valid_cuit(value: str | None) -> bool:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) != 11 or digits[:2] not in {"20", "23", "24", "27", "30", "33", "34"}:
        return False
    return cuit_check_digit(digits[:10]) == int(digits[10])


def _weighted_sum(s: Column, weights: tuple[int, ...], offset: int = 0) -> Column:
    return sum(
        (F.substring(s, offset + i + 1, 1).cast("int") * F.lit(w) for i, w in enumerate(weights)),
        F.lit(0),
    )


def cuit_dv_col(first10: Column) -> Column:
    dv = F.lit(11) - _weighted_sum(first10, CUIT_WEIGHTS) % 11
    return F.when(dv == 11, F.lit(0)).otherwise(dv)


def cuit_col(dni: Column, female: Column) -> Column:
    """Versión Spark de `build_cuit` (dni entero, female booleano)."""
    dni8 = F.lpad(dni.cast("string"), 8, "0")
    body = F.concat(F.when(female, F.lit("27")).otherwise(F.lit("20")), dni8)
    dv = cuit_dv_col(body)
    return F.when(dv == 10, F.concat(F.lit("23"), dni8, F.when(female, F.lit("4")).otherwise(F.lit("9")))) \
        .otherwise(F.concat(body, dv.cast("string")))


def cuit_company_col(n8: Column) -> Column:
    """CUIT de una persona jurídica: 30 + 8 dígitos + DV (si da 10, AFIP usa 33 y DV 9)."""
    num8 = F.lpad(n8.cast("string"), 8, "0")
    body = F.concat(F.lit("30"), num8)
    dv = cuit_dv_col(body)
    return F.when(dv == 10, F.concat(F.lit("33"), num8, F.lit("9"))).otherwise(F.concat(body, dv.cast("string")))


def cuit_valid_col(value: Column) -> Column:
    """True si el CUIT (con o sin guiones) tiene 11 dígitos, prefijo válido y dígito verificador correcto."""
    d = F.regexp_replace(value, r"[^0-9]", "")
    prefix_ok = F.substring(d, 1, 2).isin("20", "23", "24", "27", "30", "33", "34")
    return F.coalesce(
        (F.length(d) == 11) & prefix_ok & (cuit_dv_col(d) == F.substring(d, 11, 1).cast("int")),
        F.lit(False),
    )


# ------------------------------------------------------------------------ CBU
def _cbu_dv(digits: str, weights: tuple[int, ...]) -> int:
    total = sum(int(d) * w for d, w in zip(digits, weights, strict=True))
    return (10 - total % 10) % 10


def build_cbu(branch: int, account_number: int, bank: str = BANK_CODE) -> str:
    """CBU de 22 dígitos: [banco 3 + sucursal 4 + DV] [cuenta 13 + DV]."""
    block1 = f"{bank}{branch:04d}"
    block2 = f"{account_number:013d}"
    return f"{block1}{_cbu_dv(block1, CBU_BLOCK1_WEIGHTS)}{block2}{_cbu_dv(block2, CBU_BLOCK2_WEIGHTS)}"


def is_valid_cbu(value: str | None) -> bool:
    v = value or ""
    if len(v) != 22 or not v.isdigit():
        return False
    return _cbu_dv(v[:7], CBU_BLOCK1_WEIGHTS) == int(v[7]) and _cbu_dv(v[8:21], CBU_BLOCK2_WEIGHTS) == int(v[21])


def _cbu_dv_col(s: Column, weights: tuple[int, ...], offset: int) -> Column:
    return (F.lit(10) - _weighted_sum(s, weights, offset) % 10) % 10


def cbu_col(branch: Column, account_number: Column, bank: str = BANK_CODE) -> Column:
    block1 = F.concat(F.lit(bank), F.lpad(branch.cast("string"), 4, "0"))
    block2 = F.lpad(account_number.cast("string"), 13, "0")
    return F.concat(
        block1, _cbu_dv_col(block1, CBU_BLOCK1_WEIGHTS, 0).cast("string"),
        block2, _cbu_dv_col(block2, CBU_BLOCK2_WEIGHTS, 0).cast("string"),
    )


def cbu_valid_col(value: Column) -> Column:
    ok = (
        (F.length(value) == 22)
        & value.rlike(r"^[0-9]+$")
        & (_cbu_dv_col(value, CBU_BLOCK1_WEIGHTS, 0) == F.substring(value, 8, 1).cast("int"))
        & (_cbu_dv_col(value, CBU_BLOCK2_WEIGHTS, 8) == F.substring(value, 22, 1).cast("int"))
    )
    return F.coalesce(ok, F.lit(False))
