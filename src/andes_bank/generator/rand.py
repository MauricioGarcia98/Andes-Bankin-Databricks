"""Aleatoriedad determinística y distribuida para generar datos con Spark.

(Tomado de Andes Viento Austral: es la misma técnica.)

¿Por qué no `F.rand()`? Porque su resultado depende de cómo estén
particionados los datos: si el cluster cambia, cambian los datos. Usando
`xxhash64(id, salt)` cada fila produce SIEMPRE el mismo valor, sin importar
cuántos executors haya. Eso hace al generador reproducible a cualquier escala.
"""

from __future__ import annotations

import random
import zlib
from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F

_MOD = 2_147_483_647  # primo de Mersenne 2^31-1


def uniform(*cols: Column | str, salt: str) -> Column:
    """Número uniforme en [0, 1) derivado de las columnas + una sal."""
    parts = [F.col(c) if isinstance(c, str) else c for c in cols]
    return F.pmod(F.xxhash64(*parts, F.lit(salt)), F.lit(_MOD)) / F.lit(float(_MOD))


def uniform_sql(cols_sql: str, salt: str) -> str:
    """Versión SQL de `uniform` para usar dentro de lambdas (transform, filter...)."""
    return f"(pmod(xxhash64({cols_sql}, '{salt}'), {_MOD}) / {float(_MOD)})"


def int_between(u: Column, low: int, high: int) -> Column:
    """Entero uniforme en [low, high]."""
    return (F.floor(u * (high - low + 1)) + low).cast("int")


def pick(u: Column, values: Sequence) -> Column:
    """Elige un valor de la lista con probabilidad uniforme."""
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(u * len(values)) + 1).cast("int"))


def weighted_pick(u: Column, values: Sequence, weights: Sequence[float]) -> Column:
    """Elige un valor según pesos (no hace falta que sumen 1)."""
    total = float(sum(weights))
    acc = 0.0
    expr = None
    for value, weight in zip(values, weights, strict=True):
        acc += weight / total
        cond = u < F.lit(acc)
        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
    return expr.otherwise(F.lit(values[-1]))


def zipf_index(u: Column, n: Column | int, skew: float) -> Column:
    """Índice 1..n con sesgo tipo ley de potencias (pocos elementos muy populares).

    skew=1 es uniforme; skew>1 concentra la probabilidad en los primeros ids.
    Genera *data skew* realista: un puñado de clientes y productos acumula
    gran parte de las ventas (útil para aprender AQE y salting).
    """
    n_col = F.lit(n) if isinstance(n, int) else n
    return (F.floor(F.pow(u, F.lit(skew)) * n_col) + 1).cast("long")


def normal(u1: Column, u2: Column) -> Column:
    """Normal estándar a partir de dos uniformes (Box-Muller)."""
    return F.sqrt(F.lit(-2.0) * F.log(F.greatest(u1, F.lit(1e-12)))) * F.cos(F.lit(6.283185307179586) * u2)


def lognormal(u1: Column, u2: Column, median: float | Column, sigma: float) -> Column:
    """Monto con cola larga: la mayoría cerca de la mediana, algunos muy grandes (como los montos reales)."""
    med = F.lit(median) if isinstance(median, (int, float)) else median
    return med * F.exp(F.lit(sigma) * normal(u1, u2))


def poisson_approx(u1: Column, u2: Column, rate: float | Column) -> Column:
    """Cantidad de eventos con media `rate` (aproximación normal, nunca negativa)."""
    r = F.lit(rate) if isinstance(rate, (int, float)) else rate
    return F.greatest(F.lit(0), F.round(r + F.sqrt(r) * normal(u1, u2))).cast("int")


# ----------------------------------------------------------------------------- driver
# Algunas tablas son chicas y SECUENCIALES (un proceso AR(1) de clima, la suciedad que se
# acumula día a día en los paneles). Esas se calculan en el driver con un `random.Random`
# sembrado con un hash ESTABLE (crc32): `hash()` de Python cambia entre procesos.


def prng(seed: int, *keys) -> random.Random:
    """Generador pseudoaleatorio determinístico para una combinación de claves."""
    key = "|".join(str(k) for k in keys).encode()
    return random.Random(seed * 1_000_003 + zlib.crc32(key))
