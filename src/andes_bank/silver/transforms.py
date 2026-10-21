"""Transformaciones de silver: cada función arregla UN problema del catálogo (docs/03) y es una expresión de Spark
(sin UDFs de Python), así corre igual de rápido con 17 o con 170 millones de filas.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

TZ = "America/Argentina/Buenos_Aires"

# patrones de PII que aparecen escritos a mano en campos libres (DAT-11)
EMAIL_RE = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_RE = r"\+?54[ -]?9?[ -]?\d{2,4}[ -]?\d{6,8}"

# códigos de respuesta de la v1 (2 dígitos) → códigos completos de la v2
RESPONSE_CODES_V1 = {
    "00": "00_APROBADA", "05": "05_NO_AUTORIZADA", "51": "51_FONDOS_INSUFICIENTES", "54": "54_TARJETA_VENCIDA",
    "55": "55_PIN_INCORRECTO", "59": "59_SOSPECHA_FRAUDE", "61": "61_EXCEDE_LIMITE",
}


def ar_number(col: Column) -> Column:
    """'1.234,56' → 1234.56 (DAT-02). También acepta '1234.56' (formato nuevo) sin romperlo.

    Regla: si tiene coma, el punto es separador de miles y la coma es decimal. Si no tiene coma, se usa tal cual.
    """
    s = F.trim(col.cast("string"))
    return F.when(s.contains(","), F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")) \
        .otherwise(s).cast("decimal(18,2)")


def parse_date(col: Column) -> Column:
    """Fecha en cualquiera de los formatos del banco: 'dd/MM/yyyy' (core viejo), 'yyyy-MM-dd' (nuevo), 'yyyyMMdd'."""
    s = F.trim(col)
    return F.coalesce(
        F.to_date(F.try_to_timestamp(s, F.lit("yyyy-MM-dd"))),
        F.to_date(F.try_to_timestamp(s, F.lit("dd/MM/yyyy"))),
        F.to_date(F.try_to_timestamp(s, F.lit("yyyyMMdd"))),
    )


def parse_local_ts(col: Column) -> Column:
    """Hora local argentina SIN offset (así la manda el core), en cualquiera de sus formatos (DAT-06)."""
    s = F.trim(col)
    return F.coalesce(
        F.try_to_timestamp(s, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(s, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(s, F.lit("dd/MM/yyyy HH:mm:ss")),
    )


def local_to_utc(col: Column) -> Column:
    return F.to_utc_timestamp(col, TZ)


def only_digits(col: Column) -> Column:
    """'20.345.678' → '20345678'; '20-12345678-6' → '20123456786'."""
    return F.regexp_replace(col, r"[^0-9]", "")


def normalize_name(col: Column) -> Column:
    """Para COMPARAR nombres entre sistemas: mayúsculas, sin acentos, sin dobles espacios (DAT-08)."""
    return F.trim(F.regexp_replace(F.upper(F.translate(col, "áéíóúüÁÉÍÓÚÜñÑ", "aeiouuAEIOUUnN")), r"\s+", " "))


def title_case(col: Column) -> Column:
    """'JUAN LUNA' → 'Juan Luna' (para mostrar; el sistema viejo de Cuyo cargaba todo en mayúsculas)."""
    return F.initcap(F.lower(col))


def redact_pii(col: Column) -> Column:
    """Saca emails y teléfonos escritos a mano en un campo libre (DAT-11)."""
    return F.regexp_replace(F.regexp_replace(col, EMAIL_RE, "[EMAIL]"), PHONE_RE, "[TELEFONO]")


def has_pii(col: Column) -> Column:
    return F.coalesce(col.rlike(EMAIL_RE) | col.rlike(PHONE_RE), F.lit(False))


def currency_from_code(col: Column) -> Column:
    """Core viejo: 1 = pesos, 2 = dólares."""
    return F.when(col == "1", "ARS").when(col == "2", "USD").otherwise(col)


def response_code_v1(col: Column) -> Column:
    m = F.create_map(*[x for k, v in RESPONSE_CODES_V1.items() for x in (F.lit(k), F.lit(v))])
    return F.coalesce(m[col], F.concat(col, F.lit("_DESCONOCIDO")))
