"""Corre los archivos de `sql/` (gobierno y capa semántica) con las variables del proyecto.

    ${catalog} ${bronze} ${silver} ${gold} ${semantic} ${ops}   → nombres reales según la config

Las sentencias se separan por `;` al final de línea, salvo dentro de un bloque `$$ ... $$` (el YAML de las metric views).
"""

from __future__ import annotations

import os
import re

from pyspark.sql import SparkSession

from andes_bank.config import ProjectConfig

SQL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "sql"))


def render(text: str, cfg: ProjectConfig) -> str:
    values = {"catalog": cfg.catalog, **{layer: cfg.schema(layer) for layer in ("bronze", "silver", "gold", "semantic", "ops")}}
    return re.sub(r"\$\{(\w+)\}", lambda m: values.get(m.group(1), m.group(0)), text)


def split_statements(text: str) -> list[str]:
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    out, cur, in_block = [], [], False
    for ln in lines:
        cur.append(ln)
        if ln.count("$$") % 2 == 1:
            in_block = not in_block
        if not in_block and ln.rstrip().endswith(";"):
            stmt = "\n".join(cur).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            cur = []
    rest = "\n".join(cur).strip().rstrip(";").strip()
    if rest:
        out.append(rest)
    return out


def run_sql_file(spark: SparkSession, cfg: ProjectConfig, relative_path: str) -> int:
    with open(os.path.join(SQL_DIR, relative_path), encoding="utf-8") as fh:
        statements = split_statements(render(fh.read(), cfg))
    for i, stmt in enumerate(statements, 1):
        try:
            spark.sql(stmt)
        except Exception as exc:
            raise RuntimeError(f"{relative_path} sentencia {i}: {str(exc)[:500]}\n---\n{stmt[:400]}") from exc
    return len(statements)


def run_folder(spark: SparkSession, cfg: ProjectConfig, folder: str) -> dict[str, int]:
    files = sorted(f for f in os.listdir(os.path.join(SQL_DIR, folder)) if f.endswith(".sql"))
    return {f: run_sql_file(spark, cfg, f"{folder}/{f}") for f in files}
