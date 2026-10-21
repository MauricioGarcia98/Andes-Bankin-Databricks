"""Corre en el SQL warehouse TODO el SQL de los Genie Agents (ejemplos y benchmarks) antes de desplegarlos.

    python scripts/validate_genie_sql.py            # todos
    python scripts/validate_genie_sql.py --show     # además imprime la primera fila de cada benchmark

Un benchmark que no corre es un benchmark que Genie "falla" siempre: mejor enterarse acá.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="andes_bank")
    ap.add_argument("--warehouse", default="Serverless Starter Warehouse")
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    w = WorkspaceClient()
    wh = next(x.id for x in w.warehouses.list() if x.name == args.warehouse)
    failures = 0
    for path in sorted((ROOT / "genie" / "src").glob("*.yml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        items = [("ejemplo", e["question"], e["sql"]) for e in spec.get("examples", [])] + \
                [("benchmark", b["q"], b["sql"]) for b in spec.get("benchmarks", [])]
        for kind, q, sql in items:
            r = w.statement_execution.execute_statement(statement=sql.replace("${catalog}", args.catalog), warehouse_id=wh,
                                                        wait_timeout="50s")
            ok = r.status.state.value == "SUCCEEDED"
            failures += not ok
            first = (r.result.data_array or [[None]])[0] if ok and r.result else None
            msg = (r.status.error.message[:200] if r.status.error else "") if not ok else (str(first)[:120] if args.show else "")
            print(f"{'OK ' if ok else 'ERR'} {path.stem} {kind:9s} {q[:70]:70s} {msg}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
