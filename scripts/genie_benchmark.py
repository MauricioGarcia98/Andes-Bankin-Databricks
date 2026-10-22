"""Mide cuánto acierta cada Genie Agent: le hace cada benchmark de genie/src/*.yml y compara su respuesta con la
del SQL esperado, corrido en el warehouse. Criterio del MVP: al menos 80 % por agent (docs/00).

    python scripts/genie_benchmark.py                          # los 4 agents
    python scripts/genie_benchmark.py --only 02_tarjetas_fraude --limit 3

Una respuesta de Genie ACIERTA si cada valor del resultado esperado aparece en el resultado de Genie: los números con
una tolerancia de 0,5 % (y también si uno es el otro × 100, por los porcentajes), los textos sin importar mayúsculas.
Genie puede devolver columnas o filas de más (por ejemplo, el nombre al lado del número): eso no lo penaliza.
El detalle de cada pregunta queda en genie/benchmarks_<target>.json.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from genie_ask import ask  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TARGET_PCT = 80


def _num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _same(expected, got) -> bool:
    a, b = _num(expected), _num(got)
    if a is not None and b is not None:
        return any(abs(a - x) <= max(0.01, abs(a) * 0.005) for x in (b, b * 100, b / 100))
    return str(expected).strip().lower() == str(got).strip().lower()


def matches(expected_rows: list[list], genie_rows: list[list]) -> bool:
    got = [v for row in genie_rows for v in row if v is not None]
    want = [v for row in expected_rows[:20] for v in row if v is not None]
    if not want:                                   # el esperado no trae nada: acierta si Genie tampoco
        return not got
    return all(any(_same(w, g) for g in got) for w in want)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="dev")
    ap.add_argument("--catalog", default="andes_bank")
    ap.add_argument("--warehouse", default="Serverless Starter Warehouse")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--limit", type=int, help="Máximo de benchmarks por agent (para probar rápido)")
    ap.add_argument("--pause", type=float, default=4.0, help="Segundos entre preguntas (cuota de la API de Genie)")
    args = ap.parse_args()

    w = WorkspaceClient()
    wh = next(x.id for x in w.warehouses.list() if x.name == args.warehouse)
    report: dict[str, dict] = {}
    for path in sorted((ROOT / "genie" / "src").glob("*.yml")):
        if args.only and path.stem not in args.only:
            continue
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        rows = []
        for b in spec.get("benchmarks", [])[: args.limit]:
            r = w.statement_execution.execute_statement(statement=b["sql"].replace("${catalog}", args.catalog),
                                                        warehouse_id=wh, wait_timeout="50s")
            expected = (r.result.data_array or []) if r.result else []
            try:
                g = ask(path.stem, b["q"], args.target)
            except Exception as exc:  # cuota, red: cuenta como fallo, no frena la corrida
                g = {"status": "ERROR", "error": str(exc)[:300], "rows": None, "sql": None}
            # para comparar hacen falta TODAS las filas de Genie, no las 5 que muestra genie_ask
            ok = g["status"] == "COMPLETED" and matches(expected, g.get("all_rows") or g.get("rows") or [])
            rows.append({"q": b["q"], "ok": ok, "esperado": expected[:5], "genie": (g.get("rows") or [])[:5],
                         "genie_sql": g.get("sql"), "status": g["status"], "error": g.get("error")})
            print(f"{'✔' if ok else '✘'} {path.stem} {b['q'][:80]}", flush=True)
            time.sleep(args.pause)
        n_ok = sum(r["ok"] for r in rows)
        pct = round(100 * n_ok / len(rows), 1) if rows else 0.0
        report[path.stem] = {"aciertos": n_ok, "total": len(rows), "pct": pct, "detalle": rows}
        print(f"== {path.stem}: {n_ok}/{len(rows)} = {pct} % {'✔' if pct >= TARGET_PCT else '✘ (objetivo 80 %)'}",
              flush=True)
    out = ROOT / "genie" / f"benchmarks_{args.target}.json"
    previous = json.loads(out.read_text()) if out.exists() and args.only else {}
    out.write_text(json.dumps({**previous, **report}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"detalle en {out.relative_to(ROOT)}")
    raise SystemExit(0 if all(v["pct"] >= TARGET_PCT for v in report.values()) else 1)


if __name__ == "__main__":
    main()
