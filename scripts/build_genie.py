"""Convierte genie/src/*.yml en genie/agents/*.json (el `serialized_space` versionado).

    python scripts/build_genie.py

Cada JSON tiene la forma que exporta/importa la API de Genie (version 2) con el catálogo
como placeholder `${catalog}`: scripts/deploy_genie.py lo reemplaza según el target.
Los `id` son hashes estables del contenido (32 hex): reconstruir no cambia nada si no
cambió el YAML, y así el diff en Git muestra sólo lo que de verdad se tocó.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC, OUT = ROOT / "genie" / "src", ROOT / "genie" / "agents"


def _id(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _fq(name: str) -> str:
    return name if name.startswith("${catalog}") else "${catalog}." + name


def _lines(text: str) -> list[str]:
    lines = text.strip().splitlines()
    return [ln + "\n" for ln in lines[:-1]] + lines[-1:]


def build_space(spec: dict, slug: str) -> dict:
    space = {
        "version": 2,
        "config": {"sample_questions": [{"id": _id(slug, "sq", q), "question": [q]}
                                        for q in spec.get("sample_questions", [])]},
        "data_sources": {
            "tables": [{"identifier": _fq(t)} for t in sorted(spec.get("tables", []) + spec.get("metric_views", []))],
        },
        "instructions": {
            "text_instructions": [{"id": _id(slug, "ti"), "content": _lines(spec["instructions"])}],
            "example_question_sqls": [{"id": _id(slug, "ex", e["question"]), "question": [e["question"]],
                                       "sql": _lines(e["sql"])} for e in spec.get("examples", [])],
            "sql_functions": [{"id": _id(slug, "fn", f), "identifier": _fq(f)} for f in sorted(spec.get("functions", []))],
        },
        "benchmarks": {"questions": [{"id": _id(slug, "bm", b["q"]), "question": [b["q"]],
                                      "answer": [{"format": "SQL", "content": _lines(b["sql"])}]}
                                     for b in spec.get("benchmarks", [])]},
    }
    # la API exige cada lista ordenada por id ("instructions.example_question_sqls must be sorted by id")
    for section, key in (("config", "sample_questions"), ("instructions", "text_instructions"),
                         ("instructions", "example_question_sqls"), ("instructions", "sql_functions"),
                         ("benchmarks", "questions")):
        space[section][key].sort(key=lambda x: x["id"])
    return space


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for path in sorted(SRC.glob("*.yml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        slug = path.stem
        doc = {"title": spec["title"], "description": spec["description"], "domain": spec["domain"],
               "users": spec["users"], "serialized_space": build_space(spec, slug)}
        (OUT / f"{slug}.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        n = len(spec.get("benchmarks", []))
        print(f"{slug}: {len(spec.get('examples', []))} ejemplos SQL, {len(spec.get('functions', []))} funciones, {n} benchmarks")
        assert n >= 15, f"{slug}: hacen falta 15+ benchmarks"


if __name__ == "__main__":
    main()
