"""Baja de la biblioteca pública de Excalidraw (https://libraries.excalidraw.com) los íconos que usan los diagramas y
los guarda en scripts/diagramas/iconos.json (sólo el dibujo, sin el texto con el nombre).

    python scripts/diagramas/bajar_iconos.py

Las bibliotecas son de sus autores y se publican con licencia MIT en
https://github.com/excalidraw/excalidraw-libraries; cada ícono guarda de qué biblioteca salió.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

BASE = "https://raw.githubusercontent.com/excalidraw/excalidraw-libraries/main/libraries/{}.excalidrawlib"
OUT = Path(__file__).resolve().parent / "iconos.json"

# clave en los diagramas: (biblioteca, nombre del ítem)
ICONOS = {
    "databricks": ("lukethorp/databricks-architecture-icons", "Databricks"),
    "jobs": ("lukethorp/databricks-architecture-icons", "Lakeflow Jobs"),
    "notebooks": ("lukethorp/databricks-architecture-icons", "Notebooks"),
    "unity_catalog": ("lukethorp/databricks-architecture-icons", "Unity Catalog"),
    "lakehouse": ("lukethorp/databricks-architecture-icons", "Lakehouse"),
    "dbsql": ("lukethorp/databricks-architecture-icons", "Databricks SQL"),
    "genie": ("lukethorp/databricks-architecture-icons", "Genie"),
    "streaming": ("lukethorp/databricks-architecture-icons", "Streaming"),
    "dashboards": ("lukethorp/databricks-architecture-icons", "Dashboards"),
    "azure": ("esteevens/logos", "Azure"),
    "blob": ("youritjang/azure-cloud-services", "Blob Storage"),
    "storage": ("7demonsrising/azure-storage", "Storage Accounts"),
    "function": ("7demonsrising/azure-compute", "Function App"),
    "key_vault": ("youritjang/azure-cloud-services", "Key Vault"),
    "github": ("devdaejungyoon/github-actions", "GitHub"),
    "github_actions": ("devdaejungyoon/github-actions", "GitHub Actions"),
    "delta": ("chuqbach/data-platform", "Delta Lake"),
    "spark": ("chuqbach/data-platform", "Spark"),
    "parquet": ("chuqbach/data-platform", "Parquet"),
    "python": ("pclainchard/it-logos", "Python"),
    "database": ("mateuszbaransanok/it-icons", "Database"),
    "user": ("mateuszbaransanok/it-icons", "User"),
    "server": ("mateuszbaransanok/it-icons", "Server"),
    "document": ("mateuszbaransanok/it-icons", "Document"),
    "internet": ("mateuszbaransanok/it-icons", "Internet"),
    "key": ("mateuszbaransanok/it-icons", "Key"),
    "error": ("mateuszbaransanok/it-icons", "Error"),
    "bar_chart": ("mateuszbaransanok/it-icons", "Bar chart"),
    "settings": ("mateuszbaransanok/it-icons", "Settings"),
}


def main() -> None:
    libs: dict[str, list] = {}
    out = {}
    for key, (lib, name) in ICONOS.items():
        if lib not in libs:
            with urllib.request.urlopen(BASE.format(lib), timeout=60) as r:
                libs[lib] = json.load(r)["libraryItems"]
        item = next(i for i in libs[lib] if i["name"] == name)
        # fuera el texto con el nombre y los marcos invisibles (opacidad 0), que agrandan la caja del ícono
        elements = [e for e in item["elements"]
                    if e["type"] != "text" and not e.get("isDeleted") and e.get("opacity", 100) > 0]
        for e in elements:                    # 2 decimales alcanzan y el archivo pesa la mitad
            for k in ("x", "y", "width", "height"):
                e[k] = round(e[k], 2)
            if e.get("points"):
                e["points"] = [[round(px, 2), round(py, 2)] for px, py in e["points"]]
        out[key] = {"biblioteca": lib, "nombre": name, "elements": elements}
        print(f"✔ {key:15} {lib} · {name} ({len(elements)} elementos)")
    OUT.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
