"""Le pregunta algo a un Genie Agent por la Conversation API y muestra la respuesta, el SQL y las primeras filas.

    python scripts/genie_ask.py 01_comercial_clientes "¿Cuántos clientes tenemos por segmento?"
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]


def ask(slug: str, question: str, target: str = "dev", timeout: int = 180) -> dict:
    w = WorkspaceClient()
    space = json.loads((ROOT / "genie" / f"deployed_{target}.json").read_text())[slug]
    msg = w.api_client.do("POST", f"/api/2.0/genie/spaces/{space}/start-conversation", body={"content": question})
    conv, mid = msg["conversation_id"], msg["message_id"]
    t0 = time.time()
    while True:
        m = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space}/conversations/{conv}/messages/{mid}")
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED") or time.time() - t0 > timeout:
            break
        time.sleep(3)
    out = {"status": m["status"], "text": None, "sql": None, "rows": None, "all_rows": None}
    for a in m.get("attachments") or []:
        if a.get("text"):
            out["text"] = a["text"].get("content")
        if a.get("query"):
            out["sql"] = a["query"].get("query")
            r = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space}/conversations/{conv}/messages/{mid}"
                                       f"/attachments/{a['attachment_id']}/query-result")
            out["all_rows"] = (r.get("statement_response", {}).get("result", {}) or {}).get("data_array", []) or []
            out["rows"] = out["all_rows"][:5]
            out["text"] = out["text"] or a["query"].get("description")
    if m.get("error"):
        out["error"] = m["error"]
    return out


if __name__ == "__main__":
    res = ask(sys.argv[1], sys.argv[2])
    res.pop("all_rows", None)
    print(json.dumps(res, ensure_ascii=False, indent=2))
