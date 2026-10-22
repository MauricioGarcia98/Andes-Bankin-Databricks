"""Crea o actualiza los Genie Agents (ex Genie spaces) desde genie/agents/*.json.

    pip install "databricks-sdk>=0.60"
    python scripts/deploy_genie.py --target dev            # todos los agents
    python scripts/deploy_genie.py --target prod --only 03_finanzas_tesoreria

Idempotente: busca el space por título; si existe hace PATCH, si no, POST. Las credenciales
salen de DATABRICKS_HOST/DATABRICKS_TOKEN o de ~/.databrickscfg (nunca se imprimen).
Usa el cliente del SDK (`WorkspaceClient().api_client`) para hablar con la API REST de Genie:
  POST  /api/2.0/genie/spaces                        crear (title, warehouse_id, serialized_space)
  PATCH /api/2.0/genie/spaces/{space_id}             actualizar
  GET   /api/2.0/genie/spaces/{id}?include_serialized_space=true   exportar
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]
CATALOGS = {"dev": "andes_bank", "prod": "andes_bank"}


def find_warehouse(w: WorkspaceClient, name: str) -> str:
    for wh in w.warehouses.list():
        if wh.name == name:
            return wh.id
    raise SystemExit(f"No encontré el warehouse '{name}'")


def list_spaces(w: WorkspaceClient) -> dict[str, str]:
    out, token = {}, None
    while True:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_token": token} if token else None)
        for s in resp.get("spaces", []):
            out[s["title"]] = s["space_id"]
        token = resp.get("next_page_token")
        if not token:
            return out


def render(doc: dict, catalog: str, target: str) -> tuple[str, str, str]:
    space = json.dumps(doc["serialized_space"], ensure_ascii=False).replace("${catalog}", catalog)
    title = doc["title"] if target == "prod" else f"{doc['title']} [{target}]"
    return title, doc["description"], space


UNSUPPORTED_PARAM_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "INTERVAL"}


def check_functions(w: WorkspaceClient, space: str) -> None:
    """Falla antes de desplegar si una función confiable tiene parámetros que Genie no acepta.

    Genie acepta el deploy igual, pero después rechaza TODAS las preguntas del agent
    (INVALID_CERTIFIED_ANSWER_FUNCTION_EXCEPTION). Lección de Andes Viento Austral (bitácora R11 y R13).
    """
    for f in json.loads(space).get("instructions", {}).get("sql_functions", []):
        info = w.functions.get(f["identifier"])
        bad = [p.name for p in (info.input_params.parameters if info.input_params else [])
               if str(p.type_name.value if p.type_name else "").upper() in UNSUPPORTED_PARAM_TYPES]
        if bad:
            raise SystemExit(f"{f['identifier']}: parámetros {bad} no admitidos por Genie (usá STRING 'yyyy-MM-dd'). "
                             "¿Te olvidaste de 'databricks bundle deploy' antes de correr el job?")


def deploy(target: str, only: list[str] | None, warehouse_name: str, catalog: str | None) -> dict[str, str]:
    w = WorkspaceClient()
    wh = find_warehouse(w, warehouse_name)
    existing = list_spaces(w)
    deployed = {}
    for path in sorted((ROOT / "genie" / "agents").glob("*.json")):
        if only and path.stem not in only:
            continue
        doc = json.loads(path.read_text(encoding="utf-8"))
        title, description, space = render(doc, catalog or CATALOGS[target], target)
        check_functions(w, space)
        body = {"title": title, "description": description, "warehouse_id": wh, "serialized_space": space}
        if title in existing:
            sid = existing[title]
            w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{sid}", body=body)
            action = "actualizado"
        else:
            sid = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
            action = "creado"
        deployed[path.stem] = sid
        print(f"{action:11s} {title} -> {w.config.host.rstrip('/')}/genie/rooms/{sid}")
    out = ROOT / "genie" / f"deployed_{target}.json"
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    deployed = {**previous, **deployed}  # con --only no se pierden los agents que no se tocaron
    try:
        out.write_text(json.dumps(deployed, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as exc:  # p. ej. desde un notebook en una carpeta de sólo lectura
        print(f"(no se pudo guardar {out.name}: {exc})")
    return deployed


def grant_app(app_name: str, spaces: dict[str, str], catalog: str, warehouse_id: str) -> None:
    """Da al service principal de la App permiso para usar los spaces y leer gold."""
    w = WorkspaceClient()
    sp = w.apps.get(app_name).service_principal_client_id
    for sid in spaces.values():
        w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{sid}",
                        body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_RUN"}]})
    stmts = [f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp}`"]
    for schema in ("gold", "semantic", "ops"):
        stmts += [f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {catalog}.{schema} TO `{sp}`"]
    for stmt in stmts:
        r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=warehouse_id, wait_timeout="30s")
        print(f"{r.status.state.value:10s} {stmt}")
    print(f"App {app_name}: CAN_RUN en {len(spaces)} spaces")


def export(space_id: str) -> dict:
    """Trae la configuración de un space (para versionar cambios hechos desde la UI)."""
    w = WorkspaceClient()
    resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}", query={"include_serialized_space": "true"})
    return json.loads(resp["serialized_space"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", choices=list(CATALOGS), default="dev")
    ap.add_argument("--catalog", help="Pisar el catálogo del target")
    ap.add_argument("--only", nargs="*", help="Slugs a desplegar (ej. 01_comercial_clientes)")
    ap.add_argument("--warehouse", default="Serverless Starter Warehouse")
    ap.add_argument("--export", metavar="SPACE_ID", help="En vez de desplegar, imprime el serialized_space")
    ap.add_argument("--grant-app", metavar="APP_NAME", help="Dar permisos a la App (ej. andes-bank-genie)")
    args = ap.parse_args()
    if args.export:
        print(json.dumps(export(args.export), ensure_ascii=False, indent=2))
    else:
        spaces = deploy(args.target, args.only, args.warehouse, args.catalog)
        if args.grant_app:
            wh = find_warehouse(WorkspaceClient(), args.warehouse)
            grant_app(args.grant_app, spaces, args.catalog or CATALOGS[args.target], wh)


if __name__ == "__main__":
    main()
