#!/usr/bin/env bash
# Etapa 06 · Despliega la API del procesador (azure/card_api) en la Function App (Flex Consumption).
# El build lo hace Azure (--build-remote): instala requirements.txt para Linux/Python 3.12.
set -euo pipefail
source "$(dirname "$0")/env.sh"
az_login
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PKG="$(mktemp -d)/card_api.zip"
(cd "$ROOT/azure/card_api" && zip -qr "$PKG" . -x "__pycache__/*" ".venv/*" "local.settings.json")
echo "▶ desplegando $(du -h "$PKG" | cut -f1) en $FUNC_APP"
az functionapp deployment source config-zip -g "$RG" -n "$FUNC_APP" --src "$PKG" --build-remote true -o none
echo "▶ probando /api/health"
for i in 1 2 3 4 5 6; do
  if curl -fsS "https://$FUNC_APP.azurewebsites.net/api/health"; then echo; echo "✔ API arriba"; exit 0; fi
  sleep 10
done
echo "✘ la API no respondió; mirá los logs con: az functionapp log tail -n $FUNC_APP -g $RG" >&2
exit 1
