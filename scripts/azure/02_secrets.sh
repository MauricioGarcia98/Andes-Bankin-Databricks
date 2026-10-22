#!/usr/bin/env bash
# Etapa 05 · Credenciales: SAS de Blob (escritura para los "sistemas origen", lectura para el lakehouse) y el
# client id/secret de la API. Se guardan en:
#   * la configuración de la Function App (lo que la API necesita para validar)
#   * el secret scope `andes_bank` de Databricks (lo que el lakehouse necesita para llamar)
# Nunca en el repo. Correr de nuevo ROTA las credenciales (y hay que volver a desplegar nada: sólo se leen al usar).
set -euo pipefail
source "$(dirname "$0")/env.sh"
az_login
command -v databricks >/dev/null || { echo "Falta la Databricks CLI"; exit 1; }

KEY="$(az storage account keys list -n "$STORAGE" -g "$RG" --query '[0].value' -o tsv)"
EXPIRY="$(date -u -d '+365 days' '+%Y-%m-%dT00:00Z')"
# SAS de cuenta (servicio blob). Escritura: crear, escribir, listar, leer. Lectura: listar y leer.
SAS_WRITE="$(az storage account generate-sas --account-name "$STORAGE" --account-key "$KEY" --services b \
  --resource-types co --permissions rwlac --expiry "$EXPIRY" --https-only -o tsv)"
SAS_READ="$(az storage account generate-sas --account-name "$STORAGE" --account-key "$KEY" --services b \
  --resource-types co --permissions rl --expiry "$EXPIRY" --https-only -o tsv)"

CLIENT_ID="lakehouse-andes-bank"
CLIENT_SECRET="$(openssl rand -hex 24)"
SIGNING_KEY="$(openssl rand -hex 32)"
az functionapp config appsettings set -n "$FUNC_APP" -g "$RG" -o none --settings \
  "API_CLIENT_ID=$CLIENT_ID" "API_CLIENT_SECRET=$CLIENT_SECRET" "TOKEN_SIGNING_KEY=$SIGNING_KEY"

SCOPE=andes_bank
databricks secrets create-scope "$SCOPE" 2>/dev/null || true
put() { databricks secrets put-secret "$SCOPE" "$1" --string-value "$2"; }
put storage_account "$STORAGE"
put blob_sas_write "$SAS_WRITE"
put blob_sas_read "$SAS_READ"
put card_api_base_url "https://$FUNC_APP.azurewebsites.net/api"
put card_api_client_id "$CLIENT_ID"
put card_api_client_secret "$CLIENT_SECRET"
echo "✔ secretos en el scope '$SCOPE':"; databricks secrets list-secrets "$SCOPE" -o json | python3 -c "import sys,json;[print('  -',s['key']) for s in json.load(sys.stdin)]"
