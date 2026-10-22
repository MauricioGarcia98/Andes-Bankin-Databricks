#!/usr/bin/env bash
# Etapa 08 · Credenciales de Salesforce → secret scope `andes_bank` de Databricks.
# Lee las variables de entorno SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN y SF_LOGIN_URL (opcional; por defecto
# https://login.salesforce.com). Nunca las escribas en el repo ni las pegues en un chat.
set -euo pipefail
: "${SF_USERNAME:?Falta SF_USERNAME}" "${SF_PASSWORD:?Falta SF_PASSWORD}" "${SF_SECURITY_TOKEN:?Falta SF_SECURITY_TOKEN}"
SCOPE=andes_bank
databricks secrets create-scope "$SCOPE" 2>/dev/null || true
put() { databricks secrets put-secret "$SCOPE" "$1" --string-value "$2"; }
put sf_username "$SF_USERNAME"
put sf_password "$SF_PASSWORD"
put sf_security_token "$SF_SECURITY_TOKEN"
put sf_login_url "${SF_LOGIN_URL:-https://login.salesforce.com}"
echo "✔ credenciales de Salesforce en el scope '$SCOPE'"
