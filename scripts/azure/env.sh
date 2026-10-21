#!/usr/bin/env bash
# Variables comunes de los scripts de Azure. Se "importan" con: source scripts/azure/env.sh
#
# Los nombres de Storage y de la Function App son GLOBALES en Azure (únicos en el mundo): les agregamos un sufijo
# derivado de tu suscripción, así cada persona que reconstruya el proyecto obtiene nombres propios y repetibles.
set -euo pipefail

: "${AZURE_SUBSCRIPTION_ID:?Falta AZURE_SUBSCRIPTION_ID}"
export RG="${ANDES_RG:-rg-andes-bank}"
export LOCATION="${ANDES_LOCATION:-eastus2}"
SUFFIX="$(echo -n "$AZURE_SUBSCRIPTION_ID" | sha256sum | cut -c1-6)"
export SUFFIX
export STORAGE="andesbank${SUFFIX}"            # datos de los sistemas origen (core, procesador, espejo FX)
export FUNC_STORAGE="andesfn${SUFFIX}"         # almacenamiento interno de la Function App (código desplegado)
export FUNC_APP="andes-card-api-${SUFFIX}"     # la "API del procesador de tarjetas" + espejo del BCRA
export CONTAINERS="core cards-legacy api-data fx-mirror"
export TAGS="proyecto=andes-bank entorno=dev dueno=datos"

az_login() {
  # En la nube (CI o este entorno) entra con el service principal; en tu máquina ya estás logueado con `az login`.
  if [[ -n "${AZURE_CLIENT_ID:-}" && -n "${AZURE_CLIENT_SECRET:-}" ]]; then
    az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" -o none
  fi
  az account set -s "$AZURE_SUBSCRIPTION_ID"
  az config set core.collect_telemetry=false -o none 2>/dev/null || true
}
