#!/usr/bin/env bash
# Etapa 05 · Infraestructura de Azure del proyecto (idempotente: se puede correr varias veces).
#
#   Storage (datos)      andesbank<sufijo>  contenedores: core, cards-legacy, api-data, fx-mirror, crm-data
#   Storage (funciones)  andesfn<sufijo>
#   Function App         andes-card-api-<sufijo>  (Flex Consumption, Python 3.12, 512 MB, máx. 4 instancias)
#
# Nada de esto cobra por estar encendido: Storage cobra por GB y operaciones; Flex Consumption por ejecución
# (con 250 mil ejecuciones y 100 mil GB-s gratis por mes).
set -euo pipefail
source "$(dirname "$0")/env.sh"
az_login

echo "▶ resource group $RG ($LOCATION)"
az group create -n "$RG" -l "$LOCATION" --tags $TAGS -o none

for sa in "$STORAGE" "$FUNC_STORAGE"; do
  if ! az storage account show -n "$sa" -g "$RG" -o none 2>/dev/null; then
    echo "▶ storage account $sa"
    az storage account create -n "$sa" -g "$RG" -l "$LOCATION" --sku Standard_LRS --kind StorageV2 \
      --access-tier Hot --min-tls-version TLS1_2 --allow-blob-public-access false --tags $TAGS -o none
  fi
done

# container-rm crea los contenedores por el plano de administración (management.azure.com): no necesita la clave
# de la cuenta ni acceso de red al endpoint de blobs.
for c in $CONTAINERS; do
  az storage container-rm create -n "$c" --storage-account "$STORAGE" -g "$RG" --public-access off -o none
done
echo "  contenedores: $CONTAINERS"

if ! az functionapp show -n "$FUNC_APP" -g "$RG" -o none 2>/dev/null; then
  echo "▶ function app $FUNC_APP (Flex Consumption)"
  az functionapp create -n "$FUNC_APP" -g "$RG" --storage-account "$FUNC_STORAGE" \
    --flexconsumption-location "$LOCATION" --runtime python --runtime-version 3.12 \
    --instance-memory 512 --maximum-instance-count 40 --tags $TAGS -o none
fi
# máximo de instancias bajo = techo de costo (y un rate limit natural, como el de un proveedor real)
az functionapp scale config set -n "$FUNC_APP" -g "$RG" --maximum-instance-count 40 -o none 2>/dev/null || true

DATA_CONN="$(az storage account show-connection-string -n "$STORAGE" -g "$RG" -o tsv)"
az functionapp config appsettings set -n "$FUNC_APP" -g "$RG" -o none --settings \
  "DATA_STORAGE_CONNECTION=$DATA_CONN" \
  "CHAOS_PROFILE=${CHAOS_PROFILE:-normal}" \
  "CHAOS_SEED=${CHAOS_SEED:-1998}"
echo "✔ infraestructura lista: https://$FUNC_APP.azurewebsites.net"
