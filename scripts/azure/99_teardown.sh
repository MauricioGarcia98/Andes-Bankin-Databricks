#!/usr/bin/env bash
# Borra TODO lo de Azure del proyecto: el resource group entero (storage, function app, app insights).
# No se puede deshacer. Pide confirmación salvo que pases --yes.
set -euo pipefail
source "$(dirname "$0")/env.sh"
az_login
if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Vas a borrar el resource group $RG y todo su contenido. Escribí el nombre para confirmar: " answer
  [[ "$answer" == "$RG" ]] || { echo "Cancelado."; exit 1; }
fi
az group delete -n "$RG" --yes --no-wait
echo "Borrado en curso (tarda unos minutos). Verificá con: az group exists -n $RG"
