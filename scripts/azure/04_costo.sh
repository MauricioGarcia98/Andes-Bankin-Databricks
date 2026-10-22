#!/usr/bin/env bash
# Gasto del mes en curso del resource group, por servicio (Cost Management, costo real en USD).
# Criterio del MVP: menos de USD 10 por mes (docs/00). Se consulta a nivel resource group porque el service principal
# del proyecto no tiene permisos de facturación sobre toda la suscripción.
set -euo pipefail
source "$(dirname "$0")/env.sh"
az_login
BODY='{"type":"ActualCost","timeframe":"MonthToDate","dataset":{"granularity":"None",
  "aggregation":{"c":{"name":"Cost","function":"Sum"}},"grouping":[{"type":"Dimension","name":"ServiceName"}]}}'
az rest --method post -o json --body "$BODY" \
  --url "https://management.azure.com/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$RG/providers/Microsoft.CostManagement/query?api-version=2023-03-01" \
| python3 -c '
import json, sys
rows = json.load(sys.stdin)["properties"]["rows"]
total = sum(r[0] for r in rows)
for cost, svc, cur in sorted(rows, reverse=True):
    print(f"  {svc:30s} {cost:8.4f} {cur}")
estado = "OK" if total < 10 else "SUPERA EL TOPE"
print(f"  TOTAL del mes                  {total:8.4f} USD  ({estado}: tope USD 10)")'
