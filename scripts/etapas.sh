#!/usr/bin/env bash
# Crea (o mueve) los tags etapa-NN: el punto de control de cada capítulo de construir/.
#
#   bash scripts/etapas.sh            # muestra qué commit le toca a cada etapa
#   bash scripts/etapas.sh --push     # además crea los tags y los sube
#
# Cada etapa apunta al ÚLTIMO commit de main que tocó sus archivos: así `git diff etapa-NN -- <archivos>` compara tu
# código con la versión final de esa etapa (incluidos los arreglos que vinieron después, como R14 o el CRM).
set -euo pipefail
cd "$(dirname "$0")/.."
REF="${REF:-origin/main}"

declare -A ETAPAS=(
  [02]="pyproject.toml src/andes_bank/config.py src/andes_bank/utils/ids.py src/andes_bank/utils/spark.py tests/test_ids.py tests/test_config.py"
  [03]="src/andes_bank/generator/reference.py src/andes_bank/generator/rand.py src/andes_bank/generator/entities.py src/andes_bank/generator/events.py src/andes_bank/generator/truth.py"
  [04]="src/andes_bank/generator/exports.py tests/test_exports.py"
  [05]="scripts/azure"
  [06]="azure/card_api tests/test_card_api.py"
  [07]="src/andes_bank/generator/upload.py resources/andes_fuentes.job.yml notebooks/01_generate_truth.py notebooks/02_export_sources.py notebooks/03_upload_to_azure.py notebooks/04_fx_mirror.py"
  [08]="src/andes_bank/generator/crm.py notebooks/05_crm_publish.py tests/test_crm.py"
  [09]="src/andes_bank/ingestion/landing_sync.py src/andes_bank/ingestion/bronze_files.py"
  [10]="src/andes_bank/ingestion/api_client.py"
  [11]="src/andes_bank/ingestion/bronze_api.py src/andes_bank/ingestion/bronze_crm.py"
  [12]="src/andes_bank/silver/transforms.py"
  [13]="src/andes_bank/silver/core.py src/andes_bank/silver/cards.py src/andes_bank/silver/crm.py src/andes_bank/silver/load.py"
  [14]="src/andes_bank/gold"
  [15]="src/andes_bank/quality tests/test_e2e.py"
  [16]="databricks.yml resources"
  [17]="sql/governance sql/crm"
  [18]="sql/semantic"
  [19]="genie scripts/build_genie.py scripts/deploy_genie.py scripts/validate_genie_sql.py scripts/genie_benchmark.py"
  [21]=".assistant_instructions.md .assistant"
  [22]=".github/workflows"
)

for n in $(printf '%s\n' "${!ETAPAS[@]}" | sort); do
  # shellcheck disable=SC2086
  sha="$(git log -1 --format=%h "$REF" -- ${ETAPAS[$n]})"
  echo "etapa-$n  $sha  $(git log -1 --format=%s "$sha" | cut -c1-80)"
  if [[ "${1:-}" == "--push" ]]; then
    git tag -f -a "etapa-$n" "$sha" -m "Punto de control de la etapa $n (construir/)"
  fi
done
if [[ "${1:-}" == "--push" ]]; then
  git push -f origin $(printf 'refs/tags/etapa-%s ' $(printf '%s\n' "${!ETAPAS[@]}" | sort))
fi
