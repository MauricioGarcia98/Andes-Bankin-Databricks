#!/usr/bin/env bash
# Se corre una sola vez al crear el codespace: instala la CLI de Databricks y el paquete del proyecto.
set -euo pipefail

# CLI de Databricks, versión fija (la 0.270 fallaba al desplegar: bitácora, capítulo 20)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.290.0/install.sh | sudo sh

# el paquete del proyecto en modo editable, con lo necesario para los tests, Azure y Genie
if [ -f pyproject.toml ]; then
  pip install --upgrade pip
  pip install -e ".[dev,azure,genie]"
fi

echo "Listo: $(python --version) · $(java -version 2>&1 | head -1) · $(az version --query '\"azure-cli\"' -o tsv) · $(databricks --version)"
