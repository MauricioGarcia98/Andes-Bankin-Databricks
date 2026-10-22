# 08 · Etapa 05 · Azure con `az`: Storage, Function App, SAS y teardown

## 🎯 Objetivo

Crear con scripts (no con clics) todo lo que el proyecto necesita en Azure, por menos de USD 10 al mes:

- una **cuenta de Storage** con un contenedor por sistema origen;
- una **Function App** (Flex Consumption) para la API de tarjetas, el espejo FX y el CRM;
- las **credenciales**: SAS de escritura (los sistemas origen), SAS de lectura (el lakehouse) y el client id/secret de
  la API, guardadas en el secret scope `andes_bank` de Databricks;
- un script para **borrar todo** y otro para **ver cuánto se gastó**.

**Archivos:** `scripts/azure/{env,01_infra,02_secrets,03_deploy_api,04_costo,99_teardown}.sh`.

![Fuentes: el banco simulado](../docs/diagramas/01_fuentes.svg)

## 📚 Conceptos

- **Resource group:** una carpeta de recursos. Borrarla borra todo lo de adentro: es la forma segura de no dejar
  nada cobrando.
- **Nombres globales:** el nombre de un Storage o de una Function App es único **en el mundo**. Por eso llevan un
  sufijo derivado de tu suscripción (`andesbank826824`).
- **Flex Consumption:** la Function cobra por ejecución, con 250 mil ejecuciones gratis por mes. Si nadie la llama,
  cuesta cero.
- **SAS (*shared access signature*):** un token con permisos y vencimiento. El lakehouse recibe una de **sólo
  lectura**: aunque se filtre, no puede borrar nada.

## 🧱 Paso a paso

### 1. Variables comunes (`env.sh`)

```bash
SUFFIX="$(echo -n "$AZURE_SUBSCRIPTION_ID" | sha256sum | cut -c1-6)"
export STORAGE="andesbank${SUFFIX}"
export FUNC_APP="andes-card-api-${SUFFIX}"
export CONTAINERS="core cards-legacy api-data fx-mirror crm-data"
```

El sufijo sale de tu suscripción: cada persona que reconstruye el proyecto tiene sus propios nombres, y son siempre
los mismos (los scripts son idempotentes).

### 2. Infraestructura (`01_infra.sh`)

```bash
az group create -n "$RG" -l "$LOCATION" --tags $TAGS
az storage account create -n "$STORAGE" --sku Standard_LRS --kind StorageV2 --allow-blob-public-access false ...
for c in $CONTAINERS; do
  az storage container-rm create -n "$c" --storage-account "$STORAGE" -g "$RG" --public-access off
done
az functionapp create -n "$FUNC_APP" --flexconsumption-location "$LOCATION" --runtime python --runtime-version 3.12 ...
```

Fijate en `container-rm`: crea el contenedor por el **plano de administración** (`management.azure.com`), sin tocar el
endpoint de datos (`*.blob.core.windows.net`). Ver R04.

### 3. Credenciales (`02_secrets.sh`)

```bash
SAS_READ="$(az storage account generate-sas ... --permissions rl --expiry "$EXPIRY")"
CLIENT_SECRET="$(openssl rand -hex 24)"
az functionapp config appsettings set ... "API_CLIENT_ID=$CLIENT_ID" "API_CLIENT_SECRET=$CLIENT_SECRET" ...
databricks secrets put-secret andes_bank blob_sas_read --string-value "$SAS_READ"
```

Correrlo otra vez **rota** todas las credenciales: la Function y Databricks quedan con las nuevas a la vez.

### 4. Costo (`04_costo.sh`)

```bash
bash scripts/azure/04_costo.sh
#   Storage                          0.0214 USD
#   TOTAL del mes                    0.0214 USD  (OK: tope USD 10)
```

Consulta **Cost Management** a nivel del resource group (el service principal no tiene permisos de facturación sobre
toda la suscripción, y no los necesita).

## 🤔 Por qué así

- **¿Por qué scripts y no Terraform?** Para un proyecto de este tamaño, `az` es más directo de leer y de aprender;
  los scripts son idempotentes igual. Terraform aparece por debajo en los Asset Bundles de Databricks.
- **¿Por qué la SAS y no una identidad administrada?** Free Edition no puede asumir identidades de tu Azure (ADR 0002).

## 🧪 Probalo

```bash
source scripts/azure/env.sh && bash scripts/azure/01_infra.sh && bash scripts/azure/02_secrets.sh
databricks secrets list-secrets andes_bank      # blob_sas_read, blob_sas_write, card_api_*, storage_account
bash scripts/azure/04_costo.sh
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `az storage container create` → `Tunnel connection failed: 403` | la red bloquea el endpoint de datos de Blob | `az storage container-rm create` (R04) |
| `StorageAccountAlreadyTaken` | el nombre es global y quedó reservado tras un borrado | esperá unos minutos o cambiá el sufijo |
| `04_costo.sh` → `RBACAccessDenied` | se consultó a nivel suscripción | el script consulta el resource group: usá ese |
| `429 Too Many Requests` en Cost Management | la API de costos limita los pedidos | esperá un minuto y repetí |

## ✅ Punto de control

`git diff etapa-05 -- scripts/azure/` y en el portal el resource group `rg-andes-bank` con Storage y Function App.

## 🏋️ Ejercicio

Agregá a `01_infra.sh` una alerta de presupuesto por script (`az consumption budget create`) para no depender del
portal.
