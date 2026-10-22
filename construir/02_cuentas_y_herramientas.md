# 02 · Cuentas y herramientas: Free Edition, Azure con presupuesto y las CLIs

## 🎯 Objetivo

Tener las tres cuentas del proyecto andando y las herramientas instaladas **antes** de escribir una línea de código:

- **Databricks Free Edition**, verificada con LinkedIn (sin eso no hay acceso saliente a internet).
- **Azure** pago por uso, con un **presupuesto de USD 10** y alertas.
- **GitHub**, con el repo vacío.
- En tu máquina: Python 3.12, Java 17 (para Spark local), `databricks`, `az` y `git`.

![El proyecto completo](../docs/diagramas/00_panorama.svg)

## 📚 Conceptos

- **Free Edition** es un workspace de Databricks gratis, con cómputo **serverless** y una cuota diaria. No tenés que
  administrar clusters, pero tampoco podés crear *external locations* hacia tu propio storage (por eso existe el
  capítulo 12).
- **Acceso saliente:** una cuenta sin verificar sólo puede llamar a dominios "de confianza". Verificarla con LinkedIn
  desbloquea internet: así Databricks puede llamar a tu API de Azure y a la del BCRA (patrón *pull*, ADR 0001).
- **Presupuesto de Azure:** no frena el gasto, **avisa**. El tope real lo ponen las decisiones de diseño: Storage
  cobra por GB y la Function App (Flex Consumption) por ejecución, con una franja gratis enorme.
- **Service principal:** un "usuario robot" de Azure para que los scripts (y Claude Code) entren sin tu contraseña.
  Sus credenciales van en variables de entorno, nunca en el repo.

## 🧱 Paso a paso

### 1. Databricks Free Edition

1. Creá la cuenta en <https://www.databricks.com/learn/free-edition>.
2. **Settings → Verificar con LinkedIn.** Hasta que no lo hagas, el *spike* del capítulo 03 va a fallar con errores
   de conexión (no con códigos HTTP).
3. Creá un token personal: **Settings → Developer → Access tokens**. Guardalo como `DATABRICKS_TOKEN` y la URL del
   workspace como `DATABRICKS_HOST`.

### 2. Azure con presupuesto

```bash
az login
az account show --query id -o tsv          # tu AZURE_SUBSCRIPTION_ID
```

En el portal: **Cost Management → Presupuestos → Agregar**: USD 10 por mes, alertas al 50 %, 80 % y 100 %.

Para que los scripts entren solos, un service principal con permiso sobre la suscripción:

```bash
az ad sp create-for-rbac --name andes-bank-cli --role Contributor --scopes /subscriptions/$AZURE_SUBSCRIPTION_ID
# → AZURE_CLIENT_ID (appId), AZURE_CLIENT_SECRET (password), AZURE_TENANT_ID (tenant)
```

### 3. Las CLIs

```bash
pip install --upgrade pip
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh   # databricks
databricks --version
az version
java -version        # 17
```

### 4. Las variables de entorno

Un archivo `~/.andes.env` (fuera del repo) que cargás con `source ~/.andes.env`:

```bash
export DATABRICKS_HOST=https://<tu-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
export AZURE_SUBSCRIPTION_ID=... AZURE_TENANT_ID=... AZURE_CLIENT_ID=... AZURE_CLIENT_SECRET=...
```

## 🤔 Por qué así

- **¿Por qué Azure y no todo en Databricks?** Porque un banco real no te da sus datos en tu workspace: los deja en
  un storage, los sirve por una API o los tiene en un SaaS. Simular eso afuera de Databricks obliga a resolver la
  ingesta de verdad: credenciales, redes, reintentos.
- **¿Por qué un service principal?** Porque los scripts de `scripts/azure/` corren igual en tu máquina, en CI y en una
  sesión de Claude Code, sin `az login` interactivo.
- **¿Por qué no Salesforce?** Se intentó (bitácora R15 a R17) y el login por API trabó el proyecto. El CRM es una
  fuente simulada más (capítulo 11).

## 🧪 Probalo

```bash
databricks current-user me | grep userName
az account show --query name -o tsv
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `databricks` responde 401 | token vencido o de otro workspace | creá otro token y revisá `DATABRICKS_HOST` |
| `az ad sp create-for-rbac` falla por permisos | tu usuario no es Owner de la suscripción | pedile a quien sí lo sea, o usá tu usuario con `az login` |
| Más adelante, el job no llega a internet | la cuenta de Free Edition no está verificada | verificala con LinkedIn y repetí el capítulo 03 |

## ✅ Punto de control

Las dos CLIs responden con tu usuario y el presupuesto de USD 10 aparece en **Cost Management**.

## 🏋️ Ejercicio

Agregá a `~/.andes.env` un alias `andes-costo` que corra `bash scripts/azure/04_costo.sh` (el capítulo 08 lo explica).
