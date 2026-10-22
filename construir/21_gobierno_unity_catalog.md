# 21 · Etapa 17 · Gobierno en Unity Catalog

## 🎯 Objetivo

Que los datos se **expliquen solos** y que cada persona vea **lo que le corresponde**:

- comentarios en cada tabla y columna, en castellano;
- claves primarias y foráneas (informativas) en gold;
- tags por dominio y por fuente;
- máscaras de PII (DNI, CUIT, email, teléfono, CBU) y filtros de fila por región.

**Archivos:** `sql/governance/0[1-4]_*.sql`, `sql/crm/01_governance.sql`, `src/andes_bank/sql_runner.py`,
`notebooks/35_governance_semantic.py`.

## 📚 Conceptos

- **Comentarios = documentación que Genie lee.** Un comentario como "Para contar empresas usar
  `COUNT(DISTINCT cuenta_principal_id)`" es una instrucción que Genie sigue.
- **PK/FK informativas:** Databricks no las hace cumplir, pero Genie y las herramientas de BI las usan para saber cómo
  unir tablas.
- **Máscara de columna:** una función SQL que decide qué ve cada uno (`mask_documento(valor)`). Se asocia a la
  columna y aplica a cualquier consulta, también a las de Genie.
- **Row filter:** una función que decide qué **filas** ve cada uno (el gerente del NOA, sólo clientes del NOA).

## 🧱 Paso a paso

### 1. SQL versionado con variables (`sql_runner.py`)

```
${catalog} ${bronze} ${silver} ${gold} ${semantic} ${ops}   → nombres reales según la config
```

Las sentencias se separan por `;` al final de línea, salvo dentro de un bloque `$$ ... $$` (el YAML de las metric
views).

### 2. Máscaras

```sql
CREATE OR REPLACE FUNCTION ${ops}.puede_ver_pii()
RETURNS BOOLEAN
RETURN is_member('admins') OR is_account_group_member('andes_cumplimiento') OR is_account_group_member('andes_comercial');

CREATE OR REPLACE FUNCTION ${ops}.mask_documento(valor STRING)
RETURNS STRING
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN valor ELSE concat('*****', right(valor, 3)) END;
```

### 3. Filtro por región

`ops.region_access (principal, region)`: quién ve qué región (`*` = todas). Quien corre el job queda con `*`.

### 4. Idempotente

```sql
ALTER TABLE ${gold}.dim_fecha DROP CONSTRAINT IF EXISTS pk_dim_fecha CASCADE;
ALTER TABLE ${gold}.dim_fecha ADD CONSTRAINT pk_dim_fecha PRIMARY KEY (fecha);
```

## 🤔 Por qué así

- **¿Por qué SQL y no la interfaz?** Porque lo que se hace con clics no se revisa en un PR ni se reconstruye.
- **¿Por qué grupos de cuenta (`is_account_group_member`)?** Porque los permisos se dan a áreas, no a personas.

## 🧪 Probalo

```bash
databricks bundle run andes_lakehouse -t dev --only governance_semantic
```

```sql
DESCRIBE TABLE EXTENDED andes_bank.gold.dim_cliente;      -- comentarios, máscaras y row filter
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `Cannot delete constraint pk_dim_fecha with non empty child constraints` | las FK dependen de la PK | `DROP CONSTRAINT IF EXISTS ... CASCADE` (R12) |
| `UNRESOLVED_COLUMN` dentro de una función | parámetro usado en una subconsulta escalar | tabla derivada con `dense_rank()` (R13) |
| Un analista ve el DNI completo | no se asoció la máscara a la columna | `ALTER TABLE ... ALTER COLUMN dni SET MASK ...` |

## ✅ Punto de control

`git diff etapa-17 -- sql/governance/ sql/crm/`

## 🏋️ Ejercicio

Agregá el grupo `andes_auditoria` que ve todo **sin** máscara pero sólo de 2025 en adelante (row filter por fecha
sobre `fact_movimiento`).
