# 17 · Etapa 13 · Silver II: identidad, SCD2, conciliación, tarjetas y CRM

## 🎯 Objetivo

Lo que convierte datos limpios en datos **confiables**:

- la **historia** de clientes, cuentas y tarjetas (SCD2) a partir del CDC del core;
- la **identidad**: la misma persona con dos números de cliente es una sola (fusión 2008, DAT-08);
- la **conciliación** saldo vs. movimientos: detectar lo que el core nunca mandó (DAT-10);
- **tarjetas** v1 + v2 en un solo modelo, y **FX** real con el espejo de respaldo;
- el **CRM** unido al core (DAT-09, DAT-16, DAT-17, DAT-18).

**Archivos:** `src/andes_bank/silver/{core,cards,crm,load}.py`.

![Historia (SCD2) e identidad](../docs/diagramas/13_scd2_identidad.svg)

## 📚 Conceptos

- **SCD2 (*slowly changing dimension* tipo 2):** cada cambio abre una **versión** con `valid_from`, `valid_to` e
  `is_current`. Responde "¿en qué segmento estaba este cliente en marzo de 2024?".
- **SCD mixto:** el nombre para **mostrar** es tipo 1 (se pisa); el control de cambios usa el nombre **normalizado**.
  Si no, la recarga del core nuevo (con acentos) abre cientos de versiones falsas (R08).
- **Persona vs. cliente:** `customer_xref` lleva cada `cliente_id` a su `persona_id`. Gold cuenta personas.
- **Saldo posterior:** si `saldo_anterior + importe ≠ saldo`, entre esos dos movimientos falta al menos uno.

## 🧱 Paso a paso

### 1. SCD2 desde eventos (`scd2_from_events`)

```python
w = Window.partitionBy(key).orderBy(ts_col, *order_cols)
e = (events.withColumn("estado", F.when(F.col("op") == "D", "BAJA").otherwise("ACTIVO"))
     .withColumn("_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(a).cast("string"), F.lit("∅"))
                                                   for a in attrs + ["estado"]]), 256))
     .withColumn("_prev_hash", F.lag("_hash").over(w))
     .where(F.col("_prev_hash").isNull() | (F.col("_hash") != F.col("_prev_hash"))))   # sólo si CAMBIÓ algo
return (e.withColumn("valid_from", F.col(ts_col))
         .withColumn("valid_to", F.lead(ts_col).over(w2))
         .withColumn("is_current", F.col("valid_to").isNull()))
```

Los maestros son chicos: se **reconstruyen** desde bronze en cada corrida. Determinístico, idempotente y sin casos
borde de MERGE.

### 2. Identidad (`customer_xref`)

```python
cur = history.where("is_current AND tipo_persona = 'HUMANA' AND dni IS NOT NULL")
w = Window.partitionBy("dni", "nombre_normalizado").orderBy("fecha_alta", "cliente_id")
return cur.withColumn("persona_id", F.first("cliente_id").over(w))        # el número más viejo
```

### 3. Conciliación (`balance_breaks`)

```python
.withColumn("saldo_anterior", F.lag("saldo").over(w))
.withColumn("diferencia", F.col("saldo") - (F.col("saldo_anterior") + F.col("importe")))
.where(F.col("saldo_anterior").isNotNull() & (F.abs("diferencia") > 0.01))
.withColumn("causa_probable", F.lit("MOVIMIENTO_FALTANTE"))
```

### 4. Tarjetas y FX (`cards.py`)

- `auth_from_v1`: centavos / 100, epoch ms → timestamp, `A`/`R` → `APPROVED`/`DECLINED`.
- `prefer_v2`: entre abr y jun de 2025 la misma autorización llega por los dos caminos; **gana la v2** (API-09).
- `fx_rates`: la cotización del BCRA real y, si falta, la del espejo, con `fx_source` diciendo de dónde salió.

### 5. CRM (`crm.py`)

El match con el core va en orden: `ID_CORE` (el ID cargado existe y su CUIT no contradice) → `CUIT` (cubre el ID
vacío, el del banco viejo `MZA…` y el mal tipeado) → `SIN_MATCH`. Las cuentas con el mismo CUIT son **duplicadas**: la
principal es la que tiene ID del core (o la más vieja). La descripción de los casos sale sin emails ni teléfonos.

## 🤔 Por qué así

- **¿Por qué reconstruir los maestros y no hacer MERGE?** Son miles de eventos: reconstruir tarda segundos y nunca
  deja una historia a medio actualizar.
- **¿Por qué la regla de identidad es DNI + nombre y no sólo DNI?** Un DNI mal tipeado uniría a dos personas
  distintas. Pedir también el nombre normalizado es más conservador: preferimos no unir antes que unir mal.

## 🧪 Probalo

```bash
pytest -q tests/test_e2e.py -k "identidad or faltantes or crm"
```

Los tests comparan contra la verdad: cada persona del core recupera su `person_id`, los faltantes detectados son como
mucho los que el core no mandó, y cada cuenta del CRM que es cliente se une a **su** persona.

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Cientos de versiones SCD2 en mar-2024 | el core viejo mandaba "Belen" y el nuevo "Belén" | SCD mixto: el control de cambios usa el nombre normalizado (R08) |
| La misma autorización dos veces | v1 y v2 se superponen abr-jun 2025 | `prefer_v2` |
| Cuentas del CRM sin match aunque son clientes | se buscó sólo por `Core_Customer_Id__c` | caer al CUIT sin guiones (DAT-09/17) |

## ✅ Punto de control

`git diff etapa-13 -- src/andes_bank/silver/core.py src/andes_bank/silver/cards.py src/andes_bank/silver/crm.py`

## 🏋️ Ejercicio

Agregá a `customer_xref` una segunda regla: mismo CUIT en personas jurídicas. ¿Cuántos pares nuevos aparecen? ¿Son
la misma empresa de verdad? (Pista: preguntale a la verdad, `sim.customers.person_id`.)
