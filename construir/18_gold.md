# 18 · Etapa 14 · Gold: modelo dimensional y marts

## 🎯 Objetivo

El modelo que consultan las áreas, los dashboards y Genie: dimensiones y hechos en castellano, montos en su moneda
**y** en pesos **y** en dólares al oficial del día, y **marts** anchos para que Genie no tenga que hacer cinco joins.

**Archivos:** `src/andes_bank/gold/{model,marts,crm}.py`, `notebooks/30_gold.py`.

![El modelo dimensional](../docs/diagramas/14_modelo_estrella.svg)

## 📚 Conceptos

- **Grano:** qué representa una fila. `fact_movimiento` = un movimiento de cuenta; `fact_autorizacion` = una
  autorización (con su liquidación al lado); `fact_saldo_diario` = el saldo de cierre informado de una cuenta en un día.
- **`dim_fecha` con el dólar oficial:** el BCRA no publica fines de semana ni feriados; esos días repiten la última
  cotización publicada (DAT-13). Así **todo** día tiene dólar.
- **`importe_ars` e `importe_usd` siempre:** nadie tiene que convertir a mano en una pregunta a Genie. "¿Cuánto se
  depositó en dólares?" y "¿cuánto es eso en pesos?" salen de la misma fila.
- **Gold es derivado:** se reconstruye completo en cada corrida. Si cambia una regla, la próxima corrida la aplica
  a toda la historia.

## 🧱 Paso a paso

### 1. Dimensiones primero (`build_gold`)

```python
def build_gold(spark, cfg):
    """Dimensiones primero (los hechos las usan), después hechos. Reconstrucción completa: gold es derivado."""
```

`dim_cliente` sale de `customers_history` (SCD2) + `customer_xref`: cada versión sabe su `persona_id`.

### 2. Montos en las tres monedas (`_fx`)

Cada hecho se une a `dim_fecha` por su fecha (con `broadcast`: la dimensión es chica) y calcula:

```python
fx = _fx(spark, cfg).withColumnRenamed("fecha", "fecha_contable")          # fecha, usd_oficial
t.join(F.broadcast(fx), "fecha_contable", "left")
 .withColumn("importe_ars", F.when(F.col("moneda") == "USD", F.col("importe") * F.col("usd_oficial"))
                             .otherwise(F.col("importe")))
```

y lo mismo, al revés, para `importe_usd`.

### 3. Marts (`marts.py`)

| mart | área | grano |
|---|---|---|
| `mart_cliente_360` | Comercial | una fila por cliente vigente: productos, saldos, consumo, actividad |
| `mart_fraude_diario` | Tarjetas y fraude | día × rubro × exterior |
| `mart_tesoreria_mensual` | Finanzas | mes × moneda × segmento × región |
| `mart_conciliacion_tarjetas` | Operaciones | día: compras con débito según el procesador vs. según el core |
| `mart_pipeline_comercial` | Comercial (CRM) | persona: pipeline abierto, ganado, reclamos abiertos |

### 4. El CRM en gold (`crm.py`)

Las oportunidades y los casos de una cuenta duplicada se atribuyen a su **cuenta principal**: para Comercial, una
empresa es una sola aunque dos ejecutivos la hayan cargado dos veces. `dim_cuenta_crm.segmento_desactualizado` marca
las cuentas cuyo segmento en el CRM no coincide con el del core.

## 🤔 Por qué así

- **¿Por qué marts además del modelo estrella?** Genie responde mejor sobre una tabla ancha con nombres claros que
  armando joins. El modelo estrella queda para las preguntas que nadie previó.
- **¿Por qué nombres en castellano?** Porque las preguntas a Genie son en castellano. `importe_usd` se encuentra solo;
  `amt_usd_conv` no.

## 🧪 Probalo

```bash
pytest -q tests/test_e2e.py -k "gold or crm_gold"
```

```sql
SELECT segmento, count(DISTINCT persona_id) FROM andes_bank.gold.dim_cliente WHERE is_current GROUP BY ALL;
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `importe_usd` nulo los sábados | el BCRA no publica fines de semana | `dim_fecha` repite la última cotización (DAT-13) |
| Más clientes en gold que personas reales | se contó `cliente_id` | contar `persona_id` (DAT-08) |
| Pipeline comercial duplicado | oportunidades de cuentas duplicadas contadas dos veces | atribuir a `cuenta_principal_id` |

## ✅ Punto de control

`git diff etapa-14 -- src/andes_bank/gold/`

## 🏋️ Ejercicio

Agregá `mart_morosidad_tarjeta` (mes × segmento: saldo de tarjeta de crédito impago al día 10). ¿Qué hecho
necesitás que todavía no existe?
