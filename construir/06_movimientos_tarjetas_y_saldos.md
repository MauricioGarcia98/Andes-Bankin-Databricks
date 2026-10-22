# 06 · Etapa 03 (II) · Movimientos, tarjetas y saldos (y por qué crecían sin techo)

## 🎯 Objetivo

Generar lo que le **pasa** al banco todos los días: compras con tarjeta, liquidaciones, movimientos de cuenta y saldos
de cierre. Y generarlos **conectados**, como en un banco real, para que el lakehouse pueda conciliar un sistema contra
otro.

**Archivos:** `src/andes_bank/generator/events.py`, `src/andes_bank/generator/truth.py`, `tests/test_truth.py`.

![Cómo se genera el banco](../docs/diagramas/09_generador.svg)

## 📚 Conceptos

- **Los sistemas están conectados:**

  ```
  autorizaciones (procesador) ──► liquidaciones (procesador)
        │ compras con débito aprobadas                │
        ▼                                             ▼
  movimientos del core ◄── pago del resumen de la tarjeta de crédito (día 10 del mes siguiente)
        │
        ▼
  saldos de cierre (core)
  ```

  Cada compra con débito **aprobada** del procesador es un movimiento `COMPRA_DEBITO` del core. Eso es lo que
  permite, en gold, el mart de conciliación procesador vs. core.
- **Saldo posterior:** cada movimiento trae el saldo de la cuenta después de aplicarlo. Si falta un movimiento, la
  cuenta "no cierra": así se detectan los faltantes (DAT-10).
- **Hora local vs. UTC:** el procesador registra en UTC; el core, en hora argentina, y después de las 21 h el
  movimiento pasa al día contable siguiente (`posting_date`).

## 🧱 Paso a paso

### 1. Autorizaciones y liquidaciones

`authorizations_df` cruza tarjetas y comercios: monto lognormal (muchas compras chicas, pocas grandes), hora con la
distribución de un banco real (`HOUR_WEIGHTS`) y algunas rechazadas por sospecha. `is_fraud` es verdad: el procesador
no la informa. Las aprobadas se **liquidan** 1 a 3 días después; algunas se reversan o nunca llegan (DAT-15).

### 2. Movimientos

`transactions_df` une cuatro orígenes: ingresos (sueldo), movimientos propios, compras con débito y el pago del
resumen de crédito. Cada movimiento tiene un `txn_id` estable (un hash de sus datos), y el saldo se calcula con una
ventana:

```python
w = Window.partitionBy("account_key").orderBy("ts_utc", "txn_id").rowsBetween(Window.unboundedPreceding, 0)
txn = txn.withColumn("cum", F.sum("signed_amount").over(w))
```

### 3. El bug de los saldos sin techo (R01)

La primera versión generaba más plata de la que salía: el sueldo entraba todos los meses y los saldos crecían sin
techo (mediana de **12 M de pesos** en una caja de ahorro). El arreglo es lo que hace cualquiera con su sueldo:
**lo que sobra va a plazo fijo**.

```python
# lo que excede 2,5 ingresos mensuales sale de la cuenta. Con B_m = saldo acumulado a fin del mes m y T_m = el techo,
# lo invertido acumulado es S_m = max(0, max_{k<=m}(B_k - T_k)) y la inversión del mes es S_m - S_{m-1}
.withColumn("excess", F.col("cum") - 2.5 * F.col("monthly_income_2023") * F.col("price_index"))
.withColumn("s_cum", F.greatest(F.lit(0.0), F.max("excess").over(w_run)))       # máximo corrido
.withColumn("amount", F.col("s_cum") - F.coalesce(F.lag("s_cum").over(w_prev), F.lit(0.0)))
```

Lo lindo es que **no hay bucles**: un máximo corrido (`max(...).over(ventana acumulada)`) resuelve "cuánto invertí
hasta ahora" para todos los meses de todas las cuentas a la vez.

### 4. Saldos de cierre

`balances_df`: el saldo que informa el core, para las cuentas con movimiento ese día y la foto de fin de mes de todas.

## 🤔 Por qué así

- **¿Por qué tanto cuidado con la conexión entre sistemas?** Porque las preguntas de negocio más valiosas cruzan
  sistemas ("¿cuántas compras del procesador no están en el core?"). Datos sintéticos independientes no permiten
  ninguna conciliación.
- **¿Por qué el índice de precios?** En Argentina un monto de 2023 y uno de 2026 no se comparan sin inflación. El
  generador "mueve" los montos con el índice y el lakehouse convierte todo a pesos y dólares del día.

## 🧪 Probalo

```bash
pytest -q tests/test_truth.py
```

Además de lo del capítulo 05: **ninguna caja de ahorro en negativo** y **cada compra con débito aprobada está en el
core** (`test_compras_con_debito_estan_en_el_core`).

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Saldos de millones en cajas de ahorro comunes | entra más plata de la que sale | barrido a plazo fijo con máximo corrido (R01) |
| Cajas de ahorro en negativo | un débito sin control de saldo | descontar sólo si hay saldo; el test lo detecta |
| Compras del procesador que no están en el core | los dos sistemas generados por separado | los débitos salen de las autorizaciones aprobadas |

## ✅ Punto de control

`git diff etapa-03 -- src/andes_bank/generator/events.py src/andes_bank/generator/truth.py`

## 🏋️ Ejercicio

Agregá transferencias entre cuentas del mismo cliente (sale de una, entra en la otra, mismo `reference`) y un test
que compruebe que suman cero.
