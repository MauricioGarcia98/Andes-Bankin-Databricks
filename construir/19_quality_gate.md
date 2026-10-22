# 19 · Etapa 15 · Quality gate y el pipeline completo en local

## 🎯 Objetivo

- **Reglas de calidad** como código: cada una cuenta filas que **no** cumplen, con su ID del catálogo.
- Un **quality gate** que frena la corrida si falla una regla bloqueante: Genie nunca ve datos rotos.
- El **test de punta a punta**: el banco simulado → Azure (falso) → landing → bronze → silver → gold → gate, y cada
  resultado comparado contra la **verdad**.

**Archivos:** `src/andes_bank/quality/gate.py`, `notebooks/40_quality_gate.py`, `tests/test_e2e.py`.

![Gold y quality gate](../docs/diagramas/04_gold_calidad.svg)

## 📚 Conceptos

- **Regla = consulta que cuenta filas malas:** `SELECT count(*) - count(DISTINCT movimiento_id) FROM silver.transactions`.
  Cero es éxito. Cada regla también dice contra cuántas filas se mide (para sacar porcentajes).
- **Dos severidades:**
  - `fail` (9 reglas): una sola fila mala frena la corrida (duplicados, importes ilegibles, días de la API
    incompletos, PII en texto libre...);
  - `warn` (13 reglas): suciedad conocida y ya tratada que hay que seguir midiendo (CUIT inválidos, archivos vacíos,
    cuentas del CRM sin match...).
- **`ops.dq_results`:** la historia de cada corrida. El Genie Agent de Plataforma la consulta.
- **Una verdad conocida:** como el generador sabe qué pasó, el test e2e puede afirmar "silver recuperó exactamente los
  movimientos que el core mandó, ni uno más".

## 🧱 Paso a paso

### 1. Una regla

```python
Rule("dias_api_completos", "ops.api_reconciliation",
     "SELECT count(*) FROM (SELECT resource, day, max_by(complete, checked_at) AS ok FROM {t} GROUP BY ALL) WHERE NOT ok",
     "SELECT count(DISTINCT resource, day) FROM {t}", "fail", "API-06", "Cada día pedido a la API quedó completo")
```

### 2. El gate

```python
def gate(spark, cfg, run_id):
    results = evaluate(spark, cfg, run_id)            # corre todas y las guarda en ops.dq_results
    broken = [r for r in results if not r["passed"]]   # una regla warn siempre "pasa": sólo se registra
    if broken:
        raise QualityGateError(f"Quality gate: {len(broken)} reglas bloqueantes fallaron: {detail}")
```

### 3. El test de punta a punta (`test_e2e.py`)

Un fixture arma **todo** el pipeline en local, con la API en memoria y caos `normal`:

```python
api = CardApi(MemoryStore(pages), MemoryFxStore({"USD": fx}), Settings("c", "s", "k"),
              Chaos("normal", seed=9, rng=random.Random(2), clock=lambda: now[0]), crm_store=crm)
client = ApiClient("http://fake/api", "c", "s", session=FakeSession(api), sleep=lambda s: None)
```

y los tests comparan contra `sim.*`:

```python
def test_movimientos_recuperan_la_verdad(spark, lake):
    assert silver.count() == silver.select("movimiento_id").distinct().count()                 # DAT-01
    assert j.where(F.abs(F.col("importe") - F.col("signed_amount")) > 0.005).count() == 0     # DAT-02 y signo
    assert 0 < missing < truth.count() * 0.001                                                # DAT-10: sólo lo no enviado
```

## 🤔 Por qué así

- **¿Por qué el gate después de gold y no al final de silver?** Porque algunas reglas miran gold (el dólar de cada
  día, los movimientos sin cliente) y otras miran `ops` (los días de la API). Va al final, y lo que bloquea es
  **publicar**.
- **¿Por qué tantas reglas `warn`?** Porque la suciedad que ya se trata no debería frenar el banco, pero sí verse:
  si los CUIT inválidos pasan del 0,5 % al 5 %, algo cambió en el origen.

## 🧪 Probalo

```bash
pytest -q -m spark                    # todos los de Spark, e2e incluido (unos minutos)
```

```sql
SELECT rule, severity, failed_rows, passed FROM andes_bank.ops.dq_results
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM andes_bank.ops.dq_results);
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `dias_api_completos` falla con 2 días | páginas salteadas + reintentos inmediatos | rondas al final y watermark sin huecos (R14) |
| El test e2e pasa a veces sí y a veces no | el caos depende del reloj real | reloj simulado en los tests (R14) |
| Faltan ~20 mil filas en el e2e | tablas Parquet en local | Delta por defecto (R09) |

## ✅ Punto de control

`git diff etapa-15 -- src/andes_bank/quality/gate.py tests/test_e2e.py`

## 🏋️ Ejercicio

Agregá la regla `cuarentena_controlada` (`warn`): las filas en `silver.quarantine` no pueden superar el 0,1 % de los
movimientos. ¿Qué número da hoy?
