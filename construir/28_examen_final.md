# 28 · Examen final

## 🎯 Objetivo

Demostrar que el proyecto es **tuyo**: sin mirar el repo de referencia, agregar una fuente nueva de punta a punta, con
su suciedad, su arreglo, su regla de calidad y una pregunta de Genie que la use.

![El proyecto completo](../docs/diagramas/00_panorama.svg)

## 📋 La consigna

Andes Bank incorpora **cajeros automáticos (ATM)**. La red de cajeros manda un archivo diario con las extracciones:

| campo | ejemplo | suciedad |
|---|---|---|
| `id_operacion` | `ATM-20260915-000123` | 0,2 % repetidas en el archivo del día siguiente |
| `cajero` | `MZA-014` | |
| `tarjeta` | el token de la tarjeta de débito | |
| `fecha_hora` | `15/09/2026 23:41:07` | hora local, `dd/MM/yyyy` |
| `importe` | `20.000,00` | formato argentino |
| `estado` | `OK` / `SIN_SALDO` / `FUERA_DE_SERVICIO` | |

Lo tenés que hacer **todo**:

1. **Generador:** las extracciones salen de las tarjetas de débito y generan un movimiento `EXTRACCION_ATM` en el
   core cuando el estado es `OK` (los sistemas conectados, capítulo 06).
2. **Exporte y Azure:** un contenedor `atm/` con un CSV por día (capítulos 07 y 10).
3. **Landing y bronze:** el contrato de bronze, con linaje (capítulos 12 y 13).
4. **Silver:** `silver.atm_extracciones`, deduplicado, con hora UTC y local e importe decimal (capítulo 16).
5. **Conciliación:** toda extracción `OK` tiene su movimiento en el core (y al revés) (capítulo 17).
6. **Gold:** `fact_extraccion_atm` con `importe_ars` e `importe_usd` (capítulo 18).
7. **Calidad:** una regla `fail` (duplicados) y una `warn` (extracciones sin movimiento) con su ID nuevo en el
   catálogo (capítulo 19).
8. **Genie:** dos benchmarks nuevos en el agent de Tarjetas, medidos con `genie_benchmark.py` (capítulo 23).
9. **Diagrama:** sumá la fuente al panorama en `scripts/diagramas/generar.py` con un ícono de la biblioteca.

## ✅ Cómo se corrige

- `pytest -q` en verde, con un test que compare la verdad (`sim.atm`) contra silver.
- El quality gate pasa, y `ops.dq_results` muestra tus dos reglas.
- El agent de Tarjetas sigue en 80 % o más, contando tus benchmarks nuevos.
- Un compañero (o Claude) lee tu PR y entiende qué hiciste **sin preguntarte**: los IDs del catálogo y la bitácora lo
  explican.

## 💡 Pistas (sólo si te trabás)

- La suciedad sale de `uniform(..., salt=...)` y de un porcentaje arriba del archivo, como en `exports.py`.
- La conciliación es un `full outer join` por la referencia de la operación.
- Si Genie falla tu benchmark, empezá por la base de la pirámide: ¿el comentario de la tabla dice qué es una
  extracción?
