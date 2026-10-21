---
name: benchmark-genie
description: Cómo escribir y validar benchmarks para los Genie Agents de Andes Bank.
---
# Benchmarks de Genie

- Un benchmark es una pregunta como la haría el usuario + el SQL con la respuesta oficial (en `genie/src/<agent>.yml`).
- El SQL tiene que ser determinístico (sin `current_date()`: usar la fecha máxima de los datos) y devolver lo mínimo
  (un número o una fila).
- Antes de commitear: correr el SQL en el SQL Editor y mirar que el resultado tenga sentido.
- `python scripts/build_genie.py` arma el JSON; `python scripts/deploy_genie.py` lo despliega.
- Si Genie falla un benchmark, arreglá primero lo de abajo de la pirámide (comentario, métrica, grano de la tabla) y
  recién después las instrucciones del agent.
