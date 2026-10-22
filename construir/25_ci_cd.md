# 25 · Etapa 22 · CI/CD con GitHub Actions

## 🎯 Objetivo

Que ningún cambio llegue a `main` sin pasar lint y tests, y que el bundle se valide contra el workspace:

- en cada **PR**: `ruff` + tests rápidos (sin Spark), en minutos;
- en **`main`**: además, los tests con Spark (e2e incluido);
- si el repo tiene las credenciales: `databricks bundle validate`.

**Archivos:** `.github/workflows/ci.yml`.

![Despliegue, CI y orquestación](../docs/diagramas/06_despliegue_jobs.svg)

## 📚 Conceptos

- **Pirámide de tests:** muchos tests rápidos (lógica pura, la API en memoria) y pocos lentos (Spark, e2e). Los rápidos
  dan feedback en cada push; los lentos protegen `main`.
- **Secrets de GitHub:** `DATABRICKS_HOST` y `DATABRICKS_TOKEN` se cargan en el repo (Settings → Secrets) y el job los
  lee como variables. Nunca en el YAML.

## 🧱 Paso a paso

```yaml
jobs:
  lint-y-tests-rapidos:
    steps:
      - run: pip install -e ".[dev,azure]" requests
      - run: ruff check src tests azure scripts
      - run: pytest -m "not spark"

  tests-spark:
    if: github.event_name != 'pull_request'          # en main y a mano
    steps:
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "17" }
      - run: pytest -m spark

  bundle-validate:
    if: ${{ vars.DATABRICKS_CONFIGURED == 'true' }}   # sólo si el repo tiene las credenciales
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
    steps:
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t dev
```

Para activar `bundle-validate`: en GitHub, **Settings → Secrets and variables → Actions**: los secrets
`DATABRICKS_HOST` y `DATABRICKS_TOKEN`, y la variable `DATABRICKS_CONFIGURED = true`.

## 🤔 Por qué así

- **¿Por qué no correr Spark en cada PR?** Tarda varios minutos y casi siempre da lo mismo que los rápidos. En `main`
  sí: es el último filtro antes de desplegar.
- **¿Por qué no desplegar automáticamente?** En Free Edition hay un solo workspace y una cuota diaria; el deploy es
  una decisión consciente (`databricks bundle deploy`).

## 🧪 Probalo

Abrí un PR con un cambio chico y mirá la pestaña **Checks**: `lint-y-tests-rapidos` en verde; `tests-spark` y
`bundle-validate` salteados.

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| `bundle-validate` siempre salteado | falta la variable `DATABRICKS_CONFIGURED` | cargala en Settings → Variables |
| `ruff` falla sólo en CI | otra versión de ruff local | `pip install -e ".[dev]"` usa la misma |
| `tests-spark` tarda más de 90 minutos | demasiados clientes en el e2e | el e2e usa `local_config` (1.000 clientes) |

## ✅ Punto de control

`git diff etapa-22 -- .github/workflows/ci.yml`

## 🏋️ Ejercicio

Agregá un paso que corra `python scripts/validate_genie_sql.py` en `main` cuando el repo tenga credenciales: un
benchmark que no corre no debería llegar a producción.
