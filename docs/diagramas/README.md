# Diagramas del proyecto

Un diagrama por fase, dibujados con [Excalidraw](https://excalidraw.com). Cada uno está en dos formatos:

- **`.excalidraw`**: el original, editable. Abrilo en <https://excalidraw.com> (*Abrir* → elegí el archivo) o con la
  extensión de Excalidraw para VS Code.
- **`.svg`**: el exportado, que es lo que muestra GitHub.

| # | diagrama | de qué trata |
|---|---|---|
| 00 | [Panorama](#00--el-proyecto-completo) | el proyecto completo, de las fuentes a Genie |
| 01 | [Fuentes](#01--fuentes-el-banco-simulado) | el job que simula el banco y lo deja en Azure |
| 02 | [Ingesta](#02--ingesta-de-las-fuentes-a-bronze) | de Blob, la API y el BCRA a bronze |
| 03 | [Silver](#03--silver-un-arreglo-por-problema) | qué se arregla en cada fuente |
| 04 | [Gold y calidad](#04--gold-y-quality-gate) | el modelo dimensional, los marts y el quality gate |
| 05 | [Semántica y Genie](#05--capa-semántica-y-genie) | gobierno, vistas, métricas, funciones y los 4 agents |
| 06 | [Despliegue y jobs](#06--despliegue-ci-y-orquestación) | CI, Asset Bundles y las tareas de cada job |
| 07 | [La API y el cliente](#07--la-api-con-caos-y-el-cliente) | cada falla inyectada y cómo se sobrevive |

## Cómo se regeneran

Los diagramas son **código**: se escriben en `scripts/diagramas/generar.py` con una mini-librería
(`scripts/diagramas/excali.py`). Si cambia la arquitectura, se cambia ahí y se regenera:

```bash
python scripts/diagramas/generar.py                     # → docs/diagramas/*.excalidraw
cd scripts/diagramas && npm install && node exportar.mjs   # → docs/diagramas/*.svg  (--png para PNG)
```

`exportar.mjs` usa la librería de Excalidraw dentro de Chromium (Playwright) y le incrusta la tipografía, porque GitHub
no deja que un SVG cargue fuentes externas. Si editás un `.excalidraw` a mano en excalidraw.com, exportalo a SVG desde
ahí; pero la próxima regeneración lo pisa: para que el cambio quede, pasalo a `generar.py`.

## 00 · El proyecto completo

![Panorama](00_panorama.svg)

## 01 · Fuentes: el banco simulado

![Fuentes](01_fuentes.svg)

## 02 · Ingesta: de las fuentes a bronze

![Ingesta](02_ingesta.svg)

## 03 · Silver: un arreglo por problema

![Silver](03_silver.svg)

## 04 · Gold y quality gate

![Gold y calidad](04_gold_calidad.svg)

## 05 · Capa semántica y Genie

![Semántica y Genie](05_semantica_genie.svg)

## 06 · Despliegue, CI y orquestación

![Despliegue y jobs](06_despliegue_jobs.svg)

## 07 · La API con caos y el cliente

![La API y el cliente](07_api_caos_cliente.svg)
