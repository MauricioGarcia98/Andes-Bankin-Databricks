// Exporta cada docs/diagramas/*.excalidraw a .svg (para GitHub) y, con --png, también a .png.
//
//   cd scripts/diagramas && npm install && node exportar.mjs [--png] [nombre ...]
//
// Usa la misma librería que el editor de Excalidraw (`exportToSvg` de @excalidraw/excalidraw) dentro de Chromium, y le incrusta la tipografía a mano
// (Virgil) en base64: GitHub muestra los SVG como imagen y no deja que carguen fuentes externas.
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, "..", "..", "docs", "diagramas");
const args = process.argv.slice(2);
const png = args.includes("--png");
const only = args.filter((a) => !a.startsWith("--"));

const nm = join(here, "node_modules");
const lib = join(nm, "@excalidraw/excalidraw/dist/");
const scripts = [join(nm, "react/umd/react.production.min.js"), join(nm, "react-dom/umd/react-dom.production.min.js"),
                 lib + "excalidraw.production.min.js"].map((f) => readFileSync(f, "utf8"));
const assets = lib + "excalidraw-assets/";
const font = (name) => `data:font/woff2;base64,${readFileSync(assets + name).toString("base64")}`;
const fonts = { Virgil: font("Virgil.woff2"), Cascadia: font("Cascadia.woff2") };

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined });
const page = await browser.newPage({ deviceScaleFactor: 2 });
await page.setContent(`<html><head><style>
  @font-face { font-family: Virgil; src: url(${fonts.Virgil}); }
  @font-face { font-family: Cascadia; src: url(${fonts.Cascadia}); }
  body { margin: 0; background: #fff; font-family: Virgil; }</style></head>
  <body><span style="font-family:Virgil">a</span><span style="font-family:Cascadia">a</span><div id="out"></div></body></html>`);
for (const content of scripts) await page.addScriptTag({ content });
await page.evaluate(() => document.fonts.ready);

const files = readdirSync(outDir).filter((f) => f.endsWith(".excalidraw"))
  .filter((f) => !only.length || only.some((o) => f.startsWith(o))).sort();
for (const f of files) {
  const scene = JSON.parse(readFileSync(join(outDir, f), "utf8"));
  let svg = await page.evaluate(async (s) => {
    const el = await ExcalidrawLib.exportToSvg({ elements: s.elements, appState: { ...s.appState, exportEmbedScene: false }, files: s.files || {} });
    document.getElementById("out").replaceChildren(el);
    return el.outerHTML;
  }, scene);
  // la librería apunta las fuentes a unpkg: se reemplazan por la fuente incrustada
  svg = svg.replace(/url\("[^"]*\/(Virgil|Cascadia)\.woff2"\)/g, (_, f) => `url("${fonts[f]}")`);
  const name = f.replace(/\.excalidraw$/, "");
  writeFileSync(join(outDir, `${name}.svg`), svg);
  if (png) await page.locator("#out svg").screenshot({ path: join(outDir, `${name}.png`) });
  console.log(`✔ ${name}.svg${png ? " + .png" : ""}`);
}
await browser.close();
