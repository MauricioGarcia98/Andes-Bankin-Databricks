"""Genera los diagramas del proyecto en docs/diagramas/*.excalidraw.

    python scripts/diagramas/generar.py                 # todos
    cd scripts/diagramas && npm install && node exportar.mjs     # → .svg (los que muestra GitHub)

Un diagrama por fase del proyecto. Si cambia la arquitectura, se cambia acá y se regenera: el diagrama es código,
igual que el resto.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from excali import MUTED, Diagram, _icons  # noqa: E402

OUT = Path(__file__).resolve().parents[2] / "docs" / "diagramas"
ROOT_GENIE = Path(__file__).resolve().parents[2] / "genie"


# ------------------------------------------------------------------------------------------- 00 panorama
def panorama() -> Diagram:
    d = Diagram("00_panorama", "Andes Bank · el proyecto completo")
    d.heading(40, 20, "De los sistemas origen simulados a las preguntas en castellano con Genie")

    d.zone(40, 130, 400, 800, "1 · Sistemas origen (simulados)", "naranja")
    d.box("gen", 80, 190, 320, 100, "Generador (semilla)\nla verdad: sim.*", "amarillo", icon="python")
    d.box("core", 80, 340, 320, 90, "Core bancario\nCSV / Parquet sucios", "naranja", icon="database")
    d.box("proc", 80, 470, 320, 90, "Procesador de\ntarjetas: API v1 y v2", "naranja", icon="server")
    d.box("crm", 80, 600, 320, 90, "CRM comercial\nmodelo de Salesforce", "naranja", icon="user")
    d.box("bcra", 80, 790, 320, 90, "API real del BCRA\ncotizaciones oficiales", "rojo", icon="internet")
    d.text(84, 750, "la única fuente 100 % real:", 16, MUTED)
    d.arrow("gen", "core", sides="bt")
    for b in ("proc", "crm"):
        d.arrow("gen", b, sides="ll", via=[(60, 240), (60, d.boxes[b].cy)])

    d.zone(500, 130, 420, 800, "2 · Azure (tope USD 10/mes)", "azul", icon="azure")
    d.box("blob", 540, 230, 340, 180, "Blob Storage\n\ncore/ · cards-legacy/\napi-data/ · crm-data/\nfx-mirror/", "azul", icon="blob")
    d.box("func", 540, 520, 340, 200, "Azure Function\n(Flex Consumption)\n\n/v1 /v2 tarjetas · /crm/v1\n/fx espejo · OAuth + caos", "violeta", icon="function")
    d.arrow("core", "blob", "archivos")
    d.arrow("proc", "blob", "páginas", sides="rl")
    d.arrow("crm", "blob", "estado", sides="rl")
    d.arrow("blob", "func", "lee", sides="bt")

    d.zone(980, 130, 760, 800, "3 · Databricks Free Edition · Unity Catalog andes_bank", "verde", icon="databricks")
    d.box("landing", 1020, 200, 300, 90, "landing\nBlob → Volume (SAS)", "turquesa", icon="storage")
    d.box("bronze", 1020, 340, 300, 90, "bronze\ncrudo + linaje", "naranja", icon="delta")
    d.box("silver", 1020, 480, 300, 90, "silver\nlimpio, SCD2, identidad", "gris", icon="delta")
    d.box("gold", 1020, 620, 300, 90, "gold\ndimensiones, hechos, marts", "amarillo", icon="delta")
    d.box("sem", 1020, 760, 300, 90, "semantic\nvistas, métricas, funciones", "violeta", icon="unity_catalog")
    d.box("ops", 1400, 340, 300, 110, "ops\napi_calls · watermarks\ndq_results", "gris", icon="bar_chart")
    d.box("dq", 1400, 600, 300, 110, "quality gate\n¿los datos están bien?\nsi no, frena", "rojo", icon="error")
    d.arrow("blob", "landing", "SAS de lectura", sides="rl", via=[(950, 320), (950, 245)])
    d.arrow("func", "bronze", "HTTP", sides="rl", via=[(950, 620), (950, 385)], seg=1)
    d.arrow("bcra", "bronze", "HTTP", sides="rl", via=[(470, 835), (470, 950), (970, 950), (970, 405)], dashed=True,
            seg=2)
    d.arrow("landing", "bronze", "Auto Loader", sides="bt")
    d.arrow("bronze", "silver", sides="bt")
    d.arrow("silver", "gold", sides="bt")
    d.arrow("gold", "sem", sides="bt")
    d.arrow("bronze", "ops", sides="rl", dashed=True)
    d.arrow("gold", "dq", sides="rl")

    d.zone(1800, 130, 380, 800, "4 · Consumo", "violeta", icon="genie")
    d.box("genie", 1840, 230, 300, 260, "4 Genie Agents\n\nComercial y clientes\nTarjetas y fraude\nFinanzas y tesorería\nPlataforma de datos", "rosa", icon="genie")
    d.box("areas", 1840, 600, 300, 120, "Cada área pregunta\nen castellano", "blanco", icon="user")
    d.arrow("sem", "genie", sides="rl", via=[(1770, 805), (1770, 360)])
    d.arrow("genie", "areas", sides="bt")

    d.note(40, 980, 2140, 70, "Orquestación: 3 Lakeflow Jobs (fuentes, día nuevo, lakehouse)  ·  Despliegue: Databricks Asset Bundles + scripts de az  ·  "
                              "CI: GitHub Actions (ruff, pytest, bundle validate)  ·  Secretos: scope andes_bank", "gris", 18)
    return d

# ---------------------------------------------------------------------------------------------- 01 fuentes
def fuentes() -> Diagram:
    d = Diagram("01_fuentes", "Fase 1 · Fuentes: el banco simulado")
    d.heading(40, 20, "Job andes-bank-fuentes: genera la verdad, la ensucia y la deja en Azure como cada sistema origen")

    d.zone(40, 130, 1180, 840, "Job andes-bank-fuentes · Databricks serverless", "naranja", icon="jobs")
    d.box("setup", 80, 200, 300, 80, "setup\ncatálogo y esquemas", "gris", icon="settings")
    d.box("gt", 80, 340, 340, 140, "generate_truth\nla verdad: sim.*\nclientes, cuentas, tarjetas,\nmovimientos, saldos", "amarillo", icon="python")
    d.box("ex", 80, 560, 340, 180, "export_sources\nla ensucia como cada sistema:\ncore viejo y nuevo, CDC,\nAPI v1 y v2, incidentes", "naranja", icon="document")
    d.box("up", 80, 780, 360, 100, "upload_to_azure\nsube sólo lo que falta (R07)", "azul", icon="storage")
    d.box("fx", 500, 200, 320, 100, "fx_mirror\ncopia la serie del BCRA", "rojo", icon="internet")
    d.box("crm", 500, 560, 320, 140, "crm_publish\nsim.crm_* → crm_system\nId, fechas, borrados\n(~1.900 registros)", "naranja", icon="user")
    d.box("bcra", 880, 340, 300, 100, "API real del BCRA\n(la única fuente real)", "rojo", icon="internet")
    d.note(880, 770, 310, 140, "sim.* es la VERDAD.\nEl lakehouse no la lee:\nla usan los tests para\nmedir si el pipeline\nrecupera lo que pasó.")
    d.arrow("setup", "gt", sides="bt")
    d.arrow("gt", "ex", sides="bt")
    d.arrow("ex", "up", sides="bt")
    d.arrow("setup", "fx", sides="rl")
    d.arrow("bcra", "fx", "HTTP", sides="lb", via=[(660, 390)], seg=0)
    d.arrow("gt", "crm", sides="rl", via=[(460, 410), (460, 630)])

    d.zone(1280, 130, 720, 840, "Azure", "azul", icon="azure")
    d.text(1320, 180, "Blob Storage (un contenedor por sistema)", 18, "#1971c2")
    d.box("c_core", 1320, 220, 360, 90, "core/\nCSV y Parquet por día", "azul", icon="blob")
    d.box("c_leg", 1320, 340, 360, 90, "cards-legacy/\nexportes de la API v1", "azul", icon="blob")
    d.box("c_api", 1320, 460, 360, 90, "api-data/\npáginas de 1.000 + _manifest", "azul", icon="blob")
    d.box("c_crm", 1320, 580, 360, 90, "crm-data/\nAccount.json, Lead.json...", "azul", icon="blob")
    d.box("c_fx", 1320, 700, 360, 90, "fx-mirror/\ncotizaciones/USD.json", "azul", icon="blob")
    d.box("func", 1700, 300, 260, 400, "Azure Function\n\n/oauth/token\n/v2/authorizations\n/v2/settlements\n/v1/authorizations\n/crm/v1/<Objeto>\n/fx/... (espejo)\n\n+ caos", "violeta", icon="function")
    d.note(1320, 830, 640, 70, "La API no tiene base de datos: cada pedido lee un blob.\nSirve millones de filas por centavos.", "gris")
    for c in ("c_core", "c_leg", "c_api"):
        d.arrow("up", c, sides="bl", via=[(250, 930), (1260, 930), (1260, d.boxes[c].cy)])
    d.arrow("crm", "c_crm", sides="rl")
    d.arrow("fx", "c_fx", sides="rl", via=[(1230, 250), (1230, 745)])
    for c in ("c_api", "c_crm", "c_fx"):
        d.arrow(c, "func", sides="rl", dashed=True)
    return d


# ---------------------------------------------------------------------------------------------- 02 ingesta
def ingesta() -> Diagram:
    d = Diagram("02_ingesta", "Fase 2 · Ingesta: de las fuentes a bronze")
    d.heading(40, 20, "Bronze guarda lo que llegó tal cual, con linaje. Nada se limpia acá.")

    d.zone(40, 130, 420, 860, "Fuentes", "azul", icon="azure")
    d.box("blob", 80, 200, 340, 120, "Blob Storage\ncore/ · cards-legacy/", "azul", icon="blob")
    d.box("func", 80, 420, 340, 200, "Azure Function\n/v2 tarjetas\n/crm/v1\n/fx espejo\n(con caos)", "violeta", icon="function")
    d.box("bcra", 80, 760, 340, 100, "API real del BCRA", "rojo", icon="internet")

    d.zone(520, 130, 1480, 860, "Job andes-bank-lakehouse · tareas de ingesta (en paralelo)", "verde", icon="jobs")
    d.box("sync", 560, 200, 340, 120, "landing_sync\nBlob → Volume con SAS\nvalida en el borde", "turquesa", icon="storage")
    d.box("al", 980, 200, 340, 120, "bronze_files\nAuto Loader con dos\ncontratos (core viejo y nuevo)", "naranja", icon="streaming")
    d.box("client", 560, 420, 340, 200, "ApiClient\nOAuth + renovación de token\nreintentos y backoff\ncursor + dedupe\nconcilia contra totales", "violeta", icon="server")
    d.box("bapi", 980, 400, 340, 100, "bronze_api\ntarjetas v2, por día", "naranja", icon="notebooks")
    d.box("bcrm", 980, 530, 340, 100, "bronze_crm\nincremental por SystemModstamp", "naranja", icon="notebooks")
    d.box("bfx", 980, 760, 340, 100, "bronze_fx\nBCRA real + espejo\n(vía ApiClient)", "naranja", icon="notebooks")
    d.arrow("blob", "sync", sides="rl")
    d.arrow("sync", "al", "Volume", sides="rl")
    d.arrow("func", "client", "HTTP", sides="rl")
    d.arrow("client", "bapi", sides="rl", via=[(940, 520), (940, 450)])
    d.arrow("client", "bcrm", sides="rl", via=[(940, 520), (940, 580)])
    d.arrow("bcra", "bfx", "HTTP", sides="rl", seg=0)
    d.arrow("client", "bfx", sides="rl", via=[(940, 520), (940, 790)])

    d.box("t_files", 1400, 200, 560, 120, "bronze.core_* (clientes, cuentas, tarjetas,\nmovimientos, saldos; *_legacy) · cards_v1_*", "amarillo", 17, icon="delta")
    d.box("t_api", 1400, 400, 560, 100, "bronze.cards_api_authorizations\nbronze.cards_api_settlements", "amarillo", 17, icon="delta")
    d.box("t_crm", 1400, 530, 560, 100, "bronze.crm_account · crm_contact · crm_lead\ncrm_opportunity · crm_case", "amarillo", 17, icon="delta")
    d.box("t_fx", 1400, 760, 560, 100, "bronze.fx_bcra · bronze.fx_mirror", "amarillo", 17, icon="delta")
    d.box("ops", 1400, 650, 560, 90, "ops.landing_files · api_calls · api_reconciliation\napi_watermarks · crm_watermarks", "gris", 17, icon="bar_chart")
    for a, b in (("al", "t_files"), ("bapi", "t_api"), ("bcrm", "t_crm"), ("bfx", "t_fx")):
        d.arrow(a, b, sides="rl")
    d.arrow("client", "ops", "cada pedido y cada día", sides="bl", via=[(730, 695)], dashed=True, seg=1)
    d.note(560, 860, 360, 110, "Un día que no cierra contra\nlos totales no avanza el\nwatermark: se vuelve a pedir\nal final de la corrida (R14).")
    return d


# ----------------------------------------------------------------------------------------------- 03 silver
def silver() -> Diagram:
    d = Diagram("03_silver", "Fase 3 · Silver: un arreglo por problema del catálogo")
    d.heading(40, 20, "Expresiones de Spark (sin UDFs), escrituras idempotentes y cada fila inválida a cuarentena con su motivo")
    d.icon("delta", 20, 132, 34)
    d.text(64, 140, "bronze (crudo)", 22, "#e8590c")
    d.icon("python", 500, 132, 34)
    d.text(544, 140, "qué se arregla", 22, "#6741d9")
    d.icon("delta", 1240, 132, 34)
    d.text(1284, 140, "silver (limpio)", 22, "#2f9e44")
    rows = [
        ("core_clientes (+ _legacy)", "CDC → SCD2 mixto (R08)\nidentidad: la fusión de 2008 (DAT-03)", "customers_history\ncustomer_xref"),
        ("core_cuentas · core_tarjetas\n(+ _legacy)", "CDC → historia por versión", "accounts_history\ncards_history"),
        ("core_movimientos (+ _legacy)", "números argentinos, monedas, husos,\ndos contratos, duplicados (DAT-01/02/05/06)", "transactions"),
        ("core_saldos", "saldo de cierre y conciliación\ncontra los movimientos (DAT-10)", "balances\nbalance_breaks"),
        ("cards_v1_* · cards_api_*", "v1 (centavos, epoch) + v2 en un modelo\nautorización ↔ liquidación", "card_authorizations\ncard_settlements"),
        ("fx_bcra · fx_mirror", "la real, con el espejo de respaldo\n(fx_source dice de cuál vino)", "fx_rates"),
        ("crm_account · crm_contact\ncrm_lead · crm_opportunity · crm_case", "match con el core por ID o CUIT,\nduplicadas, PII redactada (DAT-09/11/16/17/18)", "crm_accounts · crm_contacts · crm_leads\ncrm_opportunities · crm_cases"),
    ]
    for i, (b, fix, t) in enumerate(rows):
        y = 190 + i * 130
        d.box(f"b{i}", 40, y, 400, 100, b, "naranja", 17)
        d.box(f"f{i}", 500, y, 680, 100, fix, "violeta", 17)
        d.box(f"s{i}", 1240, y, 520, 100, t, "verde", 17)
        d.arrow(f"b{i}", f"f{i}", sides="rl")
        d.arrow(f"f{i}", f"s{i}", sides="rl")
    d.box("q", 1820, 450, 260, 300, "silver.quarantine\n\nfilas inválidas\ncon su motivo:\nno se pierden\nni se inventan", "rojo", icon="error")
    for i in (2, 3, 4):
        d.arrow(f"s{i}", "q", sides="rl", dashed=True, color="#e03131")
    return d


# ------------------------------------------------------------------------------------------ 04 gold y calidad
def gold() -> Diagram:
    d = Diagram("04_gold_calidad", "Fase 4 · Gold y quality gate")
    d.heading(40, 20, "Modelo dimensional en castellano, montos en decimal(18,2) con importe_ars e importe_usd al oficial del día")

    d.box("silver", 40, 420, 260, 160, "silver\n(limpio, con\nhistoria e identidad)", "verde", icon="delta")
    d.zone(360, 130, 1180, 870, "gold", "amarillo", icon="lakehouse")
    d.box("dims", 400, 190, 520, 300, "Dimensiones\n\ndim_fecha (feriados + dólar oficial)\ndim_sucursal · dim_cliente (SCD2)\ndim_cuenta · dim_tarjeta\ndim_comercio", "azul", 17, icon="delta")
    d.box("facts", 400, 530, 520, 200, "Hechos\n\nfact_movimiento\nfact_autorizacion (+ liquidación)\nfact_saldo_diario", "naranja", 17, icon="delta")
    d.box("crm", 400, 770, 520, 190, "CRM unido al core\n\ndim_cuenta_crm\nfact_oportunidad · fact_caso", "rosa", 17, icon="user")
    d.box("marts", 980, 190, 520, 300, "Marts (lo que usa Genie)\n\nmart_cliente_360\nmart_fraude_diario\nmart_tesoreria_mensual\nmart_conciliacion_tarjetas\nmart_pipeline_comercial", "verde", 17, icon="bar_chart")
    d.arrow("silver", "dims", sides="rl", via=[(340, 500), (340, 340)])
    d.arrow("silver", "facts", sides="rl", via=[(340, 500), (340, 630)])
    d.arrow("silver", "crm", sides="rl", via=[(340, 500), (340, 865)])
    d.arrow("dims", "marts", sides="rl")
    d.arrow("facts", "marts", sides="rl", via=[(950, 630), (950, 420)])
    d.arrow("crm", "marts", sides="rl", via=[(960, 865), (960, 460)])
    d.note(980, 560, 520, 120, "Las oportunidades y casos de una cuenta\nduplicada se atribuyen a su cuenta\nprincipal: para Comercial, una empresa\nes una sola.")

    d.zone(1600, 130, 560, 870, "quality_gate", "rojo", icon="settings")
    d.box("rules", 1640, 190, 480, 170, "22 reglas sobre silver, gold y ops\n9 bloqueantes · 13 avisos\ncada una con su ID del catálogo", "gris", icon="settings")
    d.box("dq", 1640, 820, 480, 90, "ops.dq_results\n(historia de cada corrida)", "gris", icon="bar_chart")
    d.box("ok", 1640, 420, 480, 140, "¿falló alguna\nbloqueante?", "amarillo", 20, shape="diamond")
    d.box("fail", 1640, 620, 220, 120, "la corrida falla:\nnadie ve datos\nrotos", "rojo", 17, icon="error")
    d.box("pass", 1900, 620, 220, 120, "Genie responde\ncon datos sanos", "verde", 17, icon="genie")
    d.arrow("marts", "rules", sides="rl", via=[(1570, 340), (1570, 275)])
    d.arrow("rules", "ok", sides="bt")
    d.arrow("ok", "fail", "sí", sides="lt", via=[(1620, 490), (1620, 600), (1750, 600)], seg=2)
    d.arrow("ok", "pass", "no", sides="rt", via=[(2140, 490), (2140, 600), (2010, 600)], seg=2)
    d.arrow("fail", "dq", sides="bt", dashed=True, via=[(1750, 790), (1880, 790)])
    d.arrow("pass", "dq", sides="bt", dashed=True, via=[(2010, 790), (1880, 790)])
    return d


# --------------------------------------------------------------------------------------- 05 semántica y Genie
def genie() -> Diagram:
    d = Diagram("05_semantica_genie", "Fase 5 · Capa semántica y Genie")
    d.heading(40, 20, "Gobierno y semántica en SQL versionado; los Genie Agents son código (genie/src/*.yml) con benchmarks validados")

    d.box("gold", 40, 430, 240, 140, "gold\n(tablas y marts)", "amarillo", icon="delta")
    d.zone(340, 130, 520, 870, "gobierno (sql/governance)", "gris", icon="unity_catalog")
    d.box("gov", 380, 200, 440, 300, "comentarios en tablas\ny columnas\n\nPK y FK\n\ntags por dominio\n\nmáscaras de PII\ny row filters", "gris", 17, icon="unity_catalog")
    d.box("sem", 380, 560, 440, 400, "semantic\n\nvistas v_*\n(movimientos, autorizaciones,\nAPIs, calidad, archivos)\n\nmetric views mv_*\n(clientes, depósitos,\nmovimientos, tarjetas, APIs)\n\nfunciones fn_*\n(alertas de fraude, posición\nen dólares, resumen de\ncliente, salud de ingesta)", "violeta", 17, icon="dashboards")
    d.arrow("gold", "gov", sides="rl", via=[(310, 500), (310, 350)])
    d.arrow("gold", "sem", sides="rl", via=[(310, 500), (310, 760)])

    d.zone(920, 130, 820, 870, "4 Genie Agents (uno por área)", "rosa", icon="genie")
    agents = [
        ("Comercial y clientes", "¿Cuánto pipeline abierto hay\npor producto en el CRM?"),
        ("Tarjetas y fraude", "¿Qué rubros tuvieron más rechazos\npor sospecha de fraude?"),
        ("Finanzas y tesorería", "¿Cuál es la posición en dólares\nal 30 de junio de 2026?"),
        ("Plataforma de datos", "¿Cómo estuvieron las APIs\nen los últimos 7 días?"),
    ]
    for i, (name, q) in enumerate(agents):
        y = 190 + i * 200
        d.box(f"a{i}", 960, y, 330, 160, f"Andes ·\n{name}", "rosa", 19)
        d.box(f"q{i}", 1330, y + 20, 370, 120, q, "blanco", 17)
        d.arrow(f"q{i}", f"a{i}", sides="lr", dashed=True)
        d.arrow("sem", f"a{i}", sides="rl", via=[(890, 760), (890, y + 80)])
    d.zone(1800, 130, 360, 870, "Personas", "azul", icon="user")
    d.box("users", 1840, 400, 280, 200, "cada área pregunta\nen castellano y ve\nel SQL que Genie\nusó para responder", "blanco", icon="user")
    d.box("code", 1840, 760, 280, 160, "Genie Code:\nayuda a construir\n(.assistant_instructions)", "gris", 17, icon="python")
    for i in range(4):
        d.arrow("users", f"q{i}", sides="lr", via=[(1770, 500), (1770, 190 + i * 200 + 80)])
    return d


# ------------------------------------------------------------------------------------- 06 despliegue y jobs
def despliegue() -> Diagram:
    d = Diagram("06_despliegue_jobs", "Fase 6 · Despliegue, CI y orquestación")
    d.heading(40, 20, "Todo como código: scripts de az para Azure, Asset Bundles para Databricks, GitHub Actions para CI")

    d.box("repo", 40, 200, 300, 120, "repo en GitHub\n(rama por etapa)", "gris", icon="github")
    d.box("ci", 40, 380, 300, 200, "GitHub Actions\n\nPR: ruff + pytest rápidos\nmain: tests con Spark\nbundle validate", "gris", 17, icon="github_actions")
    d.box("az", 40, 640, 300, 200, "scripts/azure\n01_infra · 02_secrets\n03_deploy_api\n99_teardown", "azul", 17, icon="azure")
    d.arrow("repo", "ci", sides="bt")
    d.zone(400, 130, 1760, 300, "Databricks · databricks bundle deploy -t dev", "verde", icon="databricks")
    d.text(440, 180, "Job andes-bank-fuentes", 20, "#e8590c")
    jf = [("f_setup", "setup"), ("f_gt", "generate_truth"), ("f_ex", "export_sources"), ("f_up", "upload_to_azure")]
    for i, (k, t) in enumerate(jf):
        d.box(k, 440 + i * 260, 220, 220, 70, t, "naranja", 17)
        if i:
            d.arrow(jf[i - 1][0], k, sides="rl")
    d.box("f_fx", 440, 320, 220, 70, "fx_mirror", "naranja", 17)
    d.box("f_crm", 960, 320, 220, 70, "crm_publish", "naranja", 17)
    d.arrow("f_setup", "f_fx", sides="bt")
    d.arrow("f_gt", "f_crm", sides="bl", via=[(810, 355)])
    d.text(1480, 180, "Job andes-bank-dia-nuevo · a pedido", 20, "#2f9e44")
    d.box("d_new", 1480, 220, 340, 80, "nuevo_dia: simula N días,\nlos exporta y los sube", "verde", 17)
    d.box("d_fx", 1860, 220, 260, 80, "fx_mirror → corre\nel lakehouse", "verde", 17)
    d.text(1480, 330, "los dos simulan el mundo exterior:\nno son parte del lakehouse", 17, MUTED)

    d.zone(400, 470, 1760, 420, "Job andes-bank-lakehouse (a pedido, hasta el último día simulado)", "verde", icon="jobs")
    d.box("l_setup", 440, 650, 180, 70, "setup", "gris", 17)
    ing = [("l_sync", "landing_sync", 530), ("l_api", "bronze_api", 620), ("l_fx", "bronze_fx", 710), ("l_crm", "bronze_crm", 800)]
    for k, t, y in ing:
        d.box(k, 680, y, 220, 60, t, "turquesa" if k == "l_sync" else "naranja", 17)
        d.arrow("l_setup", k, sides="rl", via=[(650, 685), (650, y + 30)])
    d.box("l_files", 940, 530, 220, 60, "bronze_files", "naranja", 17)
    d.arrow("l_sync", "l_files", sides="rl")
    chain = [("l_silver", "silver"), ("l_gold", "gold"), ("l_sem", "governance_semantic"), ("l_dq", "quality_gate")]
    for i, (k, t) in enumerate(chain):
        d.box(k, 1240 + i * 230, 650, 200 if k != "l_sem" else 210, 70, t,
              "rojo" if k == "l_dq" else ("violeta" if k == "l_sem" else ("amarillo" if k == "l_gold" else "gris")), 17)
        if i:
            d.arrow(chain[i - 1][0], k, sides="rl")
    for k in ("l_files", "l_api", "l_fx", "l_crm"):
        d.arrow(k, "l_silver", sides="rl", via=[(1200, d.boxes[k].cy), (1200, 685)])
    d.arrow("repo", "f_setup", "deploy", sides="rl")
    d.note(1240, 770, 880, 80, "Cada tarea es un notebook delgado que llama al paquete andes_bank (testeado con pytest).\nReintentos por tarea; el watermark retoma donde quedó.", "gris")
    d.arrow("az", "l_setup", "secretos", sides="rl")
    return d


# ------------------------------------------------------------------------------------- 07 la API y el cliente
def api() -> Diagram:
    d = Diagram("07_api_caos_cliente", "Detalle · La API con caos y el cliente que la sobrevive")
    d.heading(40, 20, "Cada falla del catálogo (docs/03, sección A) tiene su respuesta en ApiClient; nada se da por bueno sólo porque vino un 200")
    d.icon("function", 40, 128, 40)
    d.text(90, 140, "lo que hace la API (CHAOS_PROFILE=normal | dificil)", 22, "#e03131")
    d.icon("server", 1080, 128, 40)
    d.text(1124, 140, "lo que hace el cliente (ApiClient)", 22, "#2f9e44")
    rows = [
        ("API-01 · 429 con Retry-After", "espera lo que dice Retry-After + un poco de azar"),
        ("API-02 · 500 / 502 / 503", "backoff exponencial con jitter: 1, 2, 4, 8... s"),
        ("API-03 · tarda más que el timeout", "el GET es idempotente: reintenta"),
        ("API-04 · token vencido antes de tiempo", "renueva el token y repite EL MISMO pedido"),
        ("API-10 · JSON cortado a la mitad", "falla transitoria: reintenta"),
        ("API-12 · 200 con {\"status\": \"error\"}", "valida el cuerpo, no sólo el código HTTP"),
        ("API-05 · la misma página dos veces", "dedupe por id dentro del día"),
        ("API-06 · el cursor saltea una página", "concilia con daily-totals; si falta algo,\nre-pide el día al final de la corrida (R14)"),
    ]
    for i, (fail, fix) in enumerate(rows):
        y = 190 + i * 105
        d.box(f"p{i}", 40, y, 560, 80, fail, "rojo", 18)
        d.box(f"c{i}", 1080, y, 620, 80, fix, "verde", 18)
        d.arrow(f"p{i}", f"c{i}", sides="rl")
    d.box("log", 1760, 300, 380, 560, "ops.api_calls\n\ncada pedido, también\nlos fallidos: endpoint,\nstatus, intento,\nlatencia, error\n\nops.api_reconciliation\n\nesperado vs recibido\npor día", "gris", 18, icon="bar_chart")
    for i in (1, 6):
        d.arrow(f"c{i}", "log", "se registra" if i == 1 else None, sides="rl", dashed=True)
    d.note(40, 1050, 1660, 50, "Las fallas estructurales (página repetida o salteada) son estables 10 minutos para la misma página: re-pedirla enseguida da lo mismo.", "amarillo", 17)
    return d



# ------------------------------------------------------------------------------------ 08 línea de tiempo
def linea_de_tiempo() -> Diagram:
    d = Diagram("08_linea_de_tiempo", "La historia del banco (y por qué los datos vienen así)")
    d.heading(40, 20, "Cada hito deja una marca en los datos: el lakehouse tiene que entenderla, no esconderla")
    hitos = [  # (x, arriba?, fecha, qué pasó, qué deja en los datos, color, ícono)
        (80, True, "1998 · 2008", "nace en Cuyo;\ncompra el NOA", "la misma persona con\ndos números (DAT-08)", "naranja", "user"),
        (330, False, "ene 2023", "empieza la\nhistoria simulada", "start_date del\nscale_factor", "gris", "settings"),
        (560, True, "13 dic 2023", "devaluación", "saltan los montos en\npesos y el dólar oficial", "rojo", "bar_chart"),
        (790, False, "11 mar 2024", "migración\ndel core", "dos contratos: latin1 ';'\nvs. UTF-8 gzip (DAT-03)", "azul", "database"),
        (1020, True, "3 jun 2024", "CRM comercial\nnuevo", "cuentas sin ID del core\n(DAT-09, DAT-16)", "rosa", "user"),
        (1250, False, "2 ene 2025", "columna\ncanal_digital", "schema drift en\nmovimientos", "amarillo", "document"),
        (1480, True, "1 abr 2025", "API v2 del\nprocesador", "v1 y v2 a la vez hasta\njun-2025 (API-09)", "violeta", "function"),
        (1710, False, "may-nov 2025", "incidentes\nde entrega", "vacío, tarde, truncado,\nduplicado (ING-01..04)", "rojo", "error"),
        (1940, True, "1 mar 2026", "wallet_type\nen la API", "campo nuevo sin\naviso (API-11)", "violeta", "function"),
        (2170, False, "20 oct 2026", "fin de la historia;\nsigue a pedido", "job 'día nuevo': simula\nN días (daily.py)", "verde", "genie"),
    ]
    y_line = 520
    d.box("eje_i", 40, y_line - 4, 8, 8, "", "gris")
    d.box("eje_f", 2400, y_line - 4, 8, 8, "", "gris")
    d.arrow("eje_i", "eje_f", sides="rl", color="#495057")
    for i, (x, up, fecha, que, marca, color, icon) in enumerate(hitos):
        y = 150 if up else 600
        d.box(f"h{i}", x, y, 215, 190, f"{fecha}\n{que}", color, 17, icon=icon)
        d.text(x + 4, y + 200, marca, 14, "#495057")
        d.box(f"p{i}", x + 100, y_line - 8, 16, 16, "", color, shape="ellipse")
        d.arrow(f"h{i}", f"p{i}", sides="bt" if up else "tb", dashed=True, color="#868e96")
    return d


# ------------------------------------------------------------------------------------- 09 el generador
def generador() -> Diagram:
    d = Diagram("09_generador", "Cómo se genera el banco (la verdad: sim.*)")
    d.heading(40, 20, "Spark + una semilla: xxhash64(clave, sal) en vez de rand(), así el banco es el mismo en cualquier cluster")

    d.zone(40, 130, 520, 860, "entidades (entities.py)", "naranja", icon="python")
    ents = [("br", "sucursales (62)\n+ índice mensual", "gris", "settings"), ("cu", "clientes\n(SF 0,1 = 50 mil)", "naranja", "user"),
            ("ac", "cuentas\n(ARS, USD, corriente)", "naranja", "database"), ("ca", "tarjetas\n(débito y crédito)", "naranja", "key"),
            ("me", "comercios\n(rubro, país)", "naranja", "internet")]
    for i, (k, t, c, ic) in enumerate(ents):
        d.box(k, 80, 190 + i * 155, 440, 120, t, c, icon=ic)
    for a, b in (("cu", "ac"), ("ac", "ca")):
        d.arrow(a, b, sides="bt")

    d.zone(620, 130, 900, 860, "eventos (events.py): los sistemas están conectados", "violeta", icon="streaming")
    d.box("auth", 660, 190, 380, 130, "autorizaciones\n(procesador)", "violeta", icon="function")
    d.box("sett", 1100, 190, 380, 130, "liquidaciones\n1-3 días después", "violeta", icon="function")
    d.box("txn", 660, 470, 380, 150, "movimientos del core\ncon saldo posterior", "azul", icon="database")
    d.box("bal", 660, 780, 380, 130, "saldos de cierre", "azul", icon="bar_chart")
    d.box("pay", 1100, 470, 380, 150, "pago del resumen\nde crédito (día 10)", "gris", icon="settings")
    d.arrow("auth", "sett", "aprobadas", sides="rl")
    d.arrow("auth", "txn", "compras con\ndébito", sides="bt")
    d.arrow("sett", "pay", sides="bt")
    d.arrow("pay", "txn", sides="lr")
    d.arrow("txn", "bal", sides="bt")
    d.arrow("ca", "auth", sides="rl", via=[(590, 655), (590, 255)])
    d.arrow("me", "auth", sides="rl", via=[(600, 810), (600, 290)])
    d.note(1100, 700, 380, 130, "Así el lakehouse PUEDE (y tiene\nque) conciliar un sistema contra\notro: procesador vs. core,\nmovimientos vs. saldos.")

    d.zone(1580, 130, 620, 860, "exportes sucios (exports.py)", "azul", icon="document")
    d.box("x_core", 1620, 190, 540, 190, "core: CSV por día\nantes de mar-2024: latin1, ';', 1.234,56\ndespués: UTF-8, ',', gzip\n+ duplicados, faltantes, moneda mal", "azul", 17, icon="database")
    d.box("x_v1", 1620, 420, 540, 120, "procesador v1: JSON por día\ncentavos y epoch ms", "violeta", 17, icon="document")
    d.box("x_v2", 1620, 580, 540, 120, "API v2: JSONL por día\n(lo que sirve la Function)", "violeta", 17, icon="function")
    d.box("x_up", 1620, 760, 540, 150, "upload.py → Azure Blob\n+ incidentes: vacío, truncado,\nduplicado, el que llega tarde", "turquesa", 17, icon="blob")
    d.arrow("bal", "x_core", sides="bl", via=[(850, 960), (1550, 960), (1550, 285)])
    d.arrow("auth", "x_v1", sides="tl", via=[(850, 112), (1560, 112), (1560, 480)])
    d.arrow("x_v2", "x_up", sides="bt")
    return d


# ------------------------------------------------------------------------------------- 10 el borde (landing)
def landing() -> Diagram:
    d = Diagram("10_landing_borde", "El borde del lakehouse: qué pasa con cada archivo")
    d.heading(40, 20, "landing_sync lista Blob con una SAS de sólo lectura, valida cada archivo y deja todo en ops.landing_files")
    d.box("blob", 40, 380, 260, 140, "blob nuevo o\ncon otro etag", "azul", icon="blob")
    d.box("q1", 380, 370, 260, 160, "¿ya lo\nhabíamos copiado?", "amarillo", 18, shape="diamond")
    d.box("changed", 380, 640, 260, 130, "CHANGED\nalerta, no se copia", "rojo", 17, icon="error")
    d.box("q2", 720, 370, 260, 160, "¿descomprime?", "amarillo", 18, shape="diamond")
    d.box("corrupt", 720, 640, 260, 130, "CORRUPT\na _errors/ (ING-03)", "rojo", 17, icon="error")
    d.box("q3", 1060, 370, 260, 160, "¿mismo md5 que\notro aterrizado?", "amarillo", 18, shape="diamond")
    d.box("dup", 1060, 640, 260, 130, "DUPLICATE_CONTENT\nno se copia (ING-01)", "rojo", 17, icon="error")
    d.box("q4", 1400, 370, 260, 160, "¿tiene filas?", "amarillo", 18, shape="diamond")
    d.box("empty", 1400, 640, 260, 130, "EMPTY: se copia\ny alerta (ING-02)", "naranja", 17, icon="document")
    d.box("landed", 1740, 380, 300, 140, "LANDED\nal Volume", "verde", icon="storage")
    d.box("al", 1740, 640, 300, 130, "Auto Loader\nlo levanta", "naranja", icon="streaming")
    d.box("ops", 700, 860, 660, 100, "ops.landing_files: un registro por archivo, con su estado y su detalle", "gris", 17, icon="bar_chart")
    d.arrow("blob", "q1", sides="rl")
    d.arrow("q1", "changed", "sí", sides="bt")
    d.arrow("q1", "q2", "no", sides="rl")
    d.arrow("q2", "corrupt", "no", sides="bt")
    d.arrow("q2", "q3", "sí", sides="rl")
    d.arrow("q3", "dup", "sí", sides="bt")
    d.arrow("q3", "q4", "no", sides="rl")
    d.arrow("q4", "empty", "no", sides="bt")
    d.arrow("q4", "landed", "sí", sides="rl")
    d.arrow("landed", "al", sides="bt")
    d.arrow("empty", "al", sides="rl")
    d.note(40, 140, 600, 110, "¿Por qué copiar y no leer Blob directo?\nFree Edition no permite external locations\nhacia tu storage (ADR 0002). Copiar a un\nVolume con SAS de lectura es un patrón real.")
    return d


# --------------------------------------------------------------------------------- 11 cargar sólo lo nuevo
def incremental() -> Diagram:
    d = Diagram("11_incremental", "Cargar sólo lo nuevo: cada fuente recuerda hasta dónde llegó")
    d.heading(40, 20, "Re-correr cualquier tarea no duplica nada: cada escritura es idempotente")
    rows = [
        ("files", "archivos del core\ny procesador v1", "blob", "Auto Loader + checkpoint\n(qué archivos ya leyó)",
         "streaming", "trigger availableNow:\nlee lo nuevo y termina", "_checkpoints/bronze_*"),
        ("api", "API de tarjetas v2\n(por día)", "function", "ops.api_watermarks\nel último día COMPLETO",
         "server", "un día que no cierra contra\nlos totales no avanza (R14)", "replace_where por día"),
        ("crm", "API del CRM\n(por SystemModstamp)", "user", "ops.crm_watermarks\nel mayor SystemModstamp",
         "server", "pide >= watermark (con\nsolapamiento) e incluye borrados", "MERGE por Id: gana el más nuevo"),
        ("silver", "silver de hechos\n(movimientos, saldos)", "delta", "ops.silver_watermarks\nmayor _ingested_at",
         "delta", "procesa sólo filas nuevas\nde bronze", "MERGE insert-only: gana\nla primera copia"),
    ]
    d.text(60, 130, "fuente", 22, "#1971c2")
    d.text(500, 130, "cómo sabe hasta dónde llegó", 22, "#6741d9")
    d.text(1000, 130, "la regla", 22, "#e8590c")
    d.text(1500, 130, "escritura idempotente", 22, "#2f9e44")
    for i, (k, src, ic1, mark, ic2, rule, write) in enumerate(rows):
        y = 180 + i * 190
        d.box(f"s{k}", 40, y, 400, 150, src, "azul", icon=ic1)
        d.box(f"m{k}", 480, y, 440, 150, mark, "violeta", 17, icon=ic2)
        d.box(f"r{k}", 960, y, 460, 150, rule, "amarillo", 17)
        d.box(f"w{k}", 1460, y, 440, 150, write, "verde", 17, icon="delta")
        d.arrow(f"s{k}", f"m{k}", sides="rl")
        d.arrow(f"m{k}", f"r{k}", sides="rl")
        d.arrow(f"r{k}", f"w{k}", sides="rl")
    return d


# ------------------------------------------------------------------------------- 12 el día incompleto (R14)
def dia_incompleto() -> Diagram:
    d = Diagram("12_dia_incompleto_r14", "R14 · El bug que encontró el quality gate (y no los tests)")
    d.heading(40, 20, "Una página salteada es estable 10 minutos: reintentar enseguida da lo mismo. Hay que seguir y volver después")
    d.zone(40, 130, 1000, 520, "antes: reintentos inmediatos", "rojo", icon="error")
    d.box("a1", 80, 200, 280, 120, "día 15-jul\npasada 1: 6.000/7.000", "rojo", 17)
    d.box("a2", 400, 200, 280, 120, "pasada 2, 3, 4\nla misma página falta", "rojo", 17)
    d.box("a3", 720, 200, 280, 120, "la ingesta se corta\nen el primer día malo", "rojo", 17, icon="error")
    d.arrow("a1", "a2", "enseguida", sides="rl")
    d.arrow("a2", "a3", sides="rl")
    d.note(80, 380, 920, 230, "Síntoma en producción: el quality gate frena con dias_api_completos (2 filas).\nA 2 días les faltan EXACTAMENTE 1.000 registros: una página.\n\nEn los tests pasaba de casualidad: el caos dependía del reloj real.")
    d.zone(1100, 130, 1060, 520, "después: seguir, y volver en rondas", "verde", icon="settings")
    d.box("b1", 1140, 200, 300, 120, "lote de 30 días\n(4 en paralelo)", "verde", 17, icon="server")
    d.box("b2", 1480, 200, 300, 120, "los incompletos\nse anotan y se sigue", "amarillo", 17)
    d.box("b3", 1820, 200, 300, 120, "al final: hasta 3 rondas\ncon 4 min de espera", "verde", 17)
    d.arrow("b1", "b2", sides="rl")
    d.arrow("b2", "b3", sides="rl")
    d.box("b4", 1480, 400, 640, 140, "el watermark avanza sólo hasta el último día\ncompleto SIN huecos antes: lo que falte,\nla próxima corrida lo vuelve a pedir", "violeta", 17, icon="server")
    d.arrow("b3", "b4", sides="bt")
    d.note(40, 700, 2120, 70, "Lección: el quality gate encontró el bug, no los tests. Los tests ahora usan un reloj simulado que avanza con la espera entre rondas.", "amarillo", 18)
    return d


# ------------------------------------------------------------------------------------ 13 SCD2 e identidad
def scd2() -> Diagram:
    d = Diagram("13_scd2_identidad", "Silver II · Historia (SCD2) e identidad")
    d.heading(40, 20, "Del CDC del core a 'qué sabíamos de este cliente en cada momento', y de dos números de cliente a una persona")
    d.zone(40, 130, 1140, 600, "SCD2: una versión nueva sólo si cambió algo", "azul", icon="database")
    evs = [("e1", "I · 2019\nalta en Cuyo", "azul"), ("e2", "U · 2023\ncambia de segmento", "azul"),
           ("e3", "I · mar-2024\nrecarga del core nuevo", "gris"), ("e4", "D · 2025\nbaja", "rojo")]
    for i, (k, t, c) in enumerate(evs):
        d.box(k, 80 + i * 270, 200, 240, 110, t, c, 17)
    for a, b in (("e1", "e2"), ("e2", "e3"), ("e3", "e4")):
        d.arrow(a, b, sides="rl")
    vers = [("v1", "versión 1\nvalid_from 2019\nvalid_to 2023", "verde"), ("v2", "versión 2\nvalid_from 2023\nvalid_to 2025", "verde"),
            ("v4", "versión 3 · BAJA\nis_current = true", "rojo")]
    xs = {"v1": 80, "v2": 350, "v4": 890}
    for k, t, c in vers:
        d.box(k, xs[k], 420, 240, 130, t, c, 17, icon="delta")
    d.arrow("e1", "v1", sides="bt")
    d.arrow("e2", "v2", sides="bt")
    d.arrow("e4", "v4", sides="bt")
    d.box("x3", 620, 420, 240, 130, "no abre versión:\nmismo hash (R08)", "gris", 17, dashed=True)
    d.arrow("e3", "x3", sides="bt", dashed=True)
    d.note(80, 590, 1060, 80, "R08: el core nuevo mandaba 'Belén' y el viejo 'Belen'. El control de cambios usa el nombre NORMALIZADO\n(sin acentos, mayúsculas): el nombre para mostrar es tipo 1 y no abre versiones.")
    d.zone(1240, 130, 920, 600, "identidad: la fusión de 2008 (DAT-08)", "naranja", icon="user")
    d.box("c1", 1280, 200, 380, 120, "CUY-0001234\nBelén Pérez · DNI 30.111.222", "naranja", 17, icon="user")
    d.box("c2", 1280, 360, 380, 120, "NOA-0045678\nBELEN PEREZ · DNI 30.111.222", "naranja", 17, icon="user")
    d.box("rule", 1720, 270, 400, 140, "mismo DNI +\nmismo nombre normalizado", "amarillo", 17, shape="diamond")
    d.box("p", 1720, 470, 400, 160, "persona_id = CUY-0001234\n(el número más viejo)\ncustomer_xref", "verde", 17, icon="delta")
    d.arrow("c1", "rule", sides="rl")
    d.arrow("c2", "rule", sides="rl")
    d.arrow("rule", "p", sides="bt")
    d.note(1280, 540, 400, 110, "Gold cuenta PERSONAS, no\nnúmeros de cliente: 'cuántos\nclientes tenemos' no se infla.")
    return d


# ------------------------------------------------------------------------------------- 14 modelo estrella
def estrella() -> Diagram:
    d = Diagram("14_modelo_estrella", "Gold · El modelo dimensional (y los marts que usa Genie)")
    d.heading(40, 20, "Hechos al centro, dimensiones alrededor. Montos en su moneda y SIEMPRE también importe_ars e importe_usd")
    d.box("f_mov", 760, 330, 340, 130, "fact_movimiento\nun movimiento de cuenta", "naranja", 17, icon="delta")
    d.box("f_aut", 760, 510, 340, 130, "fact_autorizacion\n(+ su liquidación)", "naranja", 17, icon="delta")
    d.box("f_sal", 760, 690, 340, 130, "fact_saldo_diario\nsaldo de cierre informado", "naranja", 17, icon="delta")
    dims = [("d_fecha", 260, 200, "dim_fecha\nferiados + dólar oficial"), ("d_cli", 260, 420, "dim_cliente\nSCD2 + persona"),
            ("d_cta", 260, 640, "dim_cuenta"), ("d_suc", 260, 860, "dim_sucursal"),
            ("d_tar", 1300, 300, "dim_tarjeta"), ("d_com", 1300, 520, "dim_comercio\nrubro, país")]
    for k, x, y, t in dims:
        d.box(k, x, y, 320, 120, t, "azul", 17, icon="database")
    for f in ("f_mov", "f_sal"):
        for k in ("d_fecha", "d_cta"):
            d.arrow(f, k, sides="lr", dashed=True, color="#1971c2")
    d.arrow("f_mov", "d_cli", sides="lr", dashed=True, color="#1971c2")
    d.arrow("d_cta", "d_suc", sides="bt", dashed=True, color="#1971c2")
    for k in ("d_tar", "d_com"):
        d.arrow("f_aut", k, sides="rl", dashed=True, color="#1971c2")
    d.arrow("f_aut", "d_fecha", sides="lr", dashed=True, color="#1971c2")
    d.zone(1720, 130, 440, 860, "marts (tablas anchas)", "verde", icon="bar_chart")
    marts = [("mart_cliente_360", "Comercial"), ("mart_fraude_diario", "Tarjetas y fraude"),
             ("mart_tesoreria_mensual", "Finanzas"), ("mart_conciliacion_tarjetas", "Operaciones"),
             ("mart_pipeline_comercial", "Comercial (CRM)")]
    for i, (m, area) in enumerate(marts):
        d.box(f"m{i}", 1760, 190 + i * 155, 360, 120, f"{m}\n{area}", "verde", 16, icon="genie")
    d.box("crm", 760, 880, 340, 110, "CRM: dim_cuenta_crm\nfact_oportunidad · fact_caso", "rosa", 16, icon="user")
    return d


# ----------------------------------------------------------------------------------------- 15 pirámide Genie
def piramide_genie() -> Diagram:
    d = Diagram("15_piramide_genie", "Qué hace que Genie responda bien (y qué arreglar primero)")
    d.heading(40, 20, "Si un benchmark falla, se arregla de abajo hacia arriba: las instrucciones del agent son lo último")
    capas = [  # de arriba (lo último) a abajo (lo primero)
        ("instrucciones del agent", "reglas de negocio que no están en los datos", "rosa", "genie"),
        ("ejemplos SQL", "preguntas típicas con su SQL correcto", "violeta", "notebooks"),
        ("funciones certificadas fn_*", "lógica que no se puede equivocar (fraude, posición USD)", "violeta", "settings"),
        ("metric views mv_*", "las métricas oficiales, con sinónimos", "azul", "dashboards"),
        ("comentarios en tablas y columnas", "qué significa cada cosa, en castellano", "turquesa", "unity_catalog"),
        ("gold con el grano correcto", "marts anchos, nombres claros, montos en ARS y USD", "amarillo", "delta"),
        ("datos limpios y conciliados", "silver + quality gate", "verde", "lakehouse"),
    ]
    for i, (t, sub, c, ic) in enumerate(capas):
        w = 620 + i * 170
        x = 880 - w / 2
        d.box(f"c{i}", x, 150 + i * 120, w, 100, f"{t}\n{sub}", c, 18, icon=ic)
    d.box("bench", 1780, 150, 380, 220, "benchmarks\n(genie/src/*.yml)\nmedidos con\nscripts/genie_benchmark.py", "gris", 17, icon="bar_chart")
    d.arrow("bench", "c0", "si falla uno...", sides="lr", dashed=True)
    d.box("orden", 1780, 520, 380, 300, "...se mira primero\nla base: ¿el dato\nestá bien?, ¿el grano?,\n¿el comentario?,\n¿hay una métrica?", "amarillo", 17)
    d.arrow("bench", "orden", sides="bt")
    return d


# ------------------------------------------------------------------------------------ 16 operar con el caos
def operacion() -> Diagram:
    d = Diagram("16_operacion", "Operar con el caos: falló una corrida del lakehouse")
    d.heading(40, 20, "Primero: ¿qué tarea falló? Cada una tiene su tabla de ops para mirar y su arreglo típico")
    d.box("mail", 40, 420, 280, 140, "mail: falló\nandes-bank-lakehouse", "rojo", icon="error")
    casos = [
        ("landing_sync", "ops.landing_files", "SAS vencida o rotada →\nscripts/azure/02_secrets.sh", "storage"),
        ("bronze_api / bronze_crm", "ops.api_calls\nops.api_reconciliation", "API caída: la tarea reintenta sola;\nlos días incompletos vuelven mañana", "server"),
        ("bronze_files", "_rescued_data\nUNKNOWN_FIELD", "columna nueva: el stream se\nreinicia con el esquema nuevo", "streaming"),
        ("silver / gold", "silver.quarantine", "un formato nuevo del origen:\nfunción nueva en transforms.py", "delta"),
        ("quality_gate", "ops.dq_results", "regla fail → su ID del catálogo\n→ dónde se arregla (docs/03)", "settings"),
    ]
    for i, (task, mira, arreglo, ic) in enumerate(casos):
        y = 140 + i * 175
        d.box(f"t{i}", 420, y, 400, 140, task, "naranja", 18, icon=ic)
        d.box(f"m{i}", 880, y, 440, 140, f"mirá\n{mira}", "gris", 17, icon="bar_chart")
        d.box(f"a{i}", 1380, y, 520, 140, arreglo, "verde", 17)
        d.arrow("mail", f"t{i}", sides="rl", via=[(370, 490), (370, y + 70)])
        d.arrow(f"t{i}", f"m{i}", sides="rl")
        d.arrow(f"m{i}", f"a{i}", sides="rl")
    d.box("genie", 1960, 420, 200, 140, "o preguntale\nal Genie de\nPlataforma", "rosa", 17)
    d.note(40, 1040, 2120, 60, "Regla de oro: re-correr cualquier tarea es seguro. Todas las escrituras son idempotentes (watermarks, MERGE, replace_where).", "amarillo", 18)
    return d


# ------------------------------------------------------------------------------------- 17 en números
def en_numeros() -> Diagram:
    d = Diagram("17_en_numeros", "Andes Bank en números")
    d.heading(40, 20, "Un banco argentino simulado, de punta a punta en Databricks Free Edition (números reales del proyecto)")
    bench = ROOT_GENIE / "benchmarks_dev.json"
    pct = {}
    if bench.exists():
        pct = {k: v["pct"] for k, v in json.loads(bench.read_text(encoding="utf-8")).items()}
    tarjetas = [
        ("datos", "17,2 M\nmovimientos (SF 0,1)\n172 M a SF 1", "azul", "database"),
        ("api", "6.151 pedidos a la API\n163 rate limits, 106 5xx,\n78 tokens vencidos", "violeta", "function"),
        ("archivos", "8.747 archivos\n1 corrupto, 1 vacío,\n1 duplicado: detectados", "turquesa", "blob"),
        ("calidad", "22 reglas de calidad\n9 bloqueantes\n22 de 22 en verde", "verde", "settings"),
        ("costo", "USD 0,02\nde Azure en el mes\n(tope: USD 10)", "amarillo", "azure"),
        ("tests", "55 tests\nel e2e compara contra\nla verdad del generador", "gris", "python"),
        ("problemas", "37 problemas\ncatalogados, cada uno\ncon su arreglo", "rojo", "error"),
        ("bitacora", "18 errores reales\nen la bitácora,\ncon causa y arreglo", "naranja", "document"),
    ]
    for i, (k, t, c, ic) in enumerate(tarjetas):
        x, y = 40 + (i % 4) * 540, 150 + (i // 4) * 260
        d.box(k, x, y, 500, 220, t, c, 22, icon=ic, icon_size=72)
    d.zone(40, 700, 2120, 300, "Genie: % de benchmarks que acierta cada agent (objetivo 80 %)", "rosa", icon="genie")
    nombres = {"01_comercial_clientes": "Comercial", "02_tarjetas_fraude": "Tarjetas y fraude",
               "03_finanzas_tesoreria": "Finanzas", "04_plataforma_datos": "Plataforma de datos"}
    for i, (slug, nombre) in enumerate(nombres.items()):
        v = pct.get(slug)
        color = "verde" if v is not None and v >= 80 else "rojo"
        d.box(f"g{i}", 80 + i * 520, 770, 480, 190, f"{nombre}\n{v if v is not None else '-'} %", color, 26, icon="genie", icon_size=72)
    return d


# --------------------------------------------------------------------------------------- 99 catálogo de íconos
def iconos() -> Diagram:
    d = Diagram("99_iconos", "Íconos disponibles")
    d.heading(40, 20, "De la biblioteca pública de Excalidraw (scripts/diagramas/bajar_iconos.py): usalos con icon=\"<clave>\"")
    for i, (key, meta) in enumerate(_icons().items()):
        x, y = 40 + (i % 8) * 260, 140 + (i // 8) * 220
        iw, ih = d.icon_dims(key, 90)
        d.icon(key, x + (220 - iw) / 2, y + (100 - ih) / 2, 90)
        d.text(x + 10, y + 120, key, 20)
        d.text(x + 10, y + 148, meta["biblioteca"].split("/")[0], 14, MUTED)
    return d


DIAGRAMS = [panorama, fuentes, ingesta, silver, gold, genie, despliegue, api, linea_de_tiempo, generador, landing, incremental, dia_incompleto, scd2, estrella, piramide_genie, operacion, en_numeros, iconos]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    only = sys.argv[1:]
    for fn in DIAGRAMS:
        d = fn()
        if only and not any(d.name.startswith(o) for o in only):
            continue
        print("✔", d.save(OUT).relative_to(OUT.parents[1]))


if __name__ == "__main__":
    main()
