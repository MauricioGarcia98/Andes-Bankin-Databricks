"""Genera los diagramas del proyecto en docs/diagramas/*.excalidraw.

    python scripts/diagramas/generar.py                 # todos
    cd scripts/diagramas && npm install && node exportar.mjs     # → .svg (los que muestra GitHub)

Un diagrama por fase del proyecto. Si cambia la arquitectura, se cambia acá y se regenera: el diagrama es código,
igual que el resto.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from excali import MUTED, Diagram, _icons  # noqa: E402

OUT = Path(__file__).resolve().parents[2] / "docs" / "diagramas"


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

    d.note(40, 980, 2140, 70, "Orquestación: 2 Lakeflow Jobs (fuentes y lakehouse)  ·  Despliegue: Databricks Asset Bundles + scripts de az  ·  "
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
    d.box("rules", 1640, 190, 480, 170, "24 reglas sobre silver y gold\n11 bloqueantes · 13 avisos\ncada una con su ID del catálogo", "gris", icon="settings")
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
    d.text(1420, 330, "simula el mundo exterior:\nno es parte del lakehouse", 17, MUTED)

    d.zone(400, 470, 1760, 420, "Job andes-bank-lakehouse (diario, serverless)", "verde", icon="jobs")
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


DIAGRAMS = [panorama, fuentes, ingesta, silver, gold, genie, despliegue, api, iconos]


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
