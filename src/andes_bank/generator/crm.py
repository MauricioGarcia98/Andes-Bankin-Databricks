"""Los datos del CRM comercial: cuentas, contactos, leads, oportunidades y casos.

El CRM es un sistema origen simulado con el modelo de datos de Salesforce (Account, Contact, Lead, Opportunity, Case,
campos custom `__c`, `Id`, `SystemModstamp`, `IsDeleted`). Lo sirve la API de Azure (`/crm/v1/<Objeto>`) a partir de
`crm-data/<Objeto>.json`, que arma `crm_system` y sube el notebook 05.

El CRM NO escala con el scale factor: tiene siempre ~1.900 registros (lo que carga a mano el equipo comercial de un
banco regional). Se arma en Python puro (es chico) a partir de una muestra fija de clientes de `sim.customers`, con
una semilla: la misma muestra, la misma suciedad y los mismos Id y fechas cada vez.

Cada registro trae `Andes_Key__c`, la clave de carga, y los hijos apuntan a su cuenta con `Account.Andes_Key__c`;
`crm_system` lo traduce a los `Id` y `AccountId` del CRM. Las columnas `_true_*` son la verdad (no se publican).

Suciedad (docs/03):
  * DAT-09 `Core_Customer_Id__c` vacío (12 %), con el número del banco viejo (6 %) o con dos dígitos cambiados (4 %).
  * DAT-16 cuentas duplicadas: la misma empresa cargada dos veces por dos ejecutivos (otro formato de nombre).
  * DAT-17 CUIT con y sin guiones, o vacío.
  * DAT-11 datos personales escritos a mano en la descripción de los casos.
  * DAT-18 leads de gente que YA es cliente (cross-sell mal cargado como prospecto).
"""

from __future__ import annotations

import random
import unicodedata
from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from andes_bank.config import ProjectConfig
from andes_bank.generator import reference as ref

N_ACCOUNTS_FROM_CORE = 460
N_PROSPECTS = 40
N_DUPLICATES = 15
N_LEADS = 250
N_OPPORTUNITIES = 350
N_CASES = 200

INDUSTRIES = ["Agriculture", "Retail", "Transportation", "Construction", "Manufacturing", "Food & Beverage",
              "Technology", "Hospitality", "Healthcare", "Energy"]
PRODUCTS = {  # producto: (monto mínimo, monto máximo) en pesos de 2025
    "Préstamo PyME": (5_000_000, 120_000_000), "Tarjeta Black": (0, 0), "Plazo Fijo": (2_000_000, 80_000_000),
    "Cuenta Sueldo": (0, 0), "Comercio Exterior": (10_000_000, 300_000_000), "Seguro de Vida": (200_000, 2_000_000),
    "Préstamo Personal": (1_000_000, 25_000_000),
}
STAGES = ["Prospecting", "Qualification", "Needs Analysis", "Proposal/Price Quote", "Negotiation/Review",
          "Closed Won", "Closed Lost"]
STAGE_WEIGHTS = [10, 10, 8, 10, 7, 35, 20]
CASE_SUBJECTS = ["Reclamo por débito no reconocido", "Consulta por límite de tarjeta", "Pedido de baja de tarjeta",
                 "Problema con transferencia", "Reclamo por comisión", "Consulta por plazo fijo",
                 "Posible fraude en tarjeta", "Actualización de datos", "Demora en acreditación de sueldo"]
TITLES = ["Gerente General", "Responsable de Administración", "Tesorero", "Socio Gerente", "Contador", "Dueño"]
LEAD_SOURCES = ["Web", "Phone Inquiry", "Partner Referral", "Purchased List", "Other"]
LEAD_STATUSES = ["Open - Not Contacted", "Working - Contacted", "Closed - Not Converted"]


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _cuit_fmt(cuit: str, r: random.Random) -> str | None:
    x = r.random()
    if x < 0.10:
        return None
    if x < 0.60:
        return f"{cuit[:2]}-{cuit[2:10]}-{cuit[10:]}"
    return cuit


def _dirty_core_id(customer_id: str, customer_key: int, r: random.Random) -> tuple[str | None, str]:
    """Devuelve (valor cargado, tipo de suciedad)."""
    x = r.random()
    if x < 0.12:
        return None, "VACIO"
    if x < 0.18:
        return f"MZA{customer_key:06d}", "BANCO_VIEJO"          # numeración del sistema anterior a la migración
    if x < 0.22:
        digits = list(customer_id[-7:])
        i = r.randrange(len(digits) - 1)
        if digits[i] != digits[i + 1]:
            digits[i], digits[i + 1] = digits[i + 1], digits[i]
            return customer_id[:-7] + "".join(digits), "TIPEO"
    return customer_id, "OK"


def _sf_date(d: date) -> str:
    return d.isoformat()


def crm_records(customers: list[dict], seed: int) -> dict[str, list[dict]]:
    """Arma el CRM a partir de una muestra de clientes (dicts de `sim.customers`)."""
    r = random.Random(seed * 7 + 3)
    out: dict[str, list[dict]] = {"accounts": [], "contacts": [], "leads": [], "opportunities": [], "cases": []}

    # --------------------------------------------------------------------------------------- cuentas
    for i, c in enumerate(customers, start=1):
        name = c["full_name"]
        if r.random() < 0.20:
            name = name.upper()
        elif r.random() < 0.10:
            name = _strip_accents(name)
        core_id, dirt = _dirty_core_id(c["customer_id"], c["customer_key"], r)
        out["accounts"].append({
            "Andes_Key__c": f"ACC-{i:05d}", "Name": name,
            "Type": "Customer - Direct", "Industry": r.choice(INDUSTRIES) if c["person_type"] == "JURIDICA" else None,
            "BillingState": c["province"], "BillingCity": c["city"], "Phone": c["phone"],
            "CUIT__c": _cuit_fmt(c["cuit"], r), "Core_Customer_Id__c": core_id,
            "Segmento__c": c["segment"] if r.random() > 0.15 else r.choice(ref.SEGMENTS),  # a veces desactualizado
            "_true_customer_id": c["customer_id"], "_true_cuit": c["cuit"], "_dirt": dirt,
        })
    for _ in range(N_PROSPECTS):
        i = len(out["accounts"]) + 1
        name = " ".join([r.choice(ref.COMPANY_WORDS), r.choice(ref.LAST_NAMES), r.choice(["S.A.", "S.R.L.", "S.A.S."])])
        out["accounts"].append({
            "Andes_Key__c": f"ACC-{i:05d}", "Name": name, "Type": "Prospect", "Industry": r.choice(INDUSTRIES),
            "BillingState": r.choice(["Mendoza", "Salta", "Córdoba", "Tucumán", "San Juan"]), "BillingCity": None,
            "Phone": None, "CUIT__c": f"30{r.randrange(50_000_000, 79_999_999)}{r.randrange(10)}",
            "Core_Customer_Id__c": None, "Segmento__c": "PYME",
            "_true_customer_id": None, "_true_cuit": None, "_dirt": "PROSPECTO",
        })
    for src in r.sample(out["accounts"][:len(customers)], N_DUPLICATES):          # DAT-16
        i = len(out["accounts"]) + 1
        dup = dict(src, **{"Andes_Key__c": f"ACC-{i:05d}", "Name": src["Name"].title() + " ", "Phone": None,
                           "Core_Customer_Id__c": None, "_dirt": "DUPLICADA"})
        out["accounts"].append(dup)

    accounts = out["accounts"]
    by_key = {a["Andes_Key__c"]: a for a in accounts}
    cust_by_id = {c["customer_id"]: c for c in customers}

    # ------------------------------------------------------------------------------------- contactos
    n = 0
    for a in accounts:
        c = cust_by_id.get(a["_true_customer_id"])
        people = []
        if c and c["person_type"] == "HUMANA":
            people.append((c["first_name"], c["last_name"], c["email"], c["phone"], "Titular"))
        else:
            for _ in range(1 if r.random() < 0.7 else 2):
                fn = r.choice(ref.FIRST_NAMES_F + ref.FIRST_NAMES_M)
                ln = r.choice(ref.LAST_NAMES)
                mail = _strip_accents(f"{fn}.{ln}").lower() + "@" + r.choice(ref.EMAIL_DOMAINS)
                people.append((fn, ln, mail, None, r.choice(TITLES)))
        for fn, ln, mail, phone, title in people:
            n += 1
            out["contacts"].append({
                "Andes_Key__c": f"CON-{n:05d}", "Account.Andes_Key__c": a["Andes_Key__c"], "FirstName": fn,
                "LastName": ln, "Email": mail if r.random() > 0.08 else None, "Phone": phone, "Title": title,
            })

    # ----------------------------------------------------------------------------------------- leads
    for i in range(1, N_LEADS + 1):
        existing = r.random() < 0.30 and customers                                  # DAT-18
        if existing:
            c = r.choice(customers)
            fn, ln, cuit = c["first_name"], c["last_name"], c["cuit"]
            company = c["full_name"] if c["person_type"] == "JURIDICA" else f"{fn} {ln}"
        else:
            fn, ln = r.choice(ref.FIRST_NAMES_F + ref.FIRST_NAMES_M), r.choice(ref.LAST_NAMES)
            company = " ".join([r.choice(ref.COMPANY_WORDS), ln, r.choice(["S.A.", "S.R.L."])])
            cuit = f"30{r.randrange(50_000_000, 79_999_999)}{r.randrange(10)}"
        out["leads"].append({
            "Andes_Key__c": f"LEA-{i:05d}", "FirstName": fn, "LastName": ln, "Company": company,
            "Email": _strip_accents(f"{fn}.{ln}{i}").lower() + "@" + r.choice(ref.EMAIL_DOMAINS),
            "LeadSource": r.choice(LEAD_SOURCES), "Status": r.choice(LEAD_STATUSES),
            "CUIT__c": _cuit_fmt(cuit, r), "_true_is_customer": bool(existing),
        })

    # --------------------------------------------------------------------------------- oportunidades
    real_accounts = [a for a in accounts if a["_dirt"] != "DUPLICADA"]
    start = ref.CRM_GO_LIVE
    for i in range(1, N_OPPORTUNITIES + 1):
        a = r.choice(real_accounts)
        product = r.choice(list(PRODUCTS))
        lo, hi = PRODUCTS[product]
        stage = r.choices(STAGES, STAGE_WEIGHTS)[0]
        close = start + timedelta(days=r.randrange(0, 900))
        amount = round(r.uniform(lo, hi), -3) if hi else None
        if amount and r.random() < 0.03:
            amount = None                                                           # monto sin cargar
        out["opportunities"].append({
            "Andes_Key__c": f"OPP-{i:05d}", "Account.Andes_Key__c": a["Andes_Key__c"],
            "Name": f"{product} - {by_key[a['Andes_Key__c']]['Name'].strip()}"[:120], "StageName": stage,
            "CloseDate": _sf_date(close), "Amount": amount, "Producto__c": product,
        })

    # ----------------------------------------------------------------------------------------- casos
    for i in range(1, N_CASES + 1):
        a = r.choice(real_accounts[:len(customers)] or real_accounts)
        c = cust_by_id.get(a["_true_customer_id"])
        subject = r.choice(CASE_SUBJECTS)
        desc = f"{subject}. Cliente se comunica por {r.choice(['teléfono', 'mail', 'sucursal', 'chat'])}."
        if c and r.random() < 0.12:                                                 # DAT-11
            desc += r.choice([f" Tel de contacto {c['phone']}.", f" Responder a {c['email']}."])
        out["cases"].append({
            "Andes_Key__c": f"CAS-{i:05d}", "Account.Andes_Key__c": a["Andes_Key__c"], "Subject": subject,
            "Description": desc, "Status": r.choices(["New", "Working", "Escalated", "Closed"], [15, 20, 5, 60])[0],
            "Priority": r.choices(["High", "Medium", "Low"], [20, 50, 30])[0],
            "Origin": r.choice(["Phone", "Email", "Web"]),
        })
    return out


def crm_sample(spark: SparkSession, cfg: ProjectConfig) -> list[dict]:
    """Muestra fija de clientes del CRM: PyMEs y Premium (los que atiende un ejecutivo), activos y no duplicados."""
    cols = ["customer_key", "customer_id", "person_type", "segment", "first_name", "last_name", "full_name", "cuit",
            "province", "city", "phone", "email"]
    df = (spark.table(cfg.table("sim", "customers"))
          .where("segment IN ('PYME', 'PREMIUM') AND NOT is_legacy_duplicate AND closed_date IS NULL")
          .withColumn("_o", F.xxhash64(F.lit(cfg.seed), F.col("customer_key")))
          .orderBy("_o").limit(N_ACCOUNTS_FROM_CORE).select(*cols))
    return sorted((row.asDict() for row in df.collect()), key=lambda c: c["customer_key"])


def generate_crm(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    """Escribe el CRM en `sim.crm_<objeto>` (columnas del CRM + verdad). `crm_state` lo lee de acá para publicarlo."""
    recs = crm_records(crm_sample(spark, cfg), cfg.seed)
    counts = {}
    for obj, rows in recs.items():
        keys = sorted({k for row in rows for k in row})
        data = [tuple(None if row.get(k) is None else str(row[k]) for k in keys) for row in rows]
        # "Account.Andes_Key__c" (relación por clave externa) se guarda como Account_Andes_Key__c: Spark no admite
        # puntos en los nombres de columna; crm_state lo vuelve a la sintaxis original
        schema = ", ".join(f"`{k.replace('.', '_')}` string" for k in keys)
        spark.createDataFrame(data, schema).write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(cfg.table("sim", f"crm_{obj}"))
        counts[obj] = len(rows)
    return counts


# ------------------------------------------------------------------------------------- el sistema CRM
OBJECTS = {"accounts": "Account", "contacts": "Contact", "leads": "Lead", "opportunities": "Opportunity",
           "cases": "Case"}
ID_PREFIX = {"Account": "001", "Contact": "003", "Lead": "00Q", "Opportunity": "006", "Case": "500"}


def _crm_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def crm_system(records: dict[str, list[dict]], seed: int, end: date) -> dict[str, list[dict]]:
    """Lo que "tiene adentro" el CRM: cada registro con su `Id`, `CreatedDate`, `SystemModstamp`, `IsDeleted` y los
    campos que calcula el sistema (`IsClosed`, `IsWon`, `CaseNumber`, `ClosedDate`, `IsConverted`).

    `records` es la salida de `crm_records` (o lo leído de `sim.crm_*`). Sin columnas `_*`. Ids y fechas salen de la
    semilla: publicar dos veces da exactamente lo mismo, y la ingesta incremental no ve cambios que no hubo.
    """
    r = random.Random(seed * 11 + 5)
    go_live = datetime.combine(ref.CRM_GO_LIVE, time(9), tzinfo=timezone.utc)
    last = datetime.combine(end, time(21), tzinfo=timezone.utc)
    span = int((last - go_live).total_seconds())

    def created_after(t0: datetime) -> datetime:
        return t0 + timedelta(seconds=r.randrange(max(1, int((last - t0).total_seconds()))))

    def modified(t0: datetime) -> datetime:
        return min(last, t0 + timedelta(hours=r.choice([0, 0, 1, 26, 24 * 9, 24 * 40])))

    out: dict[str, list[dict]] = {}
    created_by_key: dict[str, datetime] = {}
    id_by_key: dict[str, str] = {}
    for key, obj in OBJECTS.items():
        rows = []
        for rec in records.get(key, []):
            rec = {k: v for k, v in rec.items() if not k.startswith("_") and v is not None}
            n = int(rec["Andes_Key__c"].split("-")[1])
            parent = rec.pop("Account.Andes_Key__c", None) or rec.pop("Account_Andes_Key__c", None)
            t0 = created_after(created_by_key[parent]) if parent else go_live + timedelta(seconds=r.randrange(span))
            if obj == "Opportunity" and rec.get("CloseDate"):
                close = datetime.combine(date.fromisoformat(rec["CloseDate"]), time(18), tzinfo=timezone.utc)
                t0 = max(t0, min(last, close) - timedelta(days=r.randrange(10, 120)))
            t1 = modified(t0)
            row = {"Id": f"{ID_PREFIX[obj]}{n:015d}", "IsDeleted": False, "CreatedDate": _crm_ts(t0), **rec}
            if parent:
                row["AccountId"] = id_by_key[parent]
            if obj == "Opportunity":
                row["Amount"] = float(rec["Amount"]) if rec.get("Amount") else None
                row["IsClosed"] = rec["StageName"].startswith("Closed")
                row["IsWon"] = rec["StageName"] == "Closed Won"
            if obj == "Lead":
                row["IsConverted"] = False
            if obj == "Case":
                row["CaseNumber"] = f"{n:08d}"
                row["IsClosed"] = rec["Status"] == "Closed"
                closed = min(last, t0 + timedelta(hours=r.uniform(2, 240))) if row["IsClosed"] else None
                row["ClosedDate"] = _crm_ts(closed) if closed else None
                t1 = max(t1, closed or t1)
            row["SystemModstamp"] = _crm_ts(t1)
            created_by_key[rec["Andes_Key__c"]] = t0
            id_by_key[rec["Andes_Key__c"]] = row["Id"]
            rows.append(row)
        out[obj] = rows
    return out


def crm_state(spark: SparkSession, cfg: ProjectConfig) -> dict[str, list[dict]]:
    """El CRM publicado, a partir de `sim.crm_*` (escrito por `generate_crm`)."""
    records = {}
    for key in OBJECTS:
        df = spark.table(cfg.table("sim", f"crm_{key}"))
        cols = [c for c in df.columns if not c.startswith("_")]
        records[key] = sorted((row.asDict() for row in df.select(*cols).collect()), key=lambda x: x["Andes_Key__c"])
    return crm_system(records, cfg.seed, cfg.end_date)
