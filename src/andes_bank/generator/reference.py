"""Datos de referencia del banco: sucursales, nombres, rubros, calendario y macro.

Todo es ficticio salvo la geografía. Los índices macro (inflación, dólar oficial) están inspirados en 2023-2026 y
redondeados: sirven para que los montos en pesos "se muevan" como en la realidad, no son datos oficiales. La
cotización oficial que usa el lakehouse sale de la API real del BCRA (ver fuentes FX).
"""

from __future__ import annotations

from datetime import date

# ------------------------------------------------------------------------------------------- hitos
CORE_MIGRATION_DATE = date(2024, 3, 11)      # migración del core: antes latin1 + ';' + dd/MM/yyyy
CRM_GO_LIVE = date(2024, 6, 3)               # pasan del CRM casero al CRM comercial nuevo
API_V2_LAUNCH = date(2025, 4, 1)             # el procesador publica la API v2 (cursor, montos decimales)
API_V1_LAST_EXPORT = date(2025, 6, 30)       # último exporte diario de la integración vieja (v1)
CANAL_DIGITAL_SINCE = date(2025, 1, 2)       # schema drift: columna nueva en el CSV de movimientos
WALLET_TYPE_SINCE = date(2026, 3, 1)         # schema drift: campo nuevo en la API (Apple/Google Pay)
DEVALUATION_DATE = date(2023, 12, 13)

# ------------------------------------------------------------------------------------- sucursales
# (región, provincia, ciudad, cantidad de sucursales a SF 1). Las sucursales NO escalan: son 62 siempre.
BRANCH_CITIES = [
    ("CUYO", "Mendoza", "Mendoza", 6), ("CUYO", "Mendoza", "Godoy Cruz", 3), ("CUYO", "Mendoza", "Guaymallén", 3),
    ("CUYO", "Mendoza", "Las Heras", 2), ("CUYO", "Mendoza", "Maipú", 2), ("CUYO", "Mendoza", "Luján de Cuyo", 1),
    ("CUYO", "Mendoza", "San Rafael", 2), ("CUYO", "Mendoza", "Tunuyán", 1),
    ("CUYO", "San Juan", "San Juan", 4), ("CUYO", "San Juan", "Rawson", 1),
    ("CUYO", "San Luis", "San Luis", 2), ("CUYO", "San Luis", "Villa Mercedes", 1),
    ("NOA", "Salta", "Salta", 5), ("NOA", "Salta", "Orán", 1), ("NOA", "Salta", "Tartagal", 1),
    ("NOA", "Jujuy", "San Salvador de Jujuy", 4), ("NOA", "Jujuy", "Palpalá", 1),
    ("NOA", "Tucumán", "San Miguel de Tucumán", 5), ("NOA", "Tucumán", "Yerba Buena", 1),
    ("NOA", "Tucumán", "Concepción", 1),
    ("CENTRO", "Córdoba", "Córdoba", 5), ("CENTRO", "Córdoba", "Villa María", 1), ("CENTRO", "Córdoba", "Río Cuarto", 1),
    ("CENTRO", "CABA", "Ciudad de Buenos Aires", 5), ("CENTRO", "Buenos Aires", "La Plata", 1),
    ("CENTRO", "Buenos Aires", "Mar del Plata", 1), ("CENTRO", "Buenos Aires", "San Isidro", 1),
]

# reparto de clientes por región (DIGITAL = clientes 100 % app, sin sucursal física: se asignan a la 0999)
REGION_WEIGHTS = {"CUYO": 0.46, "NOA": 0.27, "CENTRO": 0.17, "DIGITAL": 0.10}
DIGITAL_BRANCH_ID = 999


def branches() -> list[dict]:
    """Las 62 sucursales + la sucursal virtual de clientes digitales."""
    out, branch_id = [], 1
    for region, province, city, n in BRANCH_CITIES:
        for i in range(n):
            name = city if n == 1 else f"{city} {['Centro', 'Norte', 'Sur', 'Este', 'Oeste', 'Shopping'][i % 6]}"
            out.append({"branch_id": branch_id, "branch_name": name, "city": city, "province": province,
                        "region": region, "opened_year": 1998 if region == "CUYO" else (2008 if region == "NOA" else 2016)})
            branch_id += 1
    out.append({"branch_id": DIGITAL_BRANCH_ID, "branch_name": "Andes Digital", "city": "Ciudad de Buenos Aires",
                "province": "CABA", "region": "DIGITAL", "opened_year": 2020})
    return out


# --------------------------------------------------------------------------------------- personas
FIRST_NAMES_F = ["María", "Lucía", "Sofía", "Valentina", "Camila", "Martina", "Florencia", "Carolina", "Gabriela",
                 "Laura", "Ana", "Paula", "Julieta", "Agustina", "Romina", "Silvina", "Natalia", "Verónica",
                 "Daniela", "Rocío", "Milagros", "Belén", "Mariana", "Graciela", "Susana", "Norma", "Patricia"]
FIRST_NAMES_M = ["Juan", "José", "Carlos", "Luis", "Jorge", "Martín", "Diego", "Pablo", "Matías", "Nicolás",
                 "Federico", "Sebastián", "Gonzalo", "Facundo", "Lucas", "Santiago", "Rodrigo", "Ezequiel",
                 "Hernán", "Gustavo", "Raúl", "Miguel", "Alejandro", "Tomás", "Joaquín", "Franco", "Ramiro"]
LAST_NAMES = ["González", "Rodríguez", "Gómez", "Fernández", "López", "Díaz", "Martínez", "Pérez", "García",
              "Sánchez", "Romero", "Sosa", "Álvarez", "Torres", "Ruiz", "Ramírez", "Flores", "Acosta", "Benítez",
              "Medina", "Suárez", "Herrera", "Aguirre", "Pereyra", "Gutiérrez", "Giménez", "Molina", "Silva",
              "Castro", "Rojas", "Ortiz", "Núñez", "Luna", "Juárez", "Cabrera", "Ríos", "Ferreyra", "Godoy",
              "Morales", "Domínguez", "Quiroga", "Videla", "Guzmán", "Ibarra", "Vera", "Mamaní", "Cruz", "Chocobar"]
COMPANY_WORDS = ["Agro", "Vinos", "Transportes", "Construcciones", "Comercial", "Servicios", "Distribuidora",
                 "Ferretería", "Olivícola", "Minera", "Turismo", "Logística", "Alimentos", "Textil", "Metalúrgica"]
COMPANY_SUFFIX = ["S.A.", "S.R.L.", "S.A.S."]
EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "yahoo.com.ar", "outlook.com", "live.com.ar"]

SEGMENTS = ["MASIVO", "PLUS", "PREMIUM", "PYME"]
SEGMENT_WEIGHTS = [0.62, 0.22, 0.08, 0.08]

# ------------------------------------------------------------------------------------- movimientos
# Las medianas se ajustan por inflación y por el nivel de ingresos del cliente.
TXN_TYPES = {
    # tipo: (signo, mediana ARS ene-2023, sigma, peso en caja de ahorro, peso en cuenta corriente)
    "TRANSFERENCIA_RECIBIDA": (+1, 15_000, 1.1, 0.18, 0.42),
    "TRANSFERENCIA_ENVIADA": (-1, 45_000, 1.1, 0.26, 0.30),
    "DEPOSITO_EFECTIVO": (+1, 20_000, 1.0, 0.06, 0.08),
    "EXTRACCION_CAJERO": (-1, 25_000, 0.7, 0.16, 0.04),
    "DEBITO_AUTOMATICO": (-1, 10_000, 0.8, 0.14, 0.06),
    "PAGO_SERVICIOS": (-1, 9_000, 0.9, 0.12, 0.06),
    "COMISION_MANTENIMIENTO": (-1, 900, 0.2, 0.04, 0.02),
    "INTERESES": (+1, 400, 1.2, 0.04, 0.02),
}
SALARY_MEDIAN_ARS_2023 = 210_000       # sueldo mediano de ene-2023 (se ajusta por inflación)
SWEEP_INCOME_MONTHS = 2.5              # lo que excede 2,5 ingresos mensuales se va a plazo fijo a fin de mes
CHANNELS = ["SUCURSAL", "CAJERO", "HOMEBANKING", "APP", "DEBITO_AUTOMATICO"]

# ------------------------------------------------------------------------------------------ tarjetas
# rubro (MCC) → (nombre, mediana ARS ene-2023, sigma, peso, riesgo de fraude relativo)
MCC = {
    "5411": ("Supermercados", 9_000, 0.9, 0.24, 1.0),
    "5541": ("Estaciones de servicio", 7_500, 0.6, 0.12, 1.0),
    "5812": ("Restaurantes", 8_500, 0.8, 0.10, 1.0),
    "5912": ("Farmacias", 4_500, 0.8, 0.08, 0.8),
    "5651": ("Indumentaria", 15_000, 0.9, 0.08, 1.2),
    "5732": ("Electrónica", 60_000, 1.0, 0.04, 3.0),
    "4111": ("Transporte", 1_200, 0.6, 0.07, 0.5),
    "4814": ("Telecomunicaciones", 6_000, 0.5, 0.05, 0.8),
    "5311": ("Tiendas departamentales", 18_000, 1.0, 0.05, 1.5),
    "7011": ("Hoteles", 45_000, 0.9, 0.02, 2.0),
    "4511": ("Aerolíneas", 120_000, 0.8, 0.02, 2.5),
    "5815": ("Servicios digitales", 3_500, 0.7, 0.08, 2.0),
    "5944": ("Joyerías", 80_000, 1.0, 0.01, 4.0),
    "7995": ("Apuestas online", 20_000, 1.0, 0.01, 5.0),
    "6011": ("Retiro de efectivo", 15_000, 0.6, 0.03, 1.0),
}
FOREIGN_MERCHANT_SHARE = 0.04          # comercios del exterior (compras en USD)
DECLINE_REASONS = {                     # motivo → probabilidad base
    "05_NO_AUTORIZADA": 0.010,
    "51_FONDOS_INSUFICIENTES": 0.030,
    "54_TARJETA_VENCIDA": 0.003,
    "55_PIN_INCORRECTO": 0.006,
    "59_SOSPECHA_FRAUDE": 0.004,
    "61_EXCEDE_LIMITE": 0.008,
}

# ------------------------------------------------------------------------------------------- macro
# inflación mensual (%) aproximada 2023-2026: mueve los montos en pesos
MONTHLY_INFLATION = {
    2023: [6.0, 6.6, 7.7, 8.4, 7.8, 6.0, 6.3, 12.4, 12.7, 8.3, 12.8, 25.5],
    2024: [20.6, 13.2, 11.0, 8.8, 4.2, 4.6, 4.0, 4.2, 3.5, 2.7, 2.4, 2.7],
    2025: [2.2, 2.4, 3.7, 2.8, 1.5, 1.6, 1.9, 1.9, 2.1, 2.3, 2.5, 2.8],
    2026: [2.6, 2.4, 2.3, 2.2, 2.1, 2.0, 2.0, 1.9, 1.9, 1.8, 1.8, 1.8],
}

# dólar oficial mayorista aproximado (ARS por USD) a fin de cada mes. El lakehouse usa la API real del BCRA;
# esto sólo sirve para que el generador convierta las compras en el exterior.
USD_OFFICIAL_EOM = {
    2023: [186, 198, 209, 222, 240, 257, 274, 350, 350, 350, 366, 808],
    2024: [826, 842, 858, 876, 894, 912, 932, 950, 970, 990, 1010, 1032],
    2025: [1052, 1062, 1074, 1160, 1190, 1205, 1300, 1340, 1380, 1450, 1460, 1470],
    2026: [1480, 1490, 1500, 1515, 1530, 1545, 1560, 1575, 1590, 1605, 1620, 1635],
}


def monthly_index_rows(start: date, end: date) -> list[tuple[str, float, float]]:
    """(yyyy-MM, índice de precios con base ene-2023 = 1, dólar oficial de fin de mes) para cada mes."""
    rows, idx = [], 1.0
    for year in range(start.year, end.year + 1):
        for m in range(1, 13):
            if (year, m) < (start.year, start.month) or (year, m) > (end.year, end.month):
                continue
            # después de 2026 (el banco sigue vivo con el job "día nuevo") se repite el último año conocido
            infl = MONTHLY_INFLATION.get(year, MONTHLY_INFLATION[max(MONTHLY_INFLATION)])
            usd = USD_OFFICIAL_EOM.get(year, [USD_OFFICIAL_EOM[max(USD_OFFICIAL_EOM)][-1]] * 12)
            rows.append((f"{year}-{m:02d}", round(idx, 6), float(usd[m - 1])))
            idx *= 1 + infl[m - 1] / 100
    return rows


# feriados nacionales (días sin cotización del BCRA ni operaciones de cámara)
HOLIDAYS = {
    "2023-01-01", "2023-02-20", "2023-02-21", "2023-03-24", "2023-04-02", "2023-04-07", "2023-05-01", "2023-05-25",
    "2023-05-26", "2023-06-17", "2023-06-19", "2023-06-20", "2023-07-09", "2023-08-21", "2023-10-13", "2023-10-16",
    "2023-11-20", "2023-12-08", "2023-12-25",
    "2024-01-01", "2024-02-12", "2024-02-13", "2024-03-24", "2024-03-28", "2024-03-29", "2024-04-01", "2024-04-02",
    "2024-05-01", "2024-05-25", "2024-06-17", "2024-06-20", "2024-06-21", "2024-07-09", "2024-08-17", "2024-10-11",
    "2024-11-18", "2024-12-08", "2024-12-25",
    "2025-01-01", "2025-03-03", "2025-03-04", "2025-03-24", "2025-04-02", "2025-04-18", "2025-05-01", "2025-05-02",
    "2025-05-25", "2025-06-16", "2025-06-20", "2025-07-09", "2025-08-15", "2025-11-21", "2025-11-24", "2025-12-08",
    "2025-12-25",
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-03-24", "2026-04-02", "2026-04-03", "2026-05-01", "2026-05-25",
    "2026-06-15", "2026-06-20", "2026-07-09", "2026-07-10", "2026-08-17", "2026-10-12", "2026-11-23", "2026-12-07",
    "2026-12-08", "2026-12-25",
}
