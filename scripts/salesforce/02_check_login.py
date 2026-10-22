"""Prueba el login a Salesforce con las variables de entorno (sin imprimir secretos).

    pip install simple-salesforce
    python scripts/salesforce/02_check_login.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from andes_bank.salesforce.org import connect  # noqa: E402

sf = connect(os.environ["SF_USERNAME"], os.environ["SF_PASSWORD"], os.environ["SF_SECURITY_TOKEN"],
             os.environ.get("SF_LOGIN_URL"))
org = sf.query("SELECT Name, OrganizationType, IsSandbox FROM Organization")["records"][0]
limits = sf.limits()
print(f"✔ conectado a '{org['Name']}' ({org['OrganizationType']}, sandbox={org['IsSandbox']})")
print(f"  almacenamiento de datos: {limits['DataStorageMB']['Remaining']} de {limits['DataStorageMB']['Max']} MB libres")
print(f"  llamadas a la API hoy: {limits['DailyApiRequests']['Remaining']} de {limits['DailyApiRequests']['Max']} libres")
