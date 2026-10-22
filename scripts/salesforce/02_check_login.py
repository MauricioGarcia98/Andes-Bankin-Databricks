"""Prueba el login a Salesforce con las variables de entorno (sin imprimir secretos).

    pip install simple-salesforce
    python scripts/salesforce/02_check_login.py

Si el login falla con INVALID_LOGIN, prueba de nuevo SIN el token para decir cuál de los datos está mal (R16).
"""

import os
import sys

from simple_salesforce.exceptions import SalesforceAuthenticationFailed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from andes_bank.salesforce.org import connect, sf_domain  # noqa: E402

user, password, token = (os.environ[k].strip() for k in ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN"))
login_url = os.environ.get("SF_LOGIN_URL")
print(f"  dominio de login: {sf_domain(login_url)}.salesforce.com")
try:
    sf = connect(user, password, token, login_url)
except SalesforceAuthenticationFailed as e:
    if e.code != "INVALID_LOGIN":
        raise
    try:
        connect(user, password, "", login_url)
        hint = "sin token entra: la IP está habilitada y el token sobra (o la org no lo pide)"
    except SalesforceAuthenticationFailed as e2:
        hint = {"LOGIN_MUST_USE_SECURITY_TOKEN": "usuario y contraseña están bien: el TOKEN no corresponde a la "
                                                 "contraseña actual → restablecelo y volvé a exportarlo (R16)",
                "INVALID_LOGIN": "el usuario o la contraseña están mal (o el usuario está bloqueado), o el dominio "
                                 "no es el de tu org"}.get(e2.code, e2.code)
    sys.exit(f"✘ INVALID_LOGIN: {hint}")
org = sf.query("SELECT Name, OrganizationType, IsSandbox FROM Organization")["records"][0]
limits = sf.limits()
print(f"✔ conectado a '{org['Name']}' ({org['OrganizationType']}, sandbox={org['IsSandbox']})")
print(f"  almacenamiento de datos: {limits['DataStorageMB']['Remaining']} de {limits['DataStorageMB']['Max']} MB libres")
print(f"  llamadas a la API hoy: {limits['DailyApiRequests']['Remaining']} de {limits['DailyApiRequests']['Max']} libres")
