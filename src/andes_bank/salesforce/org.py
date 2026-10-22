"""Conexión a Salesforce y preparación de la org (etapa 08).

Usa `simple_salesforce` (usuario + contraseña + token de seguridad). Las credenciales salen de variables de entorno
en la máquina de quien construye (`SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`, `SF_LOGIN_URL`) y del secret
scope `andes_bank` en Databricks (`sf_username`, ...). Nunca del repo.

`prepare_org` es idempotente: crea los campos custom que falten (Tooling API), un permission set que los hace
visibles y editables, y se lo asigna al usuario de integración. Los campos creados por API NO son visibles para
ningún perfil hasta que alguien les da permiso (FLS): sin el permission set, el upsert falla con "No such column".
"""

from __future__ import annotations

import time
from urllib.parse import urlparse

PERMISSION_SET = "Andes_Integracion"
TEXT = {"type": "Text", "length": 40}
EXTERNAL_KEY = {"type": "Text", "length": 20, "externalId": True, "unique": True, "caseSensitive": False}

CUSTOM_FIELDS = {
    # objeto: {campo: metadata}
    "Account": {"Andes_Key__c": EXTERNAL_KEY, "Core_Customer_Id__c": dict(TEXT, length=20),
                "CUIT__c": dict(TEXT, length=20), "Segmento__c": dict(TEXT, length=20)},
    "Contact": {"Andes_Key__c": EXTERNAL_KEY},
    "Lead": {"Andes_Key__c": EXTERNAL_KEY, "CUIT__c": dict(TEXT, length=20)},
    "Opportunity": {"Andes_Key__c": EXTERNAL_KEY, "Producto__c": TEXT},
    "Case": {"Andes_Key__c": EXTERNAL_KEY},
}


def sf_domain(login_url: str | None) -> str:
    """El `domain` de simple_salesforce (el host sin `.salesforce.com`).

    `https://login.salesforce.com` → 'login'; `https://acme-dev-ed.develop.my.salesforce.com` → 'acme-dev-ed.develop.my'.
    La URL de la barra del navegador (`acme.lightning.force.com`, `acme.my.salesforce-setup.com`) es la de la interfaz,
    no la de la API: se traduce a Mi dominio (`acme.my`).
    """
    host = urlparse(login_url or "https://login.salesforce.com").netloc or (login_url or "")
    host = host.lower().rstrip("/")
    for ui in (".lightning.force.com", ".my.salesforce-setup.com"):
        if host.endswith(ui):
            return host.removesuffix(ui) + ".my"
    return host.removesuffix(".salesforce.com") or "login"


def connect(username: str, password: str, security_token: str, login_url: str | None = None):
    from simple_salesforce import Salesforce

    return Salesforce(username=username, password=password, security_token=security_token,
                      domain=sf_domain(login_url), client_id="andes-bank")


def _label(field: str) -> str:
    return field.removesuffix("__c").replace("_", " ")


def ensure_fields(sf) -> list[str]:
    """Crea los campos custom que falten. Devuelve los creados."""
    created = []
    for obj, fields in CUSTOM_FIELDS.items():
        existing = {f["name"] for f in getattr(sf, obj).describe()["fields"]}
        for name, meta in fields.items():
            if name in existing:
                continue
            body = {"FullName": f"{obj}.{name}", "Metadata": dict(meta, label=_label(name))}
            sf.toolingexecute("sobjects/CustomField", method="POST", data=body)
            created.append(f"{obj}.{name}")
    return created


def ensure_permission_set(sf, username: str, retries: int = 6, wait_s: float = 10) -> int:
    """Permission set con lectura y edición de los campos custom, asignado al usuario. Devuelve permisos agregados."""
    ps = sf.query(f"SELECT Id FROM PermissionSet WHERE Name = '{PERMISSION_SET}'")["records"]
    ps_id = ps[0]["Id"] if ps else sf.PermissionSet.create({"Name": PERMISSION_SET, "Label": "Andes Integracion"})["id"]
    have = {r["Field"] for r in sf.query_all(f"SELECT Field FROM FieldPermissions WHERE ParentId = '{ps_id}'")["records"]}
    added = 0
    for obj, fields in CUSTOM_FIELDS.items():
        for name in fields:
            full = f"{obj}.{name}"
            if full in have:
                continue
            for attempt in range(retries):     # un campo recién creado tarda unos segundos en "existir" para FLS
                try:
                    sf.FieldPermissions.create({"ParentId": ps_id, "SobjectType": obj, "Field": full,
                                                "PermissionsRead": True, "PermissionsEdit": True})
                    added += 1
                    break
                except Exception:
                    if attempt == retries - 1:
                        raise
                    time.sleep(wait_s)
    user_id = sf.query(f"SELECT Id FROM User WHERE Username = '{username}'")["records"][0]["Id"]
    assigned = sf.query(f"SELECT Id FROM PermissionSetAssignment WHERE PermissionSetId = '{ps_id}' "
                        f"AND AssigneeId = '{user_id}'")["records"]
    if not assigned:
        sf.PermissionSetAssignment.create({"PermissionSetId": ps_id, "AssigneeId": user_id})
    return added


def prepare_org(sf, username: str) -> dict:
    created = ensure_fields(sf)
    return {"campos_creados": created, "permisos_agregados": ensure_permission_set(sf, username)}
