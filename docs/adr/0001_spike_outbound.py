# Databricks notebook source
import urllib.request, json, ssl
hosts = {
  "bcra": "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones",
  "azure_blob": "https://azure.microsoft.com",
  "azure_functions": "https://functions.azure.com",
  "salesforce": "https://login.salesforce.com",
  "github": "https://api.github.com",
}
res = {}
for k, u in hosts.items():
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "andes-spike"}), timeout=15)
        res[k] = f"OK {r.status}"
    except urllib.error.HTTPError as e:
        res[k] = f"HTTP {e.code} (llega)"
    except Exception as e:
        res[k] = f"FALLA {type(e).__name__}: {str(e)[:120]}"
print(json.dumps(res, indent=2))
dbutils.notebook.exit(json.dumps(res))
