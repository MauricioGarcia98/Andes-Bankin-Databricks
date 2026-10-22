import pytest

from andes_bank.salesforce.org import sf_domain


@pytest.mark.parametrize("url, domain", [
    (None, "login"),
    ("https://login.salesforce.com", "login"),
    ("https://test.salesforce.com", "test"),
    ("https://acme-dev-ed.develop.my.salesforce.com", "acme-dev-ed.develop.my"),
    # la URL del navegador es la de la interfaz: se traduce a Mi dominio (bitácora R15)
    ("https://acme.lightning.force.com/", "acme.my"),
    ("https://acme-dev-ed.develop.lightning.force.com", "acme-dev-ed.develop.my"),
    ("https://acme.my.salesforce-setup.com", "acme.my"),
])
def test_sf_domain(url, domain):
    assert sf_domain(url) == domain
