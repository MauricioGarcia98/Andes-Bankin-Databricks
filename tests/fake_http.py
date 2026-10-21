"""Una `requests.Session` falsa que, en vez de ir a Azure, llama a la API en el mismo proceso."""

import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "azure" / "card_api"))

from api import Request  # noqa: E402


class FakeResponse:
    def __init__(self, status, headers, text):
        self.status_code, self.headers, self.text = status, headers, text

    def json(self):
        return json.loads(self.text)


class FakeSession:
    def __init__(self, api, prefix="/api"):
        self.api, self.prefix = api, prefix

    def _path(self, url):
        return urlparse(url).path[len(self.prefix):]

    def get(self, url, params=None, timeout=None, headers=None):
        return FakeResponse(*self.api.handle(Request("GET", self._path(url), params={k: str(v) for k, v in (params or {}).items()},
                                                     headers={k.lower(): v for k, v in (headers or {}).items()})))

    def post(self, url, data=None, timeout=None, headers=None):
        return FakeResponse(*self.api.handle(Request("POST", self._path(url), form=data or {})))
