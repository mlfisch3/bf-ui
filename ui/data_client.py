from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class DataSource:
    raw_base: str

    def url_for(self, path: str) -> str:
        base = self.raw_base.rstrip("/")
        return f"{base}/{path.lstrip('/')}"


def fetch_json(source: DataSource, path: str) -> dict[str, Any]:
    url = source.url_for(path)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return json.loads(resp.text)
