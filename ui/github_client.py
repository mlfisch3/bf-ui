from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass
class GithubConfig:
    repo: str
    branch: str
    token: str | None


class GithubClient:
    def __init__(self, cfg: GithubConfig) -> None:
        self.cfg = cfg

    @property
    def base_url(self) -> str:
        return f"https://api.github.com/repos/{self.cfg.repo}/contents"

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
        }
        if self.cfg.token:
            headers["Authorization"] = f"Bearer {self.cfg.token}"
        return headers

    def get_file(self, path: str) -> tuple[dict[str, Any], str]:
        url = f"{self.base_url}/{path}"
        resp = requests.get(url, headers=self._headers(), params={"ref": self.cfg.branch}, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        content = base64.b64decode(payload["content"]).decode("utf-8")
        return json.loads(content), payload["sha"]

    def put_file(self, path: str, data: dict[str, Any], message: str, sha: Optional[str]) -> None:
        if not self.cfg.token:
            raise RuntimeError("GITHUB_TOKEN not configured")
        url = f"{self.base_url}/{path}"
        content = json.dumps(data, indent=2, sort_keys=True) + "\n"
        encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        payload: dict[str, Any] = {
            "message": message,
            "content": encoded,
            "branch": self.cfg.branch,
        }
        if sha:
            payload["sha"] = sha
        resp = requests.put(url, headers=self._headers(), json=payload, timeout=20)
        resp.raise_for_status()

    def dispatch_workflow(self, workflow_file: str, ref: str) -> None:
        if not self.cfg.token:
            raise RuntimeError("GITHUB_TOKEN not configured")
        url = f"https://api.github.com/repos/{self.cfg.repo}/actions/workflows/{workflow_file}/dispatches"
        payload = {"ref": ref}
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=20)
        resp.raise_for_status()
