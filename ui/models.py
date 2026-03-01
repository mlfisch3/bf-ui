from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def thread_id_for(title: str, subforum_key: str) -> str:
    raw = f"{subforum_key}::{title}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:12]
    safe = "-".join(title.lower().split())
    safe = "".join(ch for ch in safe if ch.isalnum() or ch == "-")
    safe = safe[:40].strip("-")
    return f"{safe}-{digest}" if safe else digest
