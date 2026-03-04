from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag


THREAD_HREF_RE = re.compile(r"/threads/[^/]*\.(\d+)(?:/|$)")


@dataclass
class UpdateResult:
    started_at: str
    finished_at: str
    checked_threads: int
    updated_threads: int
    requests_made: int
    errors: list[dict[str, str]]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def due_for_run(next_run_at: str | None, now: datetime | None = None) -> bool:
    if not next_run_at:
        return True
    next_dt = parse_iso(next_run_at)
    if not next_dt:
        return True
    now_dt = now or datetime.now(timezone.utc)
    return now_dt >= next_dt


def next_run_timestamp(interval_seconds: int) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
    return dt.isoformat()


def build_page_url(base: str, page: int) -> str:
    if page <= 1:
        return base
    if base.endswith("/"):
        return f"{base}page-{page}"
    return f"{base}/page-{page}"


def parse_thread_numeric_id_from_href(href: str | None) -> str | None:
    if not href:
        return None
    match = THREAD_HREF_RE.search(href)
    if not match:
        return None
    return match.group(1)


def parse_abbrev_number(raw: str) -> int | None:
    if not raw:
        return None
    text = raw.strip().replace(",", "")
    match = re.match(r"^(\d+(?:\.\d+)?)([KkMm])?$", text)
    if not match:
        digits = re.sub(r"[^0-9]", "", text)
        return int(digits) if digits else None
    value = float(match.group(1))
    suffix = match.group(2)
    if suffix:
        if suffix.lower() == "k":
            value *= 1_000
        elif suffix.lower() == "m":
            value *= 1_000_000
    return int(value)


def _candidate_containers(soup: BeautifulSoup) -> list[Tag]:
    rows = soup.select(".structItem--thread, .discussionListItem")
    if rows:
        return list(rows)
    items: list[Tag] = []
    seen: set[int] = set()
    for link in soup.find_all("a", href=True):
        if "/threads/" not in str(link.get("href")):
            continue
        container = link.find_parent(
            class_=lambda c: c and ("structItem" in c or "discussionListItem" in c)
        )
        if container is None:
            container = link.find_parent(["article", "li", "div"])
        if container is None:
            continue
        ident = id(container)
        if ident in seen:
            continue
        seen.add(ident)
        items.append(container)
    return items


def _extract_thread_link(container: Tag) -> Tag | None:
    link = container.select_one(".structItem-title a[href*='/threads/']")
    if link is not None:
        return link
    return container.find("a", href=re.compile(r"/threads/"))


def _extract_views(container: Tag) -> int | None:
    for dt in container.find_all("dt"):
        label = " ".join(dt.get_text(" ").split()).strip().lower()
        if label == "views":
            dd = dt.find_next_sibling("dd")
            if dd:
                return parse_abbrev_number(dd.get_text(" "))
    for data in container.select(".pairs--justified"):
        title = data.select_one("dt")
        value = data.select_one("dd")
        if title and value and "views" in title.get_text(" ").lower():
            return parse_abbrev_number(value.get_text(" "))
    return None


def parse_listing_rows(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    for idx, container in enumerate(_candidate_containers(soup)):
        link = _extract_thread_link(container)
        if not link:
            continue
        href = str(link.get("href") or "")
        thread_numeric_id = parse_thread_numeric_id_from_href(href)
        if not thread_numeric_id:
            continue
        rows.append(
            {
                "thread_numeric_id": thread_numeric_id,
                "title": " ".join(link.get_text(" ").split()).strip(),
                "views": _extract_views(container),
                "position": idx,
            }
        )
    return rows


def _headers() -> dict[str, str]:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _group_active_threads(threads: list[dict[str, Any]], selected_thread_ids: set[str] | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for thread in threads:
        if thread.get("status", "active") != "active":
            continue
        if not thread.get("thread_numeric_id"):
            continue
        if selected_thread_ids and thread.get("id") not in selected_thread_ids:
            continue
        grouped.setdefault(thread["subforum_key"], []).append(thread)
    return grouped


def run_update(
    *,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    selected_thread_ids: set[str] | None = None,
    set_action: callable | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], UpdateResult]:
    started_at = utc_now()
    requests_made = 0
    updated_threads = 0
    errors: list[dict[str, str]] = []

    global_cfg = config.get("global", {})
    min_delay = float(global_cfg.get("min_delay_seconds", 3))
    max_delay = float(global_cfg.get("max_delay_seconds", 9))
    max_retries = int(global_cfg.get("max_retries", 2))
    max_rpm = int(global_cfg.get("max_requests_per_minute", 12))

    grouped = _group_active_threads(threads_payload.get("threads", []), selected_thread_ids)
    subforums = {x["key"]: x for x in config.get("subforums", [])}
    samples_updates: dict[str, dict[str, Any]] = {}

    recent_calls: list[float] = []

    def wait_budget() -> None:
        nonlocal recent_calls
        now = time.time()
        recent_calls = [t for t in recent_calls if now - t < 60]
        if max_rpm > 0 and len(recent_calls) >= max_rpm:
            wait = 60 - (now - recent_calls[0])
            if wait > 0:
                time.sleep(wait)
        time.sleep(random.uniform(min_delay, max_delay))
        recent_calls.append(time.time())

    session = requests.Session()

    for subforum_key, sub_threads in grouped.items():
        subforum = subforums.get(subforum_key)
        if not subforum:
            errors.append({"subforum_key": subforum_key, "error": "Unknown subforum"})
            continue

        target_by_numeric_id = {str(t["thread_numeric_id"]): t for t in sub_threads}
        pending_ids = set(target_by_numeric_id.keys())
        max_pages = int(subforum.get("max_pages_per_update", 3))

        for page in range(1, max_pages + 1):
            if not pending_ids:
                break
            if set_action:
                set_action(f"Fetching {subforum['name']} page {page}")

            url = build_page_url(subforum["url"], page)
            html = None
            for attempt in range(max_retries + 1):
                try:
                    wait_budget()
                    requests_made += 1
                    resp = session.get(url, headers=_headers(), timeout=20)
                    if resp.status_code in {403, 429}:
                        time.sleep(6 + attempt * 4)
                    resp.raise_for_status()
                    html = resp.text
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt >= max_retries:
                        errors.append(
                            {
                                "subforum_key": subforum_key,
                                "error": f"Fetch failed ({url}): {exc}",
                            }
                        )
                    else:
                        time.sleep(2 + attempt * 2)
            if html is None:
                break

            listing_rows = parse_listing_rows(html)
            by_id = {row["thread_numeric_id"]: row for row in listing_rows}
            for numeric_id in list(pending_ids):
                row = by_id.get(numeric_id)
                if not row:
                    continue
                if row.get("views") is None:
                    continue
                thread = target_by_numeric_id[numeric_id]
                thread["last_seen_at"] = utc_now()
                thread["last_view_count"] = int(row["views"])
                thread["last_found_page"] = page
                thread["last_found_above"] = row.get("position")
                if row.get("title"):
                    thread["last_seen_title"] = row["title"]

                thread_id = thread["id"]
                payload = samples_updates.setdefault(
                    thread_id,
                    {
                        "thread_id": thread_id,
                        "title": thread.get("display_name") or thread.get("title") or f"Thread {numeric_id}",
                        "thread_numeric_id": numeric_id,
                        "samples": [],
                    },
                )
                payload["samples"].append(
                    {
                        "ts": utc_now(),
                        "views": int(row["views"]),
                        "page": page,
                        "above": row.get("position"),
                        "observed_title": row.get("title"),
                    }
                )
                updated_threads += 1
                pending_ids.discard(numeric_id)

    finished_at = utc_now()
    result = UpdateResult(
        started_at=started_at,
        finished_at=finished_at,
        checked_threads=sum(len(v) for v in grouped.values()),
        updated_threads=updated_threads,
        requests_made=requests_made,
        errors=errors,
    )
    return config, threads_payload, samples_updates, result
