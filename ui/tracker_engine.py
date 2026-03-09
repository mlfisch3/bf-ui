from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup, Tag


THREAD_HREF_RE = re.compile(r"/threads/[^/]*\.(\d+)(?:/|$)")
FORUM_NODE_RE = re.compile(r"\.(\d+)(?:/)?$")
SEQUENTIAL_PAGE_DEPTH = 20


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


def extract_forum_node_id(subforum_url: str | None) -> str | None:
    if not subforum_url:
        return None
    match = FORUM_NODE_RE.search(subforum_url.rstrip("/"))
    return match.group(1) if match else None


def build_search_url(keyword: str, node_id: str) -> str:
    # Guest-compatible search URL form.
    q = quote_plus(keyword)
    return (
        "https://www.bladeforums.com/search/"
        f"?q={q}&t=post&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D={node_id}&c%5Btitle_only%5D=1&o=relevance"
    )


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


def _extract_views_from_text(text: str) -> int | None:
    if not text:
        return None
    match = re.search(r"\bviews?\b\s*[:\-]?\s*([0-9][0-9,]*(?:\.\d+)?[KkMm]?)", text, flags=re.IGNORECASE)
    if match:
        return parse_abbrev_number(match.group(1))
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


def parse_search_rows(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    # Prefer search-result specific containers first.
    for idx, container in enumerate(soup.select(".contentRow, .searchResult, .structItem--searchResult")):
        link = container.select_one("a[href*='/threads/']") or _extract_thread_link(container)
        if not link:
            continue
        href = str(link.get("href") or "")
        thread_numeric_id = parse_thread_numeric_id_from_href(href)
        if not thread_numeric_id or thread_numeric_id in seen_ids:
            continue
        seen_ids.add(thread_numeric_id)
        title = " ".join(link.get_text(" ").split()).strip()
        views = _extract_views(container)
        if views is None:
            views = _extract_views_from_text(container.get_text(" ", strip=True))
        rows.append(
            {
                "thread_numeric_id": thread_numeric_id,
                "title": title,
                "views": views,
                "position": idx,
            }
        )
    if rows:
        return rows

    for idx, container in enumerate(_candidate_containers(soup)):
        link = _extract_thread_link(container)
        if not link:
            continue
        href = str(link.get("href") or "")
        thread_numeric_id = parse_thread_numeric_id_from_href(href)
        if not thread_numeric_id or thread_numeric_id in seen_ids:
            continue
        seen_ids.add(thread_numeric_id)
        rows.append(
            {
                "thread_numeric_id": thread_numeric_id,
                "title": " ".join(link.get_text(" ").split()).strip(),
                "views": _extract_views(container) or _extract_views_from_text(container.get_text(" ", strip=True)),
                "position": idx,
            }
        )
    if rows:
        return rows
    for idx, link in enumerate(soup.select("a[href*='/threads/']")):
        href = str(link.get("href") or "")
        thread_numeric_id = parse_thread_numeric_id_from_href(href)
        if not thread_numeric_id or thread_numeric_id in seen_ids:
            continue
        seen_ids.add(thread_numeric_id)
        rows.append(
            {
                "thread_numeric_id": thread_numeric_id,
                "title": " ".join(link.get_text(" ").split()).strip(),
                "views": None,
                "position": idx,
            }
        )
    return rows


def build_search_keywords(thread: dict[str, Any]) -> str | None:
    raw = (
        thread.get("current_title")
        or thread.get("last_seen_title")
        or thread.get("display_name")
        or thread.get("title")
    )
    if not raw:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", str(raw))
    words = [w for w in cleaned.split() if len(w) >= 2]
    if not words:
        return None
    return " ".join(words[:6])


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


def _ordered_active_threads(threads: list[dict[str, Any]], selected_thread_ids: set[str] | None) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for thread in threads:
        # Keep self-test thread out of normal tracker runs, but allow explicitly
        # targeted isolated runs (selected_thread_ids contains the self-test id).
        is_self_test = bool(thread.get("is_self_test")) or bool(thread.get("is_selftest"))
        if is_self_test and not (selected_thread_ids and thread.get("id") in selected_thread_ids):
            continue
        if thread.get("status", "active") != "active":
            continue
        if not thread.get("thread_numeric_id"):
            continue
        if selected_thread_ids and thread.get("id") not in selected_thread_ids:
            continue
        active.append(thread)
    return sorted(active, key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))


def run_update(
    *,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    selected_thread_ids: set[str] | None = None,
    set_action: callable | None = None,
    log_http: callable | None = None,
    max_pages_override: int | None = None,
    enable_search_fallback: bool = True,
    should_abort: callable | None = None,
    auth_cookies: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], UpdateResult]:
    started_at = utc_now()
    requests_made = 0
    updated_threads = 0
    errors: list[dict[str, str]] = []

    global_cfg = config.get("global", {})
    min_delay = float(global_cfg.get("min_delay_seconds", 0.2))
    max_delay = float(global_cfg.get("max_delay_seconds", 1.0))
    max_retries = int(global_cfg.get("max_retries", 2))
    max_rpm = int(global_cfg.get("max_requests_per_minute", 12))
    if max_delay < min_delay:
        max_delay = min_delay

    active_threads = _ordered_active_threads(threads_payload.get("threads", []), selected_thread_ids)
    subforums = {x["key"]: x for x in config.get("subforums", [])}
    samples_updates: dict[str, dict[str, Any]] = {}
    page_cache: dict[str, dict[int, dict[str, dict[str, Any]]]] = {}
    subforum_failed: set[str] = set()

    recent_calls: list[float] = []
    last_call_at: float | None = None
    target_interval = (60.0 / max_rpm) if max_rpm > 0 else 0.0

    def wait_budget() -> None:
        nonlocal recent_calls, last_call_at
        now = time.time()
        recent_calls = [t for t in recent_calls if now - t < 60]
        if max_rpm > 0 and len(recent_calls) >= max_rpm:
            wait = 60 - (now - recent_calls[0])
            if wait > 0:
                time.sleep(wait)
                now = time.time()
                recent_calls = [t for t in recent_calls if now - t < 60]

        jitter_wait = random.uniform(min_delay, max_delay) if max_delay > 0 else 0.0
        cadence_wait = 0.0
        if target_interval > 0 and last_call_at is not None:
            cadence_wait = max(0.0, target_interval - (now - last_call_at))
        wait_for = max(jitter_wait, cadence_wait)
        if wait_for > 0:
            time.sleep(wait_for)
        last_call_at = time.time()
        recent_calls.append(last_call_at)

    session = requests.Session()
    if auth_cookies:
        session.cookies.update(auth_cookies)

    for thread in active_threads:
        if should_abort and should_abort():
            errors.append({"subforum_key": "selftest", "error": "Aborted"})
            break
        subforum_key = str(thread.get("subforum_key"))
        subforum = subforums.get(subforum_key)
        if not subforum:
            errors.append({"subforum_key": subforum_key, "error": "Unknown subforum"})
            continue

        if subforum_key in subforum_failed:
            continue

        numeric_id = str(thread.get("thread_numeric_id"))
        max_pages = int(max_pages_override) if max_pages_override and max_pages_override > 0 else SEQUENTIAL_PAGE_DEPTH
        found = False

        for page in range(1, max_pages + 1):
            if should_abort and should_abort():
                errors.append({"subforum_key": subforum_key, "error": "Aborted"})
                break
            subforum_pages = page_cache.setdefault(subforum_key, {})
            by_id = subforum_pages.get(page)
            if by_id is None:
                if set_action:
                    set_action(f"Fetching {subforum['name']} page {page}")
                url = build_page_url(subforum["url"], page)
                html = None
                for attempt in range(max_retries + 1):
                    if should_abort and should_abort():
                        errors.append({"subforum_key": subforum_key, "error": "Aborted"})
                        break
                    try:
                        wait_budget()
                        requests_made += 1
                        request_headers = _headers()
                        if log_http:
                            log_http(
                                {
                                    "ts": utc_now(),
                                    "phase": "request",
                                    "kind": "listing",
                                    "url": url,
                                    "attempt": attempt + 1,
                                    "method": "GET",
                                    "request_headers": request_headers,
                                }
                            )
                        resp = session.get(url, headers=request_headers, timeout=20)
                        if log_http:
                            log_http(
                                {
                                    "ts": utc_now(),
                                    "phase": "response",
                                    "kind": "listing",
                                    "url": url,
                                    "attempt": attempt + 1,
                                    "status_code": resp.status_code,
                                    "response_headers": dict(resp.headers),
                                    "body": resp.text,
                                }
                            )
                        if resp.status_code in {403, 429}:
                            time.sleep(6 + attempt * 4)
                        resp.raise_for_status()
                        html = resp.text
                        break
                    except Exception as exc:  # noqa: BLE001
                        if log_http:
                            log_http(
                                {
                                    "ts": utc_now(),
                                    "phase": "error",
                                    "kind": "listing",
                                    "url": url,
                                    "attempt": attempt + 1,
                                    "error": str(exc),
                                }
                            )
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
                    subforum_failed.add(subforum_key)
                    break
                listing_rows = parse_listing_rows(html)
                by_id = {row["thread_numeric_id"]: row for row in listing_rows}
                subforum_pages[page] = by_id

            row = by_id.get(numeric_id)
            if not row or row.get("views") is None:
                continue

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
                    "source": "listing",
                }
            )
            updated_threads += 1
            found = True
            break
        if found:
            continue

        if not enable_search_fallback:
            continue

        forum_node_id = extract_forum_node_id(subforum.get("url"))
        keywords = build_search_keywords(thread)
        if not forum_node_id or not keywords:
            continue

        if set_action:
            set_action(f"Searching {subforum['name']} for thread {numeric_id}")
        search_url = build_search_url(keywords, forum_node_id)
        search_html = None
        for attempt in range(max_retries + 1):
            try:
                wait_budget()
                requests_made += 1
                request_headers = _headers()
                if log_http:
                    log_http(
                        {
                            "ts": utc_now(),
                            "phase": "request",
                            "kind": "search",
                            "url": search_url,
                            "attempt": attempt + 1,
                            "method": "GET",
                            "request_headers": request_headers,
                        }
                    )
                resp = session.get(search_url, headers=request_headers, timeout=20)
                if log_http:
                    log_http(
                        {
                            "ts": utc_now(),
                            "phase": "response",
                            "kind": "search",
                            "url": search_url,
                            "attempt": attempt + 1,
                            "status_code": resp.status_code,
                            "response_headers": dict(resp.headers),
                            "body": resp.text,
                        }
                    )
                if resp.status_code in {403, 429}:
                    time.sleep(6 + attempt * 4)
                resp.raise_for_status()
                search_html = resp.text
                break
            except Exception as exc:  # noqa: BLE001
                if log_http:
                    log_http(
                        {
                            "ts": utc_now(),
                            "phase": "error",
                            "kind": "search",
                            "url": search_url,
                            "attempt": attempt + 1,
                            "error": str(exc),
                        }
                    )
                if attempt >= max_retries:
                    errors.append(
                        {
                            "subforum_key": subforum_key,
                            "error": f"Search failed ({search_url}): {exc}",
                        }
                    )
                else:
                    time.sleep(2 + attempt * 2)
        if not search_html:
            continue

        search_rows = parse_search_rows(search_html)
        search_by_id = {row["thread_numeric_id"]: row for row in search_rows}
        row = search_by_id.get(numeric_id)
        if not row:
            continue

        if row.get("title"):
            thread["last_seen_title"] = row["title"]
        if row.get("views") is None:
            continue

        thread["last_seen_at"] = utc_now()
        thread["last_view_count"] = int(row["views"])
        thread["last_found_page"] = None
        thread["last_found_above"] = row.get("position")
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
                "page": None,
                "above": row.get("position"),
                "observed_title": row.get("title"),
                "source": "search",
            }
        )
        updated_threads += 1

    finished_at = utc_now()
    result = UpdateResult(
        started_at=started_at,
        finished_at=finished_at,
        checked_threads=len(active_threads),
        updated_threads=updated_threads,
        requests_made=requests_made,
        errors=errors,
    )
    return config, threads_payload, samples_updates, result


def check_bladeforums_auth(auth_cookies: dict[str, str] | None = None) -> tuple[bool, str]:
    session = requests.Session()
    if auth_cookies:
        session.cookies.update(auth_cookies)
    resp = session.get(
        "https://www.bladeforums.com/search/?type=post",
        headers=_headers(),
        timeout=20,
        allow_redirects=True,
    )
    text = resp.text.lower()
    if resp.status_code >= 400:
        return False, f"http_{resp.status_code}"
    if "login" in str(resp.url).lower() and "search" not in str(resp.url).lower():
        return False, "redirected_to_login"
    if "search titles only" in text or "search in forums" in text:
        return True, "authenticated_search_available"
    if "you must be logged in" in text or "you must be logged-in" in text:
        return False, "search_requires_login"
    return False, "auth_status_unknown"
