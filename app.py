from __future__ import annotations

import io
import json
import os
import platform
import re
import subprocess
import time
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
try:
    from streamlit_sortables import sort_items
except Exception:  # noqa: BLE001
    sort_items = None

from ui.data_client import DataSource, fetch_json
from ui.github_client import GithubClient, GithubConfig
from ui.models import thread_id_for, utc_now
from ui.tracker_engine import due_for_run, next_run_timestamp, run_update


st.set_page_config(
    page_title="BladeForums View Tracker",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
.history-table {
  width: max-content;
  min-width: 100%;
  border-collapse: collapse;
  table-layout: auto;
}
.history-table th, .history-table td {
  border: 1px solid #d7d7d7;
  padding: 4px;
  text-align: right;
  font-size: 0.72rem;
}
.history-table th {
  text-align: left;
  white-space: normal;
  overflow-wrap: anywhere;
  line-height: 1rem;
  vertical-align: bottom;
  min-width: 7.5rem;
  max-width: 12rem;
}
.history-wrap {
  width: 100%;
  overflow-x: auto;
}
.history-table td.ts-col {
  text-align: left;
  white-space: nowrap;
}
.panel-row-label {
  font-size: 0.78rem;
  line-height: 1rem;
  margin-top: 0.2rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.panel-head {
  font-size: 0.7rem;
  color: #666;
  font-weight: 600;
  margin-top: 0.15rem;
}
</style>
""",
    unsafe_allow_html=True,
)

NY_TZ = ZoneInfo("America/New_York")
THREAD_ID_INPUT_RE = re.compile(r"(?:^|\.)(\d+)(?:/)?(?:[#?].*)?$")
TITLE_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]


def to_ny_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(NY_TZ)


def to_ny_24h(value: str | None) -> str:
    dt = to_ny_dt(value)
    if not dt:
        return "N/A" if value is None else str(value)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_setting(key: str, default: str | None = None) -> str | None:
    if key in st.secrets:
        return st.secrets.get(key)
    return os.getenv(key, default)


def build_clients() -> tuple[DataSource, GithubClient | None, str, str]:
    repo = get_setting("TRACKER_REPO")
    branch = get_setting("TRACKER_BRANCH", "main")
    token = get_setting("GITHUB_TOKEN")
    if not repo:
        st.error("TRACKER_REPO is not configured")
        st.stop()
    source = DataSource(raw_base=f"https://raw.githubusercontent.com/{repo}/{branch}")
    github = GithubClient(GithubConfig(repo=repo, branch=branch, token=token)) if token else None
    return source, github, repo, branch


def fetch_or_default(source: DataSource, path: str, default: dict[str, Any]) -> dict[str, Any]:
    try:
        return fetch_json(source, path)
    except Exception:  # noqa: BLE001
        return default


def _deepcopy_doc(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload))


def init_session_docs(source: DataSource, force_reload: bool = False) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if force_reload or "config_doc" not in st.session_state:
        st.session_state["config_doc"] = fetch_or_default(
            source,
            "data/config.json",
            {"schema_version": 1, "tracker": {}, "global": {}, "subforums": []},
        )
    if force_reload or "threads_doc" not in st.session_state:
        st.session_state["threads_doc"] = fetch_or_default(source, "data/threads.json", {"schema_version": 1, "threads": []})
    if force_reload or "runtime_doc" not in st.session_state:
        st.session_state["runtime_doc"] = load_runtime(source)
    if force_reload or "catalog_doc" not in st.session_state:
        st.session_state["catalog_doc"] = load_catalog(source)
    if force_reload:
        st.session_state["sample_cache"] = {}
    return (
        _deepcopy_doc(st.session_state["config_doc"]),
        _deepcopy_doc(st.session_state["threads_doc"]),
        _deepcopy_doc(st.session_state["runtime_doc"]),
        _deepcopy_doc(st.session_state["catalog_doc"]),
    )


def store_session_docs(
    *,
    config: dict[str, Any] | None = None,
    threads_payload: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    catalog: dict[str, Any] | None = None,
) -> None:
    if config is not None:
        st.session_state["config_doc"] = _deepcopy_doc(config)
    if threads_payload is not None:
        st.session_state["threads_doc"] = _deepcopy_doc(threads_payload)
    if runtime is not None:
        st.session_state["runtime_doc"] = _deepcopy_doc(runtime)
    if catalog is not None:
        st.session_state["catalog_doc"] = _deepcopy_doc(catalog)


def parse_thread_numeric_id(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        return text
    match = THREAD_ID_INPUT_RE.search(text)
    return match.group(1) if match else None


def _is_forbidden_error(exc: Exception) -> bool:
    text = str(exc)
    return "403" in text or "Forbidden" in text


def _mark_write_forbidden(exc: Exception) -> None:
    st.session_state["repo_write_forbidden"] = True
    st.session_state["repo_write_forbidden_error"] = str(exc)


def put_json(github: GithubClient, path: str, payload: dict[str, Any], message: str) -> bool:
    attempts = 4
    for attempt in range(attempts):
        try:
            current, sha = github.get_file(path)
        except Exception:  # noqa: BLE001
            current, sha = None, None
        if current == payload:
            return
        try:
            github.put_file(path, payload, message, sha)
            return True
        except Exception as exc:  # noqa: BLE001
            if _is_forbidden_error(exc):
                _mark_write_forbidden(exc)
                return False
            text = str(exc)
            is_conflict = "409" in text or "Conflict" in text
            if not is_conflict or attempt >= attempts - 1:
                raise
            time.sleep(0.15 * (attempt + 1))
    return False


def append_process_log(github: GithubClient, path: str, records: list[dict[str, Any]], message: str) -> bool:
    if not records:
        return True
    chunk = "".join(json.dumps(r, sort_keys=True) + "\n" for r in records)
    attempts = 4
    for attempt in range(attempts):
        try:
            current_text, sha = github.get_text_file(path)
        except Exception:  # noqa: BLE001
            current_text, sha = "", None
        new_text = current_text + chunk
        try:
            github.put_text_file(path, new_text, message, sha)
            return True
        except Exception as exc:  # noqa: BLE001
            if _is_forbidden_error(exc):
                _mark_write_forbidden(exc)
                return False
            text = str(exc)
            is_conflict = "409" in text or "Conflict" in text
            if not is_conflict or attempt >= attempts - 1:
                raise
            time.sleep(0.15 * (attempt + 1))
    return False


def put_text(github: GithubClient, path: str, text_payload: str, message: str) -> bool:
    attempts = 4
    for attempt in range(attempts):
        try:
            current_text, sha = github.get_text_file(path)
        except Exception:  # noqa: BLE001
            current_text, sha = "", None
        if current_text == text_payload:
            return
        try:
            github.put_text_file(path, text_payload, message, sha)
            return True
        except Exception as exc:  # noqa: BLE001
            if _is_forbidden_error(exc):
                _mark_write_forbidden(exc)
                return False
            text = str(exc)
            is_conflict = "409" in text or "Conflict" in text
            if not is_conflict or attempt >= attempts - 1:
                raise
            time.sleep(0.15 * (attempt + 1))
    return False


def load_runtime(source: DataSource) -> dict[str, Any]:
    runtime = fetch_or_default(
        source,
        "data/runtime.json",
        {
            "current_action": "idle",
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_result": "never",
            "last_run_summary": {},
            "next_run_at": None,
            "events": [],
        },
    )
    runtime.setdefault("current_action", "idle")
    runtime.setdefault("last_run_started_at", None)
    runtime.setdefault("last_run_finished_at", None)
    runtime.setdefault("last_run_result", "never")
    runtime.setdefault("last_run_summary", {})
    runtime.setdefault("next_run_at", None)
    runtime.setdefault("events", [])
    return runtime


def load_catalog(source: DataSource) -> dict[str, Any]:
    payload = fetch_or_default(source, "data/thread_catalog.json", {"schema_version": 1, "threads": []})
    payload.setdefault("threads", [])
    return payload


def load_selftest_config(source: DataSource) -> dict[str, Any]:
    payload = fetch_or_default(
        source,
        "data/selftest_config.json",
        {
            "schema_version": 1,
            "target": {
                "thread_numeric_id": "2066634",
                "subforum_key": "for-sale-folding-knives-individual.892",
                "display_name": "Self-Test Target: Dodo",
            },
            "delay_seconds": 4,
            "max_repair_attempts": 2,
        },
    )
    payload.setdefault("target", {})
    payload.setdefault("delay_seconds", 4)
    payload.setdefault("max_repair_attempts", 2)
    return payload


def load_selftest_runtime(source: DataSource) -> dict[str, Any]:
    payload = fetch_or_default(
        source,
        "data/selftest_runtime.json",
        {
            "status": "idle",
            "stage": "idle",
            "run_started_at": None,
            "run_finished_at": None,
            "abort_requested": False,
            "thread_id": None,
            "next_action_at": None,
            "repair_attempts": 0,
            "last_error": None,
            "last_result": None,
        },
    )
    payload.setdefault("status", "idle")
    payload.setdefault("stage", "idle")
    payload.setdefault("abort_requested", False)
    payload.setdefault("repair_attempts", 0)
    return payload


def load_selftest_report(source: DataSource) -> dict[str, Any]:
    payload = fetch_or_default(source, "data/selftest_report.json", {"schema_version": 1, "logs": []})
    payload.setdefault("logs", [])
    return payload


def append_selftest_log(
    report: dict[str, Any],
    action: str,
    ok: bool,
    details: str,
    *,
    expected: str | None = None,
    observed: str | None = None,
    remedy: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    report.setdefault("logs", []).append(
        {
            "ts": utc_now(),
            "action": action,
            "ok": bool(ok),
            "details": details,
            "expected": expected,
            "observed": observed,
            "remedy": remedy,
            "meta": meta or {},
        }
    )
    report["logs"] = report["logs"][-600:]


def upsert_catalog_entries(catalog: dict[str, Any], threads: list[dict[str, Any]]) -> dict[str, Any]:
    by_id = {str(t.get("id")): t for t in catalog.get("threads", []) if t.get("id")}
    for thread in threads:
        thread_id = str(thread.get("id"))
        if not thread_id:
            continue
        entry = by_id.get(thread_id, {"id": thread_id, "created_at": utc_now()})
        entry["display_name"] = thread.get("display_name")
        entry["thread_numeric_id"] = thread.get("thread_numeric_id")
        entry["subforum_key"] = thread.get("subforum_key")
        entry["last_seen_title"] = thread.get("last_seen_title")
        entry["current_title"] = thread.get("current_title")
        entry["status"] = thread.get("status", "paused")
        entry["include_in_adhoc"] = bool(thread.get("include_in_adhoc", False))
        by_id[thread_id] = entry
    catalog["threads"] = sorted(by_id.values(), key=lambda x: (x.get("created_at", ""), x.get("id", "")))
    return catalog


def append_event(runtime: dict[str, Any], level: str, message: str) -> None:
    runtime.setdefault("events", []).append({"ts": utc_now(), "level": level, "message": message})
    runtime["events"] = runtime["events"][-200:]


def update_runtime_file(github: GithubClient, runtime: dict[str, Any], message: str) -> None:
    put_json(github, "data/runtime.json", runtime, message)


def normalize_threads_defaults(threads_payload: dict[str, Any]) -> bool:
    changed = False
    threads = threads_payload.get("threads", [])
    for idx, thread in enumerate(sorted(threads, key=lambda x: x.get("order", 10_000))):
        if "include_in_adhoc" not in thread:
            thread["include_in_adhoc"] = True
            changed = True
        if "status" not in thread:
            thread["status"] = "active"
            changed = True
        if "order" not in thread:
            thread["order"] = idx
            changed = True
        if "title_history" not in thread:
            thread["title_history"] = []
            changed = True
        if "title_color_map" not in thread:
            thread["title_color_map"] = {}
            changed = True
    return changed


def persist_threads_doc(github: GithubClient, threads_payload: dict[str, Any], message: str) -> None:
    threads_doc, _ = github.get_file("data/threads.json")
    threads_doc["threads"] = threads_payload.get("threads", [])
    put_json(github, "data/threads.json", threads_doc, message)


def load_sample_payload(github: GithubClient, thread: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    thread_id = thread["id"]
    try:
        payload, sha = github.get_file(f"data/samples/{thread_id}.json")
        payload.setdefault("thread_id", thread_id)
        payload.setdefault("samples", [])
        return payload, sha
    except Exception:  # noqa: BLE001
        return (
            {
                "thread_id": thread_id,
                "thread_numeric_id": thread.get("thread_numeric_id"),
                "title": thread.get("display_name") or f"Thread {thread_id}",
                "samples": [],
            },
            None,
        )


def ensure_title_color(thread: dict[str, Any], title: str) -> str:
    observed_title = title.strip() if title else "(Unknown Title)"
    color_map = thread.setdefault("title_color_map", {})
    title_history = thread.setdefault("title_history", [])
    if observed_title not in color_map:
        color_map[observed_title] = TITLE_COLORS[len(title_history) % len(TITLE_COLORS)]
        title_history.append(observed_title)
    return color_map[observed_title]


def persist_update_results(
    github: GithubClient,
    threads_payload: dict[str, Any],
    sample_updates: dict[str, dict[str, Any]],
    summary: dict[str, Any],
    runtime: dict[str, Any],
) -> None:
    persist_threads_doc(github, threads_payload, "Update thread stats")
    store_session_docs(threads_payload=threads_payload)
    threads_by_id = {t["id"]: t for t in threads_payload.get("threads", [])}

    for thread_id, update_payload in sample_updates.items():
        thread = threads_by_id.get(thread_id)
        if not thread:
            continue
        payload, _ = load_sample_payload(github, thread)
        payload.setdefault("samples", []).extend(update_payload.get("samples", []))
        payload["thread_numeric_id"] = thread.get("thread_numeric_id")
        payload["title"] = thread.get("current_title") or thread.get("display_name")
        put_json(github, f"data/samples/{thread_id}.json", payload, f"Append samples {thread_id}")
        st.session_state.setdefault("sample_cache", {})[thread_id] = _deepcopy_doc(payload)

    runtime["last_run_summary"] = summary
    update_runtime_file(github, runtime, "Update runtime after tracker run")
    store_session_docs(runtime=runtime)


def execute_update(
    github: GithubClient,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    runtime: dict[str, Any],
    selected_thread_ids: set[str] | None,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    tracker_state = config.get("tracker", {}).get("state", "stopped")
    runtime["current_action"] = "updating" if tracker_state != "paused" else "updating (paused)"
    runtime["last_run_started_at"] = utc_now()
    runtime["last_run_result"] = "running"
    append_event(runtime, "info", f"Run started ({reason})")
    update_runtime_file(github, runtime, "Tracker run started")
    enable_process_logging = bool(config.get("global", {}).get("enable_process_logging", False))
    process_logs: list[dict[str, Any]] = []

    def set_action(text: str) -> None:
        state = config.get("tracker", {}).get("state", "stopped")
        runtime["current_action"] = text if state != "paused" else f"{text} (paused)"

    def log_http(record: dict[str, Any]) -> None:
        if enable_process_logging:
            process_logs.append(record)

    config, threads_payload, sample_updates, result = run_update(
        config=config,
        threads_payload=threads_payload,
        selected_thread_ids=selected_thread_ids,
        set_action=set_action,
        log_http=log_http,
    )

    by_id = {t["id"]: t for t in threads_payload.get("threads", [])}
    for thread_id, update_payload in sample_updates.items():
        thread = by_id.get(thread_id)
        if not thread:
            continue
        observed_title = thread.get("last_seen_title") or thread.get("current_title") or thread.get("display_name") or "(Unknown Title)"
        color = ensure_title_color(thread, observed_title)
        thread["current_title"] = observed_title
        thread["current_title_color"] = color
        for sample in update_payload.get("samples", []):
            sample["observed_title"] = sample.get("observed_title") or observed_title
            sample["title_color"] = ensure_title_color(thread, sample["observed_title"])

    runtime["last_run_finished_at"] = result.finished_at
    runtime["last_run_result"] = "ok" if not result.errors else "warning"
    runtime["next_run_at"] = next_run_timestamp(int(config.get("tracker", {}).get("interval_seconds", 1800)))

    state = config.get("tracker", {}).get("state", "stopped")
    runtime["current_action"] = "paused" if state == "paused" else "idle"

    summary = {
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "checked_threads": result.checked_threads,
        "updated_threads": result.updated_threads,
        "requests_made": result.requests_made,
        "errors": result.errors,
        "reason": reason,
    }
    if result.errors:
        append_event(runtime, "warning", f"Run finished with {len(result.errors)} errors")
    else:
        append_event(runtime, "info", "Run finished successfully")

    persist_update_results(github, threads_payload, sample_updates, summary, runtime)
    if enable_process_logging and process_logs:
        append_process_log(github, "data/process_log.jsonl", process_logs, "Append BladeForums process log")
    store_session_docs(config=config, threads_payload=threads_payload, runtime=runtime)
    return config, threads_payload, runtime, summary


def render_status(config: dict[str, Any], runtime: dict[str, Any]) -> None:
    state = config.get("tracker", {}).get("state", "stopped")
    action = runtime.get("current_action", "idle")
    if state == "paused" and action == "idle":
        action = "paused"
    elif state == "paused" and "(paused)" not in action:
        action = f"{action} (paused)"

    st.markdown(
        """
<style>
.status-line{font-size:0.78rem;line-height:1.2rem;margin:0.15rem 0;}
.status-k{font-weight:600;}
</style>
""",
        unsafe_allow_html=True,
    )
    state_text = "Running" if state == "running" else ("Paused" if state == "paused" else "Stopped")
    next_run = runtime.get("next_run_at") if state == "running" else None
    st.markdown(f"<div class='status-line'><span class='status-k'>State:</span> {state_text}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='status-line'><span class='status-k'>Current action:</span> {action}</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='status-line'><span class='status-k'>Last run:</span> {to_ny_24h(runtime.get('last_run_finished_at'))}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(f"<div class='status-line'><span class='status-k'>Next run:</span> {to_ny_24h(next_run)}</div>", unsafe_allow_html=True)


def load_samples(source: DataSource, thread_id: str) -> dict[str, Any]:
    cache: dict[str, dict[str, Any]] = st.session_state.setdefault("sample_cache", {})
    if thread_id not in cache:
        cache[thread_id] = fetch_or_default(source, f"data/samples/{thread_id}.json", {"thread_id": thread_id, "samples": []})
    return _deepcopy_doc(cache[thread_id])


def effective_cards_per_row(requested: int) -> int:
    # Best-effort user-agent detection to prevent crowded multi-column graphs on phones.
    try:
        headers = st.context.headers  # type: ignore[attr-defined]
        user_agent = headers.get("User-Agent", "") if headers else ""
    except Exception:  # noqa: BLE001
        user_agent = ""

    ua = user_agent.lower()
    if "mobile" in ua and "ipad" not in ua and "tablet" not in ua:
        return min(requested, 1)
    if "ipad" in ua or "tablet" in ua:
        return min(requested, 2)
    return requested


def abbreviate_label(label: str, width: int = 12) -> str:
    text = " ".join(str(label).split()).strip()
    if len(text) <= width:
        return text
    return text[: width - 1] + "…"


def build_history_table(source: DataSource, threads: list[dict[str, Any]]) -> tuple[pd.DataFrame, dict[tuple[str, str], str]]:
    rows: list[dict[str, Any]] = []
    color_lookup: dict[tuple[str, str], str] = {}

    for thread in threads:
        thread_id = thread.get("id")
        if not thread_id:
            continue
        col_name = str(thread.get("display_name") or thread.get("current_title") or thread_id)
        samples_payload = load_samples(source, thread_id)
        for sample in samples_payload.get("samples", []):
            ts = to_ny_24h(sample.get("ts"))
            views = sample.get("views")
            if views is None:
                continue
            rows.append({"ts": ts, "thread": col_name, "value": int(views)})
            if sample.get("title_color"):
                color_lookup[(ts, col_name)] = str(sample["title_color"])

    if not rows:
        return pd.DataFrame(), color_lookup

    df = pd.DataFrame(rows)
    pivot = df.pivot_table(index="ts", columns="thread", values="value", aggfunc="last")
    pivot = pivot.sort_index(ascending=False)
    return pivot, color_lookup


def render_history_html(df: pd.DataFrame, color_lookup: dict[tuple[str, str], str]) -> None:
    if df.empty:
        st.info("No samples available")
        return

    headers = ["Timestamp"] + list(df.columns)
    html = ["<table class='history-table'><thead><tr>"]
    for header in headers:
        html.append(f"<th>{header}</th>")
    html.append("</tr></thead><tbody>")

    for ts, row in df.iterrows():
        html.append("<tr>")
        html.append(f"<td class='ts-col'>{ts}</td>")
        for col in df.columns:
            value = row[col]
            if pd.isna(value):
                html.append("<td></td>")
            else:
                color = color_lookup.get((ts, col), "#111111")
                html.append(f"<td style='color:{color};font-weight:600'>{int(value)}</td>")
        html.append("</tr>")

    html.append("</tbody></table>")
    st.markdown(f"<div class='history-wrap'>{''.join(html)}</div>", unsafe_allow_html=True)


def render_title_legend(thread: dict[str, Any]) -> None:
    title_history = thread.get("title_history", [])
    color_map = thread.get("title_color_map", {})
    if not title_history:
        return
    st.markdown("**Observed titles (in order)**")
    for idx, title in enumerate(title_history, start=1):
        color = color_map.get(title, "#111111")
        st.markdown(f"<span style='color:{color};'>{idx}. {title}</span>", unsafe_allow_html=True)


def choose_dtick_ms(ts_values: pd.Series) -> int | None:
    if ts_values.empty:
        return None
    span = ts_values.max() - ts_values.min()
    hours = span.total_seconds() / 3600 if span is not pd.NaT else 0
    if hours <= 24:
        return 60 * 60 * 1000
    if hours <= 72:
        return 2 * 60 * 60 * 1000
    if hours <= 7 * 24:
        return 6 * 60 * 60 * 1000
    return None


def build_axis_ticks(start_ts: pd.Timestamp, end_ts: pd.Timestamp, count: int = 6) -> tuple[list[pd.Timestamp], list[str]]:
    start = pd.Timestamp(start_ts)
    end = pd.Timestamp(end_ts)
    if end < start:
        start, end = end, start
    if start == end:
        start = start - pd.Timedelta(minutes=2)
        end = end + pd.Timedelta(minutes=2)
    tick_vals = list(pd.date_range(start=start, end=end, periods=max(2, count)))
    tick_text = [ts.strftime("%H:%M\n%Y-%m-%d") for ts in tick_vals]
    return tick_vals, tick_text


def sorted_threads(threads_payload: dict[str, Any]) -> list[dict[str, Any]]:
    threads = [
        t
        for t in threads_payload.get("threads", [])
        if not bool(t.get("is_self_test")) and not bool(t.get("is_selftest")) and not str(t.get("id", "")).startswith("selftest-")
    ]
    return sorted(threads, key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))


def thread_label(thread: dict[str, Any]) -> str:
    label = str(thread.get("display_name") or thread.get("current_title") or thread.get("id"))
    if not thread.get("thread_numeric_id") and "-" in label:
        parts = label.split("-")
        if len(parts) > 1:
            label = "-".join(parts[:-1]).strip()
    return label


def sync_layout_rows(threads: list[dict[str, Any]], rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    existing = {str(item.get("thread_id")): item for item in (rows or [])}
    ordered: list[dict[str, Any]] = []
    for thread in threads:
        thread_id = str(thread.get("id"))
        current = existing.get(thread_id, {})
        ordered.append(
            {
                "thread_id": thread_id,
                "show_card": bool(current.get("show_card", True)),
                "show_x_range": bool(current.get("show_x_range", False)),
            }
        )
    return ordered


def sync_tracker_rows(threads: list[dict[str, Any]], rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    existing = {str(item.get("thread_id")): item for item in (rows or [])}
    ordered: list[dict[str, Any]] = []
    for thread in threads:
        thread_id = str(thread.get("id"))
        current = existing.get(thread_id, {})
        ordered.append(
            {
                "thread_id": thread_id,
                "track": bool(current.get("track", thread.get("status", "active") == "active")),
                "adhoc": bool(current.get("adhoc", thread.get("include_in_adhoc", True))),
            }
        )
    return ordered


def rows_dirty(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> bool:
    return json.dumps(a, sort_keys=True) != json.dumps(b, sort_keys=True)


def selftest_thread_id(target: dict[str, Any]) -> str:
    return thread_id_for(
        f"selftest-{target.get('thread_numeric_id')}-{target.get('subforum_key')}",
        str(target.get("subforum_key", "selftest")),
    )


def summarize_selftest_failure(logs: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in logs:
        if not bool(item.get("ok", True)):
            action = str(item.get("action", "unknown"))
            observed = str(item.get("observed") or item.get("details") or "n/a")
            remedy = str(item.get("remedy") or "n/a")
            likely = "unknown"
            lower = f"{action} {observed}".lower()
            if "http_response" in lower and "status=403" in lower:
                likely = "Access blocked by remote site (403)"
            elif "http_response" in lower and "status=429" in lower:
                likely = "Rate limiting by remote site (429)"
            elif "no sample" in lower:
                likely = "Parsing or persistence path did not produce sample data"
            elif "search failed" in lower:
                likely = "Search fallback request failed"
            elif "fetch failed" in lower:
                likely = "Sequential page retrieval failed"
            return {
                "action": action,
                "observed": observed,
                "likely_cause": likely,
                "suggested_remedy": remedy,
                "ts": item.get("ts"),
            }
    return None


DIAG_COMMANDS: dict[str, list[str]] = {
    "pwd": ["pwd"],
    "ls_root": ["ls", "-la", "/mount/src"],
    "ls_app": ["ls", "-la", "/mount/src/bf-ui"],
    "ls_tracker_data": ["ls", "-la", "/mount/src/bf-tracker/data"],
    "find_top": ["find", "/mount/src", "-maxdepth", "3", "-type", "d"],
}

CONSOLE_ALLOWED_COMMANDS: dict[str, list[str]] = {
    "pwd": ["pwd"],
    "ls": ["ls"],
    "ls -la": ["ls", "-la"],
    "ls /tmp": ["ls", "/tmp"],
    "ls -la /tmp": ["ls", "-la", "/tmp"],
    "ls /mount/src": ["ls", "/mount/src"],
    "ls -la /mount/src": ["ls", "-la", "/mount/src"],
    "ls -la /mount/src/bf-ui": ["ls", "-la", "/mount/src/bf-ui"],
    "ls -la /mount/src/bf-tracker": ["ls", "-la", "/mount/src/bf-tracker"],
    "find . -maxdepth 3 -type f": ["find", ".", "-maxdepth", "3", "-type", "f"],
    "find /tmp -maxdepth 3 -type f": ["find", "/tmp", "-maxdepth", "3", "-type", "f"],
    "ps -ef": ["ps", "-ef"],
    "env": ["env"],
}


def collect_tree(root: str, max_depth: int = 3, max_entries: int = 500) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        if depth > max_depth:
            dirnames[:] = []
            continue
        rows.append({"type": "dir", "path": dirpath, "depth": depth})
        for name in sorted(filenames):
            fpath = os.path.join(dirpath, name)
            try:
                size = os.path.getsize(fpath)
            except OSError:
                size = None
            rows.append({"type": "file", "path": fpath, "depth": depth + 1, "size": size})
            if len(rows) >= max_entries:
                return rows
        if len(rows) >= max_entries:
            return rows
    return rows


def load_diagnostics(source: DataSource) -> dict[str, Any]:
    payload = fetch_or_default(source, "data/diagnostics.json", {"schema_version": 1, "events": []})
    payload.setdefault("events", [])
    return payload


def append_diagnostics_event(diag: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    diag.setdefault("events", []).append(event)
    diag["events"] = diag["events"][-300:]
    return diag


def query_flag(name: str) -> bool:
    try:
        raw = st.query_params.get(name)
    except Exception:  # noqa: BLE001
        return False
    if isinstance(raw, list):
        value = raw[0] if raw else ""
    else:
        value = raw
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def resolve_console_command(raw: str) -> tuple[list[str] | None, str | None]:
    text = " ".join((raw or "").strip().split())
    if not text:
        return None, "Enter a command"
    if text in CONSOLE_ALLOWED_COMMANDS:
        return CONSOLE_ALLOWED_COMMANDS[text], None
    if text.startswith("cat "):
        name = text[4:].strip()
        if not name or os.path.basename(name) != name:
            return None, "Only file names in the app working directory are allowed"
        path = os.path.join(os.getcwd(), name)
        if not os.path.isfile(path):
            return None, f"File not found in working directory: {name}"
        return ["cat", path], None
    if text.startswith("head -n "):
        parts = text.split(" ", 3)
        if len(parts) != 4:
            return None, "Use format: head -n <count> <filename>"
        count = parts[2]
        name = parts[3].strip()
        if not count.isdigit():
            return None, "head count must be a positive integer"
        if not name or os.path.basename(name) != name:
            return None, "Only file names in the app working directory are allowed"
        path = os.path.join(os.getcwd(), name)
        if not os.path.isfile(path):
            return None, f"File not found in working directory: {name}"
        return ["head", "-n", count, path], None
    return None, "Command is not allowed"


def run_local_update_if_due(
    github: GithubClient | None,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    runtime: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bool]:
    if not github:
        return config, threads_payload, runtime, False
    if bool(st.session_state.get("repo_write_forbidden", False)):
        return config, threads_payload, runtime, False
    state = config.get("tracker", {}).get("state", "stopped")
    if state != "running":
        return config, threads_payload, runtime, False

    has_trackable = any(
        t.get("status", "active") == "active" and t.get("thread_numeric_id")
        for t in threads_payload.get("threads", [])
    )
    if not has_trackable:
        return config, threads_payload, runtime, False
    if runtime.get("current_action", "idle").startswith("updating"):
        return config, threads_payload, runtime, False
    if not due_for_run(runtime.get("next_run_at")):
        return config, threads_payload, runtime, False

    try:
        config, threads_payload, runtime, _ = execute_update(
            github,
            config,
            threads_payload,
            runtime,
            selected_thread_ids=None,
            reason="interval",
        )
        return config, threads_payload, runtime, True
    except Exception as exc:  # noqa: BLE001
        if _is_forbidden_error(exc):
            _mark_write_forbidden(exc)
            runtime["current_action"] = "write blocked (GitHub 403)"
            append_event(runtime, "error", f"Write blocked: {exc}")
            return config, threads_payload, runtime, False
        raise


def main() -> None:
    source, github, repo, branch = build_clients()
    read_only = github is None

    try:
        force_reload = bool(st.session_state.pop("force_reload_docs", False))
        config, threads_payload, runtime, catalog = init_session_docs(source, force_reload=force_reload)
        selftest_cfg = load_selftest_config(source)
        selftest_runtime = load_selftest_runtime(source)
        selftest_report = load_selftest_report(source)
        diagnostics = load_diagnostics(source)
    except Exception as exc:  # noqa: BLE001
        if github:
            try:
                payload, _ = github.get_file("data/ui_errors.json")
            except Exception:  # noqa: BLE001
                payload = {"errors": []}
            payload.setdefault("errors", []).append({"ts": utc_now(), "error": str(exc), "traceback": traceback.format_exc()})
            put_json(github, "data/ui_errors.json", payload, "Log UI error")
        st.error("Unexpected error")
        st.exception(exc)
        st.stop()

    # Keep immediate local consistency after writes to avoid perceived no-op ordering changes.
    if "threads_override" in st.session_state:
        threads_payload["threads"] = st.session_state["threads_override"]
        st.session_state["threads_doc"] = _deepcopy_doc(threads_payload)
        catalog = upsert_catalog_entries(catalog, threads_payload.get("threads", []))
        store_session_docs(catalog=catalog)

    tracker_cfg = config.setdefault("tracker", {})
    tracker_cfg.setdefault("state", "paused")
    if tracker_cfg.get("state") == "stopped":
        tracker_cfg["state"] = "paused"
    if "interval_seconds" not in tracker_cfg:
        if "interval_minutes" in tracker_cfg:
            tracker_cfg["interval_seconds"] = int(tracker_cfg.get("interval_minutes", 30)) * 60
        else:
            tracker_cfg["interval_seconds"] = 1800
    tracker_cfg.setdefault("start_immediately", True)

    defaults_changed = normalize_threads_defaults(threads_payload)
    if defaults_changed and github and not read_only:
        persist_threads_doc(github, threads_payload, "Normalize thread defaults")
        store_session_docs(threads_payload=threads_payload)
    catalog = upsert_catalog_entries(catalog, threads_payload.get("threads", []))
    store_session_docs(catalog=catalog)

    state = tracker_cfg.get("state", "stopped")
    if state == "running":
        st_autorefresh(interval=15000, key="tracker_refresh")
    if selftest_runtime.get("status") == "running":
        st_autorefresh(interval=1500, key="selftest_refresh")

    threads_sorted = sorted_threads(threads_payload)
    st.session_state["layout_applied"] = sync_layout_rows(threads_sorted, st.session_state.get("layout_applied"))
    st.session_state["layout_draft"] = sync_layout_rows(threads_sorted, st.session_state.get("layout_draft"))
    st.session_state["tracker_applied"] = sync_tracker_rows(threads_sorted, st.session_state.get("tracker_applied"))
    st.session_state["tracker_draft"] = sync_tracker_rows(threads_sorted, st.session_state.get("tracker_draft"))

    st.sidebar.header("Controls")
    show_console_tab = query_flag("console")
    side_tab_names = ["Tracker", "Threads", "Layout", "Display", "Stats", "Export"]
    if show_console_tab:
        side_tab_names.append("Console")
    side_tabs = st.sidebar.tabs(side_tab_names)
    side_tab_map = {name: side_tabs[idx] for idx, name in enumerate(side_tab_names)}
    if bool(st.session_state.get("repo_write_forbidden", False)):
        st.sidebar.error(f"Repository writes blocked: {st.session_state.get('repo_write_forbidden_error', 'GitHub 403')}")

    def hard_delete_thread(thread_id: str) -> None:
        threads_doc, threads_sha = github.get_file("data/threads.json")
        threads_doc["threads"] = [t for t in threads_doc.get("threads", []) if str(t.get("id")) != str(thread_id)]
        for idx, thread in enumerate(threads_doc.get("threads", [])):
            thread["order"] = idx
        github.put_file("data/threads.json", threads_doc, "Hard delete thread", threads_sha)

        try:
            _, sample_sha = github.get_text_file(f"data/samples/{thread_id}.json")
            github.delete_file(f"data/samples/{thread_id}.json", "Hard delete thread sample history", sample_sha)
        except Exception:  # noqa: BLE001
            pass

        catalog_doc = load_catalog(source)
        catalog_doc["threads"] = [t for t in catalog_doc.get("threads", []) if str(t.get("id")) != str(thread_id)]
        put_json(github, "data/thread_catalog.json", catalog_doc, "Hard delete thread from catalog")

        st.session_state.setdefault("sample_cache", {}).pop(str(thread_id), None)
        st.session_state["threads_override"] = threads_doc.get("threads", [])
        st.session_state["layout_applied"] = [r for r in st.session_state.get("layout_applied", []) if str(r.get("thread_id")) != str(thread_id)]
        st.session_state["layout_draft"] = [r for r in st.session_state.get("layout_draft", []) if str(r.get("thread_id")) != str(thread_id)]
        st.session_state["tracker_applied"] = [r for r in st.session_state.get("tracker_applied", []) if str(r.get("thread_id")) != str(thread_id)]
        st.session_state["tracker_draft"] = [r for r in st.session_state.get("tracker_draft", []) if str(r.get("thread_id")) != str(thread_id)]
        pending_ids = set(st.session_state.get("pending_registration_ids", []))
        pending_ids.discard(str(thread_id))
        st.session_state["pending_registration_ids"] = sorted(pending_ids)
        store_session_docs(threads_payload=threads_doc, catalog=catalog_doc)
        st.rerun()

    def persist_selftest_docs() -> None:
        put_json(github, "data/selftest_config.json", selftest_cfg, "Update self-test config")
        put_json(github, "data/selftest_runtime.json", selftest_runtime, "Update self-test runtime")
        put_json(github, "data/selftest_report.json", selftest_report, "Update self-test report")
        text_lines = [json.dumps(item, sort_keys=True) for item in selftest_report.get("logs", [])]
        put_text(
            github,
            "data/selftest_verbose_log.jsonl",
            ("\n".join(text_lines) + ("\n" if text_lines else "")),
            "Update self-test verbose log",
        )

    def persist_diagnostics_docs() -> None:
        put_json(github, "data/diagnostics.json", diagnostics, "Update diagnostics events")

    def ensure_selftest_entry() -> str:
        target = selftest_cfg.get("target", {})
        thread_id = selftest_thread_id(target)
        threads_doc, threads_sha = github.get_file("data/threads.json")
        threads = threads_doc.get("threads", [])
        existing = next((t for t in threads if str(t.get("id")) == thread_id), None)
        if not existing:
            order_val = max([x.get("order", -1) for x in threads] + [-1]) + 1
            threads.append(
                {
                    "id": thread_id,
                    "display_name": target.get("display_name") or f"Self-Test {target.get('thread_numeric_id')}",
                    "thread_numeric_id": str(target.get("thread_numeric_id")),
                    "subforum_key": target.get("subforum_key"),
                    "status": "paused",
                    "include_in_adhoc": False,
                    "order": order_val,
                    "created_at": utc_now(),
                    "title_history": [],
                    "title_color_map": {},
                    "is_self_test": True,
                }
            )
            threads_doc["threads"] = threads
            github.put_file("data/threads.json", threads_doc, "Ensure self-test thread", threads_sha)
            st.session_state["threads_override"] = threads
            store_session_docs(threads_payload=threads_doc)
        return thread_id

    def purge_selftest_traces() -> None:
        target = selftest_cfg.get("target", {})
        thread_id = selftest_thread_id(target)
        threads_doc, threads_sha = github.get_file("data/threads.json")
        threads_doc["threads"] = [t for t in threads_doc.get("threads", []) if str(t.get("id")) != thread_id]
        github.put_file("data/threads.json", threads_doc, "Purge self-test thread", threads_sha)
        try:
            _, sample_sha = github.get_text_file(f"data/samples/{thread_id}.json")
            github.delete_file(f"data/samples/{thread_id}.json", "Purge self-test samples", sample_sha)
        except Exception:  # noqa: BLE001
            pass
        catalog_doc = load_catalog(source)
        catalog_doc["threads"] = [t for t in catalog_doc.get("threads", []) if str(t.get("id")) != thread_id]
        put_json(github, "data/thread_catalog.json", catalog_doc, "Purge self-test catalog entry")
        st.session_state.setdefault("sample_cache", {}).pop(thread_id, None)
        st.session_state["threads_override"] = threads_doc.get("threads", [])
        st.session_state["layout_applied"] = [r for r in st.session_state.get("layout_applied", []) if str(r.get("thread_id")) != thread_id]
        st.session_state["layout_draft"] = [r for r in st.session_state.get("layout_draft", []) if str(r.get("thread_id")) != thread_id]
        store_session_docs(threads_payload=threads_doc, catalog=catalog_doc)

    def run_isolated_thread_update(thread_id: str, reason: str) -> tuple[bool, str, list[dict[str, Any]]]:
        tmp_threads = _deepcopy_doc(threads_payload)
        for thread in tmp_threads.get("threads", []):
            if str(thread.get("id")) == str(thread_id):
                thread["status"] = "active"
                break
        trace_events: list[dict[str, Any]] = []
        fast_cfg = _deepcopy_doc(config)
        fast_global = fast_cfg.setdefault("global", {})
        fast_global["min_delay_seconds"] = 0.0
        fast_global["max_delay_seconds"] = 0.2
        fast_global["max_retries"] = 0
        trace_events.append(
            {
                "ts": utc_now(),
                "action": "update_begin",
                "ok": True,
                "details": f"Running isolated update ({reason})",
                "expected": "One sample should be appended for self-test target",
                "observed": "Update started",
                "remedy": None,
                "meta": {"reason": reason},
            }
        )

        def _trace_action(msg: str) -> None:
            method = "search" if "Searching " in msg else "sequential_paging"
            trace_events.append(
                {
                    "ts": utc_now(),
                    "action": "tracker_action",
                    "ok": True,
                    "details": msg,
                    "expected": "Tracker should use sequential paging first, then search fallback if needed",
                    "observed": msg,
                    "remedy": None,
                    "meta": {"method": method},
                }
            )

        def _trace_http(record: dict[str, Any]) -> None:
            phase = str(record.get("phase", "unknown"))
            kind = str(record.get("kind", "unknown"))
            status_code = record.get("status_code")
            ok = bool(status_code is None or int(status_code) < 400)
            trace_events.append(
                {
                    "ts": record.get("ts", utc_now()),
                    "action": f"http_{phase}",
                    "ok": ok,
                    "details": f"{kind} {phase} {record.get('url')}",
                    "expected": "2xx responses and parseable thread row",
                    "observed": f"status={status_code}" if status_code is not None else str(record.get("error") or "request_sent"),
                    "remedy": "retry or fallback to search" if not ok else None,
                    "meta": record,
                }
            )

        _, updated_threads_doc, sample_updates, result = run_update(
            config=fast_cfg,
            threads_payload=tmp_threads,
            selected_thread_ids={thread_id},
            set_action=_trace_action,
            log_http=_trace_http,
            max_pages_override=3,
            enable_search_fallback=True,
            should_abort=lambda: bool(selftest_runtime.get("abort_requested")),
        )
        if result.errors:
            trace_events.append(
                {
                    "ts": utc_now(),
                    "action": "update_result",
                    "ok": False,
                    "details": "Update failed with errors",
                    "expected": "No tracker errors",
                    "observed": str(result.errors),
                    "remedy": "diagnostic + re-ensure target thread + retry",
                    "meta": {"errors": result.errors},
                }
            )
            return False, f"update errors: {result.errors}", trace_events
        if thread_id not in sample_updates:
            trace_events.append(
                {
                    "ts": utc_now(),
                    "action": "update_result",
                    "ok": False,
                    "details": "No sample update returned for self-test target",
                    "expected": "Sample payload for target thread",
                    "observed": "sample_updates missing target id",
                    "remedy": "diagnostic + verify parse + retry",
                    "meta": {"thread_id": thread_id},
                }
            )
            return False, "no sample update returned", trace_events
        # Persist only the updated target thread and its samples.
        real_threads, real_sha = github.get_file("data/threads.json")
        by_id_new = {str(t.get("id")): t for t in updated_threads_doc.get("threads", [])}
        for idx, thread in enumerate(real_threads.get("threads", [])):
            thread_id_cur = str(thread.get("id"))
            if thread_id_cur in by_id_new:
                merged = by_id_new[thread_id_cur]
                for key in [
                    "last_seen_at",
                    "last_view_count",
                    "last_found_page",
                    "last_found_above",
                    "last_seen_title",
                    "current_title",
                    "current_title_color",
                    "title_history",
                    "title_color_map",
                ]:
                    if key in merged:
                        thread[key] = merged.get(key)
                real_threads["threads"][idx] = thread
                break
        github.put_file("data/threads.json", real_threads, f"Self-test update ({reason})", real_sha)

        sample_payload, sample_sha = load_sample_payload(github, {"id": thread_id})
        sample_payload.setdefault("samples", []).extend(sample_updates[thread_id].get("samples", []))
        github.put_file(f"data/samples/{thread_id}.json", sample_payload, f"Self-test sample append ({reason})", sample_sha)
        st.session_state.setdefault("sample_cache", {})[thread_id] = _deepcopy_doc(sample_payload)
        st.session_state["threads_override"] = real_threads.get("threads", [])
        store_session_docs(threads_payload=real_threads)
        trace_events.append(
            {
                "ts": utc_now(),
                "action": "update_result",
                "ok": True,
                "details": "Isolated update persisted",
                "expected": "Thread + sample files updated",
                "observed": f"sample_count={len(sample_payload.get('samples', []))}",
                "remedy": None,
                "meta": {"thread_id": thread_id},
            }
        )
        return True, "ok", trace_events

    def process_selftest_tick() -> None:
        if selftest_runtime.get("status") != "running":
            return
        now = datetime.now(timezone.utc)
        if selftest_runtime.get("abort_requested"):
            selftest_runtime["status"] = "aborted"
            selftest_runtime["stage"] = "aborted"
            selftest_runtime["run_finished_at"] = utc_now()
            append_selftest_log(selftest_report, "abort", True, "Self-test aborted by user")
            persist_selftest_docs()
            return
        next_action = parse_iso(selftest_runtime.get("next_action_at"))
        if next_action and now < next_action:
            return

        stage = selftest_runtime.get("stage", "init")
        target_thread_id = str(selftest_runtime.get("thread_id") or selftest_thread_id(selftest_cfg.get("target", {})))
        if stage == "init":
            append_selftest_log(selftest_report, "init", True, "Purging previous self-test traces")
            purge_selftest_traces()
            target_thread_id = ensure_selftest_entry()
            selftest_runtime["thread_id"] = target_thread_id
            selftest_runtime["stage"] = "update_1"
            selftest_runtime["next_action_at"] = utc_now()
            persist_selftest_docs()
            return

        if stage.startswith("update_"):
            update_no = int(stage.split("_")[1])
            append_selftest_log(
                selftest_report,
                f"update_{update_no}",
                True,
                f"Attempting retrieval {update_no}",
                expected="Tracker should retrieve live view count for self-test target",
                observed="Update requested",
            )
            ok, info, trace_events = run_isolated_thread_update(target_thread_id, f"selftest_{update_no}")
            selftest_report.setdefault("logs", []).extend(trace_events)
            selftest_report["logs"] = selftest_report["logs"][-1200:]
            if not ok:
                if "Aborted" in info or "aborted" in info:
                    selftest_runtime["status"] = "aborted"
                    selftest_runtime["stage"] = "aborted"
                    selftest_runtime["run_finished_at"] = utc_now()
                    append_selftest_log(
                        selftest_report,
                        f"update_{update_no}",
                        False,
                        "Aborted during update step",
                        expected="Abort request should stop update quickly",
                        observed=info,
                    )
                    persist_selftest_docs()
                    return
                selftest_runtime["repair_attempts"] = int(selftest_runtime.get("repair_attempts", 0)) + 1
                append_selftest_log(
                    selftest_report,
                    f"update_{update_no}",
                    False,
                    info,
                    expected="Update should append one sample",
                    observed=info,
                    remedy="re-ensure self-test thread and retry",
                )
                if int(selftest_runtime.get("repair_attempts", 0)) <= int(selftest_cfg.get("max_repair_attempts", 2)):
                    append_selftest_log(
                        selftest_report,
                        "diagnostic",
                        True,
                        "Applying remedy: re-ensure self-test thread entry",
                        expected="Thread entry should exist and be valid",
                        observed="Remedy applied",
                    )
                    ensure_selftest_entry()
                    selftest_runtime["next_action_at"] = utc_now()
                    persist_selftest_docs()
                    return
                selftest_runtime["status"] = "failed"
                selftest_runtime["stage"] = "failed"
                selftest_runtime["last_error"] = info
                selftest_runtime["run_finished_at"] = utc_now()
                persist_selftest_docs()
                return

            try:
                samples_doc, _ = github.get_file(f"data/samples/{target_thread_id}.json")
            except Exception:  # noqa: BLE001
                samples_doc = {"samples": []}
            count = len(samples_doc.get("samples", []))
            if count < update_no:
                append_selftest_log(
                    selftest_report,
                    f"verify_{update_no}",
                    False,
                    "No samples recorded banner condition failed",
                    expected=f"At least {update_no} samples recorded for self-test thread",
                    observed=f"samples={count}",
                    remedy="run diagnostic + retry if attempts remain",
                )
                selftest_runtime["status"] = "failed"
                selftest_runtime["stage"] = "failed"
                selftest_runtime["last_error"] = "No samples recorded"
                selftest_runtime["run_finished_at"] = utc_now()
                persist_selftest_docs()
                return

            append_selftest_log(
                selftest_report,
                f"verify_{update_no}",
                True,
                f"Samples count now {count}",
                expected=f"At least {update_no} samples present and card should not show 'No samples recorded'",
                observed=f"samples={count}",
            )
            if update_no >= 3:
                selftest_runtime["status"] = "passed"
                selftest_runtime["stage"] = "complete"
                selftest_runtime["last_result"] = "3 updates complete"
                selftest_runtime["run_finished_at"] = utc_now()
                persist_selftest_docs()
                return

            selftest_runtime["stage"] = f"update_{update_no + 1}"
            selftest_runtime["next_action_at"] = (datetime.now(timezone.utc) + timedelta(seconds=int(selftest_cfg.get("delay_seconds", 4)))).isoformat()
            persist_selftest_docs()

    with side_tab_map["Display"]:
        st.subheader("Display options")
        style = st.selectbox("Trace style", ["lines", "lines+markers", "markers"], index=1, key="disp_mode")
        line_shape = st.selectbox("Line shape", ["linear", "spline"], index=0, key="disp_line_shape")
        y_scale = st.selectbox("Y scale", ["linear", "log"], index=0, key="disp_y_scale")
        line_width = st.slider("Line width", min_value=1, max_value=6, value=2, key="disp_line_width")
        marker_size = st.slider("Marker size", min_value=4, max_value=16, value=8, key="disp_marker_size")
        cards_per_row = int(
            st.number_input(
                "Thread cards per row",
                min_value=1,
                max_value=6,
                value=3,
                step=1,
                key="disp_cards_per_row",
            )
        )
        auto_fit_mobile = st.toggle(
            "Auto-fit cards per row on mobile",
            value=True,
            key="disp_auto_fit_mobile",
        )
        auto_y = st.checkbox("Auto Y", value=True, key="disp_auto_y")
        y_min = y_max = None
        if not auto_y:
            y_min = st.number_input("Y min", value=0.0, key="disp_y_min")
            y_max = st.number_input("Y max", value=1000.0, key="disp_y_max")

        chart_opts = {
            "mode": style,
            "line_shape": line_shape,
            "y_scale": y_scale,
            "line_width": line_width,
            "marker_size": marker_size,
            "y_min": y_min,
            "y_max": y_max,
            "cards_per_row": cards_per_row,
            "auto_fit_mobile": auto_fit_mobile,
        }

    with side_tab_map["Tracker"]:
        st.subheader("BladeForums View Tracker")
        st.caption(f"Tracker repo: {repo} ({branch})")
        render_status(config, runtime)
        st.divider()
        interval_seconds = int(tracker_cfg.get("interval_seconds", 1800))
        run_immediately = bool(tracker_cfg.get("start_immediately", True))
        current_running = tracker_cfg.get("state", "paused") == "running"
        desired_running = st.toggle("Tracker running", value=current_running, key="tracker_running_desired")
        if desired_running != current_running:
            if st.button("Apply tracker state", disabled=read_only):
                tracker_cfg["state"] = "running" if desired_running else "paused"
                if desired_running:
                    append_event(runtime, "info", "Tracker state changed to running")
                    runtime["next_run_at"] = next_run_timestamp(int(tracker_cfg.get("interval_seconds", 1800)))
                    if tracker_cfg.get("start_immediately", True):
                        config, threads_payload, runtime, _ = execute_update(
                            github,
                            config,
                            threads_payload,
                            runtime,
                            selected_thread_ids=None,
                            reason="toggle_running_immediate",
                        )
                        st.session_state["threads_override"] = threads_payload.get("threads", [])
                else:
                    runtime["current_action"] = "paused"
                    runtime["next_run_at"] = None
                    append_event(runtime, "info", "Tracker state changed to paused")
                put_json(github, "data/config.json", config, "Apply tracker state")
                update_runtime_file(github, runtime, "Tracker state updated")
                store_session_docs(config=config, threads_payload=threads_payload, runtime=runtime)
                st.rerun()

        st.subheader("Run controls")
        run_immediately_new = st.checkbox("Run immediately on start", value=run_immediately, disabled=read_only)
        if not read_only and run_immediately_new != run_immediately:
            tracker_cfg["start_immediately"] = run_immediately_new
            put_json(github, "data/config.json", config, "Update start behavior")
            store_session_docs(config=config)

        global_cfg = config.setdefault("global", {})
        process_logging_now = bool(global_cfg.get("enable_process_logging", False))
        process_logging_new = st.toggle("Enable process logging", value=process_logging_now, disabled=read_only)
        if not read_only and process_logging_new != process_logging_now:
            global_cfg["enable_process_logging"] = bool(process_logging_new)
            put_json(github, "data/config.json", config, "Update process logging toggle")
            store_session_docs(config=config)

        st.divider()
        interval_new = st.number_input(
            "Seconds between updates",
            min_value=5,
            max_value=3600,
            value=interval_seconds,
            step=5,
            disabled=read_only,
        )
        if not read_only and interval_new != interval_seconds:
            tracker_cfg["interval_seconds"] = int(interval_new)
            tracker_cfg["interval_minutes"] = max(1, int(interval_new) // 60)
            put_json(github, "data/config.json", config, "Update tracker interval")
            if tracker_cfg.get("state") == "running":
                runtime["next_run_at"] = next_run_timestamp(int(interval_new))
                update_runtime_file(github, runtime, "Reschedule next run")
            store_session_docs(config=config, runtime=runtime)

        max_rate = int(config.get("global", {}).get("max_requests_per_minute", 12))
        max_rate_new = st.number_input(
            "Max requests per minute",
            min_value=1,
            max_value=120,
            value=max_rate,
            step=1,
            disabled=read_only,
        )
        if not read_only and max_rate_new != max_rate:
            config.setdefault("global", {})["max_requests_per_minute"] = int(max_rate_new)
            put_json(github, "data/config.json", config, "Update rate limit")
            store_session_docs(config=config)

        min_delay = float(global_cfg.get("min_delay_seconds", 0.2))
        max_delay = float(global_cfg.get("max_delay_seconds", 1.0))
        delay_cols = st.columns(2)
        min_delay_new = float(
            delay_cols[0].number_input(
                "Min delay (s)",
                min_value=0.0,
                max_value=10.0,
                value=min_delay,
                step=0.1,
                disabled=read_only,
            )
        )
        max_delay_new = float(
            delay_cols[1].number_input(
                "Max delay (s)",
                min_value=0.0,
                max_value=15.0,
                value=max_delay,
                step=0.1,
                disabled=read_only,
            )
        )
        if max_delay_new < min_delay_new:
            st.caption("Max delay is lower than min delay; it will be clamped to min delay.")
        if not read_only and (min_delay_new != min_delay or max_delay_new != max_delay):
            global_cfg["min_delay_seconds"] = float(min_delay_new)
            global_cfg["max_delay_seconds"] = float(max(max_delay_new, min_delay_new))
            put_json(github, "data/config.json", config, "Update request delay jitter")
            store_session_docs(config=config)

        st.divider()
        st.subheader("Ad hoc update")
        selected_threads = [
            t
            for t in sorted_threads(threads_payload)
            if t.get("status", "active") == "active"
            and t.get("thread_numeric_id")
            and bool(t.get("include_in_adhoc", True))
        ]
        if selected_threads:
            for t in selected_threads:
                st.write(f"- {t.get('display_name') or t.get('current_title') or t['id']}")
        else:
            st.caption("No threads currently selected")

        if st.button(
            "Refresh selected threads",
            disabled=read_only or runtime.get("current_action", "idle").startswith("updating") or not selected_threads,
        ):
            selected_ids = {t["id"] for t in selected_threads}
            config, threads_payload, runtime, _ = execute_update(
                github,
                config,
                threads_payload,
                runtime,
                selected_thread_ids=selected_ids,
                reason="adhoc_selected",
            )
            st.session_state["threads_override"] = threads_payload.get("threads", [])
            st.rerun()

        all_active = [
            t
            for t in threads_payload.get("threads", [])
            if t.get("status", "active") == "active" and t.get("thread_numeric_id")
        ]
        if st.button(
            "Refresh all active threads",
            disabled=read_only or runtime.get("current_action", "idle").startswith("updating") or not all_active,
        ):
            config, threads_payload, runtime, _ = execute_update(
                github,
                config,
                threads_payload,
                runtime,
                selected_thread_ids=None,
                reason="adhoc_all_active",
            )
            st.session_state["threads_override"] = threads_payload.get("threads", [])
            st.rerun()

    with side_tab_map["Threads"]:
        st.subheader("Add thread")
        st.caption("Paste a thread URL or numeric ID. The numeric ID is extracted automatically.")
        with st.form("add_thread_form"):
            id_or_url = st.text_input("Thread URL or numeric ID")
            display_name = st.text_input("Display name (optional)")
            subforums = config.get("subforums", [])
            subforum_map = {x["key"]: x["name"] for x in subforums}
            subforum_key = st.selectbox("Subforum", options=list(subforum_map.keys()), format_func=lambda x: subforum_map[x])
            submitted = st.form_submit_button("Add", disabled=read_only)
            if submitted:
                numeric = parse_thread_numeric_id(id_or_url)
                if not numeric:
                    st.error("Enter a valid thread URL or numeric ID")
                else:
                    threads_doc, sha = github.get_file("data/threads.json")
                    threads = threads_doc.get("threads", [])
                    selftest_id = selftest_thread_id(selftest_cfg.get("target", {}))
                    duplicate = any(
                        t.get("subforum_key") == subforum_key
                        and str(t.get("thread_numeric_id")) == str(numeric)
                        and str(t.get("id")) != str(selftest_id)
                        and not bool(t.get("is_self_test"))
                        and not bool(t.get("is_selftest"))
                        for t in threads
                    )
                    if duplicate:
                        st.warning("Thread already exists in active tracker list. Hard-delete it below to re-add cleanly.")
                    else:
                        label = display_name.strip() if display_name.strip() else f"Thread {numeric}"
                        new_id = thread_id_for(f"{numeric}-{subforum_key}", subforum_key)
                        order_val = max([x.get("order", -1) for x in threads] + [-1]) + 1
                        threads.append(
                            {
                                "id": new_id,
                                "display_name": label,
                                "thread_numeric_id": str(numeric),
                                "subforum_key": subforum_key,
                                "status": "active",
                                "include_in_adhoc": True,
                                "order": order_val,
                                "created_at": utc_now(),
                                "title_history": [],
                                "title_color_map": {},
                            }
                        )
                        threads_doc["threads"] = threads
                        github.put_file("data/threads.json", threads_doc, "Add tracked thread", sha)
                        catalog = upsert_catalog_entries(catalog, threads_doc.get("threads", []))
                        put_json(github, "data/thread_catalog.json", catalog, "Update thread catalog")
                        store_session_docs(catalog=catalog)
                        st.session_state["threads_override"] = threads
                        pending_ids = set(st.session_state.get("pending_registration_ids", []))
                        pending_ids.add(new_id)
                        st.session_state["pending_registration_ids"] = sorted(pending_ids)
                        store_session_docs(threads_payload=threads_doc)
                        st.rerun()

        st.divider()
        st.subheader("Hard delete thread")
        thread_options_delete = sorted_threads(threads_payload)
        if not thread_options_delete:
            st.caption("No threads available")
        else:
            delete_labels = {
                f"{t.get('display_name') or t.get('id')} ({t.get('thread_numeric_id') or 'MISSING'})": t.get("id")
                for t in thread_options_delete
            }
            delete_label = st.selectbox("Thread to hard delete", options=list(delete_labels.keys()), key="hard_delete_thread_select")
            if st.button("Hard delete selected thread", disabled=read_only):
                hard_delete_thread(str(delete_labels[delete_label]))

        st.divider()
        st.subheader("Edit thread numeric ID")
        thread_options = sorted_threads(threads_payload)
        if not thread_options:
            st.caption("No threads available")
        else:
            option_labels = {
                f"{t.get('display_name') or t.get('id')} ({t.get('thread_numeric_id') or 'MISSING'})": t.get("id")
                for t in thread_options
            }
            selected_label = st.selectbox("Thread", options=list(option_labels.keys()), key="edit_thread_select")
            replacement_raw = st.text_input("Replacement URL or numeric ID", key="edit_thread_numeric_id")
            if st.button("Save replacement ID", disabled=read_only):
                numeric = parse_thread_numeric_id(replacement_raw)
                if not numeric:
                    st.error("Invalid URL/ID")
                else:
                    selected_id = option_labels[selected_label]
                    mutate_doc, sha = github.get_file("data/threads.json")
                    for thread in mutate_doc.get("threads", []):
                        if thread.get("id") == selected_id:
                            thread["thread_numeric_id"] = str(numeric)
                            break
                    github.put_file("data/threads.json", mutate_doc, "Update thread numeric id", sha)
                    catalog = upsert_catalog_entries(catalog, mutate_doc.get("threads", []))
                    put_json(github, "data/thread_catalog.json", catalog, "Update thread catalog")
                    store_session_docs(catalog=catalog)
                    st.session_state["threads_override"] = mutate_doc.get("threads", [])
                    store_session_docs(threads_payload=mutate_doc)
                    st.rerun()

        st.divider()
        st.subheader("Tracking & Ad Hoc Selection")
        thread_map = {str(t["id"]): t for t in sorted_threads(threads_payload)}
        tracker_applied = sync_tracker_rows(sorted_threads(threads_payload), st.session_state.get("tracker_applied"))
        tracker_draft = sync_tracker_rows(sorted_threads(threads_payload), st.session_state.get("tracker_draft"))
        st.session_state["tracker_applied"] = tracker_applied
        st.session_state["tracker_draft"] = tracker_draft

        draft_ids = [row["thread_id"] for row in tracker_draft]
        if sort_items and draft_ids:
            label_to_id: dict[str, str] = {}
            for item_id in draft_ids:
                if item_id not in thread_map:
                    continue
                base = thread_label(thread_map[item_id])
                label = base
                n = 2
                while label in label_to_id:
                    label = f"{base} ({n})"
                    n += 1
                label_to_id[label] = item_id
            sorted_labels = sort_items(list(label_to_id.keys()), direction="vertical", key="threads_sort_panel")
            ordered_ids = [label_to_id[x] for x in sorted_labels if x in label_to_id]
            ordered_ids.extend([x for x in draft_ids if x not in ordered_ids])
        else:
            ordered_ids = draft_ids
            if not sort_items:
                st.caption("Drag-and-drop is unavailable (sortable component missing).")

        updated_tracker_rows: list[dict[str, Any]] = []
        panel = st.container(border=True)
        with panel:
            head = st.columns([0.6, 0.6, 4.8])
            head[0].markdown("<div class='panel-head'>T</div>", unsafe_allow_html=True)
            head[1].markdown("<div class='panel-head'>A</div>", unsafe_allow_html=True)
            head[2].markdown("<div class='panel-head'>Thread</div>", unsafe_allow_html=True)
            for thread_id in ordered_ids:
                row = next((x for x in tracker_draft if x["thread_id"] == thread_id), None)
                if not row or thread_id not in thread_map:
                    continue
                cols = st.columns([0.6, 0.6, 4.8])
                track_val = cols[0].toggle(
                    "Track",
                    value=bool(row.get("track", False)),
                    key=f"tracker_track_{thread_id}",
                    help="Track",
                    label_visibility="collapsed",
                )
                adhoc_val = cols[1].toggle(
                    "Adhoc",
                    value=bool(row.get("adhoc", True)),
                    key=f"tracker_adhoc_{thread_id}",
                    help="Include in ad hoc refresh",
                    label_visibility="collapsed",
                )
                cols[2].markdown(f"<div class='panel-row-label'>{thread_label(thread_map[thread_id])}</div>", unsafe_allow_html=True)
                updated_tracker_rows.append({"thread_id": thread_id, "track": bool(track_val), "adhoc": bool(adhoc_val)})

        st.session_state["tracker_draft"] = updated_tracker_rows
        pending_registration_ids = set(st.session_state.get("pending_registration_ids", []))
        tracker_is_dirty = rows_dirty(updated_tracker_rows, tracker_applied) or bool(pending_registration_ids)
        if tracker_is_dirty:
            if st.button("Apply Thread Settings", disabled=read_only):
                by_id = {row["thread_id"]: row for row in updated_tracker_rows}
                threads_doc, sha = github.get_file("data/threads.json")
                ordered_threads: list[dict[str, Any]] = []
                for idx, thread_id in enumerate([row["thread_id"] for row in updated_tracker_rows]):
                    thread = next((t for t in threads_doc.get("threads", []) if str(t.get("id")) == str(thread_id)), None)
                    if not thread:
                        continue
                    row = by_id.get(str(thread_id), {"track": False, "adhoc": False})
                    thread["status"] = "active" if bool(row.get("track")) else "paused"
                    thread["include_in_adhoc"] = bool(row.get("adhoc"))
                    thread["order"] = idx
                    ordered_threads.append(thread)
                # Keep any missing threads at the end to avoid accidental drops.
                existing_ids = {str(t.get("id")) for t in ordered_threads}
                for tail in threads_doc.get("threads", []):
                    if str(tail.get("id")) in existing_ids:
                        continue
                    tail["order"] = len(ordered_threads)
                    ordered_threads.append(tail)
                threads_doc["threads"] = ordered_threads
                github.put_file("data/threads.json", threads_doc, "Apply thread panel settings", sha)
                catalog = upsert_catalog_entries(catalog, threads_doc.get("threads", []))
                put_json(github, "data/thread_catalog.json", catalog, "Update thread catalog")
                store_session_docs(catalog=catalog)
                st.session_state["threads_override"] = ordered_threads
                st.session_state["tracker_applied"] = _deepcopy_doc(updated_tracker_rows)
                active_pending = {x["thread_id"] for x in updated_tracker_rows if x["thread_id"] in pending_registration_ids and x.get("track")}
                pending_registration_ids = pending_registration_ids.difference(active_pending)
                st.session_state["pending_registration_ids"] = sorted(pending_registration_ids)
                store_session_docs(threads_payload=threads_doc)
                st.rerun()

        st.divider()
        st.subheader("Restore Previously Tracked")
        active_ids = {str(t.get("id")) for t in threads_payload.get("threads", [])}
        archived_entries = [t for t in catalog.get("threads", []) if str(t.get("id")) not in active_ids]
        if not archived_entries:
            st.caption("No archived threads available")
        else:
            restore_labels = {
                f"{thread_label(item)} ({item.get('thread_numeric_id') or 'no id'})": str(item.get("id"))
                for item in archived_entries
            }
            restore_picks = st.multiselect("Archived threads", options=list(restore_labels.keys()), key="restore_threads_pick")
            if restore_picks and st.button("Restore selected threads", disabled=read_only):
                threads_doc, sha = github.get_file("data/threads.json")
                existing_ids = {str(t.get("id")) for t in threads_doc.get("threads", [])}
                order_start = len(threads_doc.get("threads", []))
                appended = 0
                for pick in restore_picks:
                    thread_id = restore_labels[pick]
                    if thread_id in existing_ids:
                        continue
                    entry = next((x for x in archived_entries if str(x.get("id")) == thread_id), None)
                    if not entry:
                        continue
                    threads_doc.setdefault("threads", []).append(
                        {
                            "id": thread_id,
                            "display_name": entry.get("display_name") or f"Thread {entry.get('thread_numeric_id') or thread_id}",
                            "thread_numeric_id": entry.get("thread_numeric_id"),
                            "subforum_key": entry.get("subforum_key"),
                            "status": "paused",
                            "include_in_adhoc": False,
                            "order": order_start + appended,
                            "created_at": entry.get("created_at") or utc_now(),
                            "title_history": [],
                            "title_color_map": {},
                            "last_seen_title": entry.get("last_seen_title"),
                            "current_title": entry.get("current_title"),
                        }
                    )
                    appended += 1
                github.put_file("data/threads.json", threads_doc, "Restore archived threads", sha)
                catalog = upsert_catalog_entries(catalog, threads_doc.get("threads", []))
                put_json(github, "data/thread_catalog.json", catalog, "Update thread catalog")
                store_session_docs(catalog=catalog, threads_payload=threads_doc)
                st.session_state["threads_override"] = threads_doc.get("threads", [])
                st.rerun()
            if restore_picks and st.button("Re-add selected as fresh", disabled=read_only):
                threads_doc, sha = github.get_file("data/threads.json")
                existing_ids = {str(t.get("id")) for t in threads_doc.get("threads", [])}
                order_start = len(threads_doc.get("threads", []))
                appended = 0
                for pick in restore_picks:
                    old_id = restore_labels[pick]
                    entry = next((x for x in archived_entries if str(x.get("id")) == old_id), None)
                    if not entry:
                        continue
                    fresh_id = thread_id_for(
                        f"fresh-{entry.get('thread_numeric_id')}-{entry.get('subforum_key')}-{utc_now()}",
                        str(entry.get("subforum_key", "restored")),
                    )
                    if fresh_id in existing_ids:
                        continue
                    threads_doc.setdefault("threads", []).append(
                        {
                            "id": fresh_id,
                            "display_name": entry.get("display_name") or f"Thread {entry.get('thread_numeric_id') or fresh_id}",
                            "thread_numeric_id": entry.get("thread_numeric_id"),
                            "subforum_key": entry.get("subforum_key"),
                            "status": "paused",
                            "include_in_adhoc": False,
                            "order": order_start + appended,
                            "created_at": utc_now(),
                            "title_history": [],
                            "title_color_map": {},
                        }
                    )
                    appended += 1
                github.put_file("data/threads.json", threads_doc, "Re-add selected threads as fresh", sha)
                catalog = upsert_catalog_entries(catalog, threads_doc.get("threads", []))
                put_json(github, "data/thread_catalog.json", catalog, "Update thread catalog")
                store_session_docs(catalog=catalog, threads_payload=threads_doc)
                st.session_state["threads_override"] = threads_doc.get("threads", [])
                st.rerun()

        st.divider()
        if st.button("Save data and remove all threads", disabled=read_only or not threads_payload.get("threads")):
            snapshot_name = f"snapshot_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}.json"
            snapshot_payload = {
                "ts": utc_now(),
                "config": config,
                "runtime": runtime,
                "threads": threads_payload.get("threads", []),
                "catalog": catalog,
            }
            put_json(github, f"data/snapshots/{snapshot_name}", snapshot_payload, "Write tracker snapshot")
            threads_doc, sha = github.get_file("data/threads.json")
            for entry in catalog.get("threads", []):
                if str(entry.get("id")) in {str(t.get("id")) for t in threads_doc.get("threads", [])}:
                    entry["status"] = "paused"
                    entry["include_in_adhoc"] = False
                    entry["archived_at"] = utc_now()
            threads_doc["threads"] = []
            github.put_file("data/threads.json", threads_doc, "Remove all tracker threads", sha)
            put_json(github, "data/thread_catalog.json", catalog, "Archive all threads in catalog")
            st.session_state["threads_override"] = []
            st.session_state["layout_applied"] = []
            st.session_state["layout_draft"] = []
            st.session_state["tracker_applied"] = []
            st.session_state["tracker_draft"] = []
            store_session_docs(threads_payload=threads_doc, catalog=catalog)
            st.rerun()

    with side_tab_map["Layout"]:
        st.subheader("Thread Cards Layout")
        threads_for_layout = sorted_threads(threads_payload)
        thread_map = {str(t["id"]): t for t in threads_for_layout}
        layout_applied = sync_layout_rows(threads_for_layout, st.session_state.get("layout_applied"))
        layout_draft = sync_layout_rows(threads_for_layout, st.session_state.get("layout_draft"))
        st.session_state["layout_applied"] = layout_applied
        st.session_state["layout_draft"] = layout_draft

        draft_ids = [row["thread_id"] for row in layout_draft]
        if sort_items and draft_ids:
            label_to_id: dict[str, str] = {}
            for item_id in draft_ids:
                if item_id not in thread_map:
                    continue
                base = thread_label(thread_map[item_id])
                label = base
                n = 2
                while label in label_to_id:
                    label = f"{base} ({n})"
                    n += 1
                label_to_id[label] = item_id
            sorted_labels = sort_items(list(label_to_id.keys()), direction="vertical", key="layout_sort_panel")
            ordered_ids = [label_to_id[x] for x in sorted_labels if x in label_to_id]
            ordered_ids.extend([x for x in draft_ids if x not in ordered_ids])
        else:
            ordered_ids = draft_ids
            if not sort_items:
                st.caption("Drag-and-drop is unavailable (sortable component missing).")

        updated_layout_rows: list[dict[str, Any]] = []
        panel = st.container(border=True)
        with panel:
            head = st.columns([0.6, 0.6, 4.8])
            head[0].markdown("<div class='panel-head'>C</div>", unsafe_allow_html=True)
            head[1].markdown("<div class='panel-head'>X</div>", unsafe_allow_html=True)
            head[2].markdown("<div class='panel-head'>Thread</div>", unsafe_allow_html=True)
            for thread_id in ordered_ids:
                row = next((x for x in layout_draft if x["thread_id"] == thread_id), None)
                if not row or thread_id not in thread_map:
                    continue
                cols = st.columns([0.6, 0.6, 4.8])
                show_card = cols[0].toggle(
                    "Card",
                    value=bool(row.get("show_card", True)),
                    key=f"layout_show_{thread_id}",
                    help="Show card",
                    label_visibility="collapsed",
                )
                show_x = cols[1].toggle(
                    "X range",
                    value=bool(row.get("show_x_range", False)),
                    key=f"layout_x_{thread_id}",
                    help="Show X range controls",
                    label_visibility="collapsed",
                )
                cols[2].markdown(f"<div class='panel-row-label'>{thread_label(thread_map[thread_id])}</div>", unsafe_allow_html=True)
                updated_layout_rows.append({"thread_id": thread_id, "show_card": bool(show_card), "show_x_range": bool(show_x)})

        st.session_state["layout_draft"] = updated_layout_rows
        if rows_dirty(updated_layout_rows, layout_applied):
            if st.button("Apply Layout"):
                st.session_state["layout_applied"] = _deepcopy_doc(updated_layout_rows)
                st.rerun()

    with side_tab_map["Stats"]:
        st.subheader("Thread update counts")
        stats_rows = []
        for thread in sorted_threads(threads_payload):
            count = len(load_samples(source, thread["id"]).get("samples", []))
            stats_rows.append(
                {
                    "Thread": thread.get("display_name") or thread.get("id"),
                    "Updates": count,
                }
            )
        if stats_rows:
            st.dataframe(pd.DataFrame(stats_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("No thread data yet")

    with side_tab_map["Export"]:
        st.subheader("Export")
        export_threads = []
        for thread in sorted_threads(threads_payload):
            export_threads.append({"thread": thread, "samples": load_samples(source, thread["id"]).get("samples", [])})

        payload = {"generated_at": utc_now(), "threads": export_threads}
        st.download_button(
            "Download JSON",
            data=json.dumps(payload, indent=2),
            file_name="bladeforums_views.json",
            mime="application/json",
        )

        csv_rows = []
        for item in export_threads:
            thread = item["thread"]
            for sample in item["samples"]:
                csv_rows.append(
                    {
                        "thread_id": thread.get("id"),
                        "thread_numeric_id": thread.get("thread_numeric_id"),
                        "display_name": thread.get("display_name"),
                        "subforum": thread.get("subforum_key"),
                        "status": thread.get("status"),
                        "timestamp": sample.get("ts"),
                        "views": sample.get("views"),
                        "page": sample.get("page"),
                        "above": sample.get("above"),
                        "observed_title": sample.get("observed_title"),
                        "title_color": sample.get("title_color"),
                    }
                )
        csv_bytes = pd.DataFrame(csv_rows).to_csv(index=False).encode("utf-8") if csv_rows else b""
        st.download_button("Download CSV", data=csv_bytes, file_name="bladeforums_views.csv", mime="text/csv")

        st.divider()
        st.subheader("Full Data Archive")
        if st.button("Build archive ZIP"):
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("data/config.json", json.dumps(config, indent=2))
                zf.writestr("data/runtime.json", json.dumps(runtime, indent=2))
                zf.writestr("data/threads.json", json.dumps(threads_payload, indent=2))
                zf.writestr("data/thread_catalog.json", json.dumps(catalog, indent=2))
                zf.writestr("data/selftest_runtime.json", json.dumps(selftest_runtime, indent=2))
                zf.writestr("data/selftest_report.json", json.dumps(selftest_report, indent=2))
                zf.writestr("data/diagnostics.json", json.dumps(diagnostics, indent=2))
                thread_ids = {str(t.get("id")) for t in threads_payload.get("threads", []) if t.get("id")}
                thread_ids.update({str(t.get("id")) for t in catalog.get("threads", []) if t.get("id")})
                for thread_id in sorted(thread_ids):
                    try:
                        sample_doc = fetch_or_default(source, f"data/samples/{thread_id}.json", {"thread_id": thread_id, "samples": []})
                        zf.writestr(f"data/samples/{thread_id}.json", json.dumps(sample_doc, indent=2))
                    except Exception:  # noqa: BLE001
                        continue
                ui_errors = fetch_or_default(source, "data/ui_errors.json", {"errors": []})
                zf.writestr("data/ui_errors.json", json.dumps(ui_errors, indent=2))
            st.session_state["archive_zip_bytes"] = buf.getvalue()

        if st.session_state.get("archive_zip_bytes"):
            downloaded = st.download_button(
                "Download full archive ZIP",
                data=st.session_state["archive_zip_bytes"],
                file_name=f"bf_tracker_archive_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
                key="download_full_archive_zip",
            )
            if downloaded:
                st.session_state["archive_zip_bytes"] = b""

    if show_console_tab:
        with side_tab_map["Console"]:
            st.subheader("Diagnostics Console")
            st.caption("Allowlisted commands only. Output is capped and timed out for safety.")
            st.code(f"pwd: {os.getcwd()}", language="text")
            st.caption("Allowed examples: pwd, ls -la, find . -maxdepth 3 -type f, cat <filename>, head -n 40 <filename>")
            cmd_text = st.text_input("Command", key="console_cmd_input")
            if st.button("Run command", key="console_run_btn", disabled=read_only):
                cmd, err = resolve_console_command(cmd_text)
                if err:
                    st.session_state["console_last_output"] = f"ERROR: {err}"
                elif cmd is None:
                    st.session_state["console_last_output"] = "ERROR: Missing command"
                else:
                    started = utc_now()
                    try:
                        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
                        stdout = proc.stdout[-12000:]
                        stderr = proc.stderr[-6000:]
                        output = (
                            f"ts={started}\ncmd={' '.join(cmd)}\nexit_code={proc.returncode}\n\n"
                            f"=== STDOUT ===\n{stdout}\n\n=== STDERR ===\n{stderr}\n"
                        )
                        st.session_state["console_last_output"] = output
                        append_diagnostics_event(
                            diagnostics,
                            {
                                "ts": utc_now(),
                                "type": "console_command",
                                "ok": proc.returncode == 0,
                                "cmd": cmd,
                                "exit_code": proc.returncode,
                            },
                        )
                        persist_diagnostics_docs()
                    except Exception as exc:  # noqa: BLE001
                        st.session_state["console_last_output"] = f"ERROR: {exc}"
                        append_diagnostics_event(
                            diagnostics,
                            {
                                "ts": utc_now(),
                                "type": "console_command",
                                "ok": False,
                                "cmd": cmd,
                                "error": str(exc),
                            },
                        )
                        persist_diagnostics_docs()
            st.text_area(
                "Output",
                value=str(st.session_state.get("console_last_output", "")),
                height=280,
                disabled=True,
                key="console_output_area",
            )

            st.divider()
            st.caption("Download file from current working directory")
            file_name = st.text_input("File name in pwd", key="console_download_name")
            if file_name and os.path.basename(file_name) == file_name:
                candidate = os.path.join(os.getcwd(), file_name)
                if os.path.isfile(candidate):
                    try:
                        with open(candidate, "rb") as handle:
                            file_data = handle.read()
                        st.download_button(
                            "Download file",
                            data=file_data,
                            file_name=file_name,
                            key="console_download_btn",
                        )
                    except Exception as exc:  # noqa: BLE001
                        st.caption(f"Unable to read file: {exc}")
                else:
                    st.caption("File not found in working directory")
            elif file_name:
                st.caption("Enter only a file name (no path)")

    process_selftest_tick()

    config, threads_payload, runtime, did_run = run_local_update_if_due(github, config, threads_payload, runtime)
    if did_run:
        store_session_docs(config=config, threads_payload=threads_payload, runtime=runtime)

    main_tabs = st.tabs(["Thread Cards", "History Table", "Runtime Events", "Self-Test", "Diagnostics"])

    def mutate_threads(mutator: callable, message: str) -> None:
        threads_doc, sha = github.get_file("data/threads.json")
        mutator(threads_doc)
        threads_list = sorted(threads_doc.get("threads", []), key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))
        for i, thread in enumerate(threads_list):
            thread["order"] = i
        threads_doc["threads"] = threads_list
        github.put_file("data/threads.json", threads_doc, message, sha)
        current_catalog = load_catalog(source)
        current_catalog = upsert_catalog_entries(current_catalog, threads_list)
        put_json(github, "data/thread_catalog.json", current_catalog, "Sync thread catalog")
        store_session_docs(catalog=current_catalog)
        st.session_state["threads_override"] = threads_list
        store_session_docs(threads_payload=threads_doc)
        st.rerun()

    with main_tabs[0]:
        threads = sorted_threads(threads_payload)
        if not threads:
            st.info("No threads configured")
        else:
            subforum_name_map = {x["key"]: x["name"] for x in config.get("subforums", [])}

            layout_applied_rows = st.session_state.get("layout_applied", [])
            layout_by_id = {str(row.get("thread_id")): row for row in layout_applied_rows}

            def render_thread(thread: dict[str, Any]) -> None:
                thread_id = thread["id"]
                display_name = thread.get("display_name") or f"Thread {thread_id}"
                current_title = thread.get("current_title") or thread.get("last_seen_title") or "N/A"
                current_color = thread.get("current_title_color", "#111111")
                status = thread.get("status", "active")
                with st.container():
                    payload = load_samples(source, thread_id)
                    samples = payload.get("samples", [])
                    card_tabs = st.tabs(["Graph", "Details"])
                    with card_tabs[0]:
                        if not samples:
                            st.info("No samples recorded")
                        else:
                            df = pd.DataFrame(samples)
                            df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True).dt.tz_convert(NY_TZ)
                            df = df.dropna(subset=["ts", "views"]).copy()
                            if df.empty:
                                st.info("No valid samples")
                            else:
                                if "title_color" not in df.columns:
                                    df["title_color"] = "#1f77b4"
                                df["title_color"] = df["title_color"].fillna("#1f77b4")

                                df = df.sort_values("ts").reset_index(drop=True)
                                initial_upper = df["ts"].iloc[-1]
                                initial_lower = df["ts"].iloc[0]
                                layout_row = layout_by_id.get(str(thread_id), {"show_x_range": False})
                                show_x_range = bool(layout_row.get("show_x_range", False))
                                if show_x_range:
                                    range_cols = st.columns([1.1, 1.1, 1.2, 1.8, 1.8, 1])
                                    upper_mode = range_cols[0].selectbox(
                                        "↑",
                                        options=["Latest", "Manual"],
                                        index=0,
                                        key=f"x_upper_mode_{thread_id}",
                                        help="Upper x bound mode",
                                    )
                                    lower_mode = range_cols[1].selectbox(
                                        "↓",
                                        options=["Window", "Manual"],
                                        index=0,
                                        key=f"x_lower_mode_{thread_id}",
                                        help="Lower x bound mode",
                                    )
                                    window_points = int(
                                        range_cols[2].number_input(
                                            "N",
                                            min_value=1,
                                            max_value=5000,
                                            value=25,
                                            step=1,
                                            key=f"x_window_points_{thread_id}",
                                            help="Past points before current upper point",
                                        )
                                    )

                                    default_upper_dt = initial_upper.to_pydatetime().replace(tzinfo=None)
                                    default_lower_dt = initial_lower.to_pydatetime().replace(tzinfo=None)
                                    manual_upper_date = range_cols[3].date_input(
                                        "Upper D",
                                        value=default_upper_dt.date(),
                                        key=f"x_upper_date_{thread_id}",
                                        disabled=upper_mode != "Manual",
                                    )
                                    manual_upper_time = range_cols[3].time_input(
                                        "Upper T",
                                        value=default_upper_dt.time(),
                                        key=f"x_upper_time_{thread_id}",
                                        disabled=upper_mode != "Manual",
                                    )
                                    manual_lower_date = range_cols[4].date_input(
                                        "Lower D",
                                        value=default_lower_dt.date(),
                                        key=f"x_lower_date_{thread_id}",
                                        disabled=lower_mode != "Manual",
                                    )
                                    manual_lower_time = range_cols[4].time_input(
                                        "Lower T",
                                        value=default_lower_dt.time(),
                                        key=f"x_lower_time_{thread_id}",
                                        disabled=lower_mode != "Manual",
                                    )
                                    if range_cols[5].button("⤢", key=f"x_full_{thread_id}", help="Full range"):
                                        st.session_state[f"x_upper_mode_{thread_id}"] = "Latest"
                                        st.session_state[f"x_lower_mode_{thread_id}"] = "Window"
                                        st.session_state[f"x_window_points_{thread_id}"] = 25
                                        st.session_state[f"x_upper_date_{thread_id}"] = default_upper_dt.date()
                                        st.session_state[f"x_upper_time_{thread_id}"] = default_upper_dt.time()
                                        st.session_state[f"x_lower_date_{thread_id}"] = default_lower_dt.date()
                                        st.session_state[f"x_lower_time_{thread_id}"] = default_lower_dt.time()
                                        st.rerun()
                                else:
                                    upper_mode = "Latest"
                                    lower_mode = "Window"
                                    window_points = 25
                                    default_upper_dt = initial_upper.to_pydatetime().replace(tzinfo=None)
                                    default_lower_dt = initial_lower.to_pydatetime().replace(tzinfo=None)
                                    manual_upper_date = default_upper_dt.date()
                                    manual_upper_time = default_upper_dt.time()
                                    manual_lower_date = default_lower_dt.date()
                                    manual_lower_time = default_lower_dt.time()

                                upper_ts = initial_upper
                                if upper_mode == "Manual":
                                    upper_ts = pd.Timestamp(datetime.combine(manual_upper_date, manual_upper_time))
                                    if upper_ts.tzinfo is None:
                                        upper_ts = upper_ts.tz_localize(NY_TZ)
                                    else:
                                        upper_ts = upper_ts.tz_convert(NY_TZ)

                                df_u = df[df["ts"] <= upper_ts].copy()
                                if df_u.empty:
                                    df_u = df.iloc[[0]].copy()
                                upper_ts = df_u["ts"].iloc[-1]

                                if lower_mode == "Window":
                                    idx_upper = len(df_u) - 1
                                    idx_lower = max(0, idx_upper - window_points)
                                    lower_ts = df_u.iloc[idx_lower]["ts"]
                                else:
                                    lower_ts = pd.Timestamp(datetime.combine(manual_lower_date, manual_lower_time))
                                    if lower_ts.tzinfo is None:
                                        lower_ts = lower_ts.tz_localize(NY_TZ)
                                    else:
                                        lower_ts = lower_ts.tz_convert(NY_TZ)

                                plot_df = df[(df["ts"] >= lower_ts) & (df["ts"] <= upper_ts)].copy()
                                if plot_df.empty:
                                    plot_df = df.copy()

                                fig = go.Figure()
                                fig.add_trace(
                                    go.Scatter(
                                        x=plot_df["ts"],
                                        y=plot_df["views"],
                                        mode=chart_opts["mode"],
                                        line={"shape": chart_opts["line_shape"], "width": chart_opts["line_width"], "color": "#444444"},
                                        marker={"size": chart_opts["marker_size"], "color": plot_df["title_color"]},
                                        name="Views",
                                    )
                                )
                                fig.update_layout(
                                    height=640,
                                    margin={"l": 10, "r": 10, "t": 45, "b": 115},
                                    title={"text": current_title, "x": 0.02, "xanchor": "left"},
                                    xaxis_title="Timestamp (America/New_York)",
                                    yaxis_title="Views",
                                )
                                tick_vals, tick_text = build_axis_ticks(plot_df["ts"].iloc[0], plot_df["ts"].iloc[-1], count=7)
                                xaxis_cfg: dict[str, Any] = {
                                    "type": "date",
                                    "tickmode": "array",
                                    "tickvals": tick_vals,
                                    "ticktext": tick_text,
                                    "hoverformat": "%Y-%m-%d %H:%M:%S",
                                    "range": [plot_df["ts"].iloc[0], plot_df["ts"].iloc[-1]],
                                    "showticklabels": True,
                                    "ticks": "outside",
                                    "ticklen": 6,
                                    "tickcolor": "#666666",
                                    "tickfont": {"size": 11, "color": "#333333"},
                                    "showline": True,
                                    "linecolor": "#666666",
                                    "linewidth": 1,
                                    "automargin": True,
                                }
                                fig.update_xaxes(**xaxis_cfg)
                                fig.update_yaxes(type=chart_opts["y_scale"])
                                if chart_opts["y_min"] is not None or chart_opts["y_max"] is not None:
                                    fig.update_yaxes(range=[chart_opts["y_min"], chart_opts["y_max"]])
                                st.plotly_chart(fig, use_container_width=True)

                    with card_tabs[1]:
                        controls = st.columns([1, 1, 1, 1])
                        if controls[0].button("↻", key=f"refresh_{thread_id}", disabled=read_only or not thread.get("thread_numeric_id"), help="Refresh this thread"):
                            execute_update(
                                github,
                                config,
                                threads_payload,
                                runtime,
                                selected_thread_ids={thread_id},
                                reason=f"refresh_thread_{thread_id}",
                            )
                            st.session_state["threads_override"] = threads_payload.get("threads", [])
                            st.rerun()
                        if controls[1].button("⟲", key=f"reset_{thread_id}", disabled=read_only, help="Reset thread samples"):
                            payload, sha = load_sample_payload(github, thread)
                            payload["samples"] = []
                            github.put_file(f"data/samples/{thread_id}.json", payload, "Reset thread samples", sha)
                            st.session_state.setdefault("sample_cache", {})[thread_id] = _deepcopy_doc(payload)
                            st.rerun()
                        if controls[2].button("✕", key=f"remove_{thread_id}", disabled=read_only, help="Remove thread"):
                            mutate_threads(
                                lambda doc: doc.update({"threads": [t for t in doc.get("threads", []) if t.get("id") != thread_id]}),
                                "Remove thread",
                            )
                        if controls[3].button("DEL", key=f"hard_remove_{thread_id}", disabled=read_only, help="Hard delete thread + history"):
                            hard_delete_thread(str(thread_id))

                        st.write(
                            f"**Current title:** <span style='color:{current_color};'>{current_title}</span>",
                            unsafe_allow_html=True,
                        )
                        st.caption(f"{display_name} ({status})")
                        st.caption(subforum_name_map.get(thread.get("subforum_key"), thread.get("subforum_key", "Unknown")))
                        st.write(f"Last views: `{thread.get('last_view_count', 'N/A')}`")
                        if thread.get("last_found_page") is not None:
                            st.write(
                                f"Last location: page {thread.get('last_found_page')} | threads above: {thread.get('last_found_above', 'N/A')}"
                            )
                        st.write(f"Last seen: {to_ny_24h(thread.get('last_seen_at'))}")
                        if samples:
                            last_source = samples[-1].get("source")
                            if last_source:
                                st.caption(f"Last sample source: {last_source}")
                        render_title_legend(thread)

            threads_by_id = {str(t["id"]): t for t in threads}
            visible_rows = [row for row in layout_applied_rows if row.get("show_card", True) and str(row.get("thread_id")) in threads_by_id]
            visible_threads = [threads_by_id[str(row["thread_id"])] for row in visible_rows]
            if not visible_threads:
                st.info("No thread cards selected in Layout.")
            else:
                requested = max(1, int(chart_opts.get("cards_per_row", 3)))
                per_row = (
                    effective_cards_per_row(requested)
                    if bool(chart_opts.get("auto_fit_mobile", True))
                    else requested
                )
                for start in range(0, len(visible_threads), per_row):
                    row_items = visible_threads[start : start + per_row]
                    cols = st.columns(per_row)
                    for offset, thread in enumerate(row_items):
                        with cols[offset]:
                            render_thread(thread)

    with main_tabs[1]:
        history_df, color_lookup = build_history_table(source, sorted_threads(threads_payload))
        render_history_html(history_df, color_lookup)

    with main_tabs[2]:
        events = runtime.get("events", [])
        if events:
            recent_events = list(reversed(events))[:200]
            events_df = pd.DataFrame(recent_events)
            events_df["ts"] = pd.to_datetime(events_df["ts"], errors="coerce", utc=True).dt.tz_convert(NY_TZ)
            events_df["ts"] = events_df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(events_df[["ts", "level", "message"]], use_container_width=True, hide_index=True)
        else:
            st.caption("No runtime events yet")

    with main_tabs[3]:
        st.subheader("Runtime Self-Test")
        st.caption("Runs on fixed target: Dodo thread ID 2066634. Executes 3 retrievals with 4-second spacing.")
        st.write(
            f"Status: `{selftest_runtime.get('status', 'idle')}` | Stage: `{selftest_runtime.get('stage', 'idle')}` | "
            f"Repair attempts: `{selftest_runtime.get('repair_attempts', 0)}`"
        )
        st.write(
            f"Started: `{to_ny_24h(selftest_runtime.get('run_started_at'))}` | "
            f"Finished: `{to_ny_24h(selftest_runtime.get('run_finished_at'))}`"
        )
        cst = st.columns(3)
        if cst[0].button("Run self-test", disabled=read_only):
            selftest_report = {"schema_version": 1, "logs": []}
            selftest_runtime = {
                "status": "running",
                "stage": "init",
                "run_started_at": utc_now(),
                "run_finished_at": None,
                "abort_requested": False,
                "thread_id": None,
                "next_action_at": utc_now(),
                "repair_attempts": 0,
                "last_error": None,
                "last_result": None,
            }
            append_selftest_log(selftest_report, "start", True, "Self-test run initiated")
            persist_selftest_docs()
            st.rerun()
        if cst[1].button("Abort self-test", disabled=read_only or selftest_runtime.get("status") != "running"):
            selftest_runtime["abort_requested"] = True
            append_selftest_log(selftest_report, "abort_request", True, "Abort requested by user")
            persist_selftest_docs()
            st.rerun()
        if cst[2].button("Purge self-test traces", disabled=read_only):
            purge_selftest_traces()
            selftest_runtime = {
                "status": "idle",
                "stage": "idle",
                "run_started_at": None,
                "run_finished_at": None,
                "abort_requested": False,
                "thread_id": None,
                "next_action_at": None,
                "repair_attempts": 0,
                "last_error": None,
                "last_result": None,
            }
            selftest_report = {"schema_version": 1, "logs": []}
            persist_selftest_docs()
            st.rerun()

        st.markdown("**Self-Test Console (verbose)**")
        logs = selftest_report.get("logs", [])
        if logs:
            failure_summary = summarize_selftest_failure(logs)
            if failure_summary:
                st.error(
                    "First failure summary: "
                    f"{failure_summary.get('action')} | cause={failure_summary.get('likely_cause')} | "
                    f"observed={failure_summary.get('observed')}"
                )
                st.caption(f"Suggested remedy: {failure_summary.get('suggested_remedy')}")
            last = logs[-1]
            st.caption(
                f"Current/Latest: {last.get('action')} | expected: {last.get('expected') or 'n/a'} | observed: {last.get('observed') or last.get('details')}"
            )
            lines = [
                (
                    f"{item.get('ts')} | {'OK' if item.get('ok') else 'FAIL'} | {item.get('action')} | "
                    f"expected={item.get('expected') or 'n/a'} | observed={item.get('observed') or 'n/a'} | "
                    f"details={item.get('details')}"
                    + (f" | remedy={item.get('remedy')}" if item.get("remedy") else "")
                )
                for item in logs[-300:]
            ]
            st.code("\n".join(lines), language="text")
            st.download_button(
                "Download self-test verbose log",
                data="".join(json.dumps(item, sort_keys=True) + "\n" for item in logs),
                file_name=f"selftest_verbose_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}.jsonl",
                mime="application/json",
                key="download_selftest_verbose_log",
            )
        else:
            st.caption("No self-test logs yet")

    with main_tabs[4]:
        st.subheader("Diagnostics")
        st.caption("Safe diagnostics capture for server-side visibility (allowlisted commands only).")
        d1, d2 = st.columns([1, 2])
        if d1.button("Capture diagnostics snapshot", disabled=read_only):
            snapshot = {
                "ts": utc_now(),
                "cwd": os.getcwd(),
                "python": platform.python_version(),
                "platform": platform.platform(),
                "tree": collect_tree("/mount/src", max_depth=3, max_entries=600),
                "runtime_state": {
                    "tracker_state": config.get("tracker", {}).get("state"),
                    "current_action": runtime.get("current_action"),
                    "selftest_status": selftest_runtime.get("status"),
                    "selftest_stage": selftest_runtime.get("stage"),
                },
            }
            snap_name = f"data/diagnostics/snapshot_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}.json"
            put_json(github, snap_name, snapshot, "Add diagnostics snapshot")
            append_diagnostics_event(
                diagnostics,
                {
                    "ts": utc_now(),
                    "type": "snapshot",
                    "ok": True,
                    "path": snap_name,
                    "details": "Diagnostics snapshot captured",
                },
            )
            persist_diagnostics_docs()
            st.success(f"Captured snapshot: {snap_name}")

        cmd_key = d2.selectbox("Allowlisted command", options=list(DIAG_COMMANDS.keys()), key="diag_cmd_key")
        if st.button("Run diagnostics command", disabled=read_only):
            cmd = DIAG_COMMANDS[cmd_key]
            started = utc_now()
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
                payload = (
                    f"ts={started}\ncmd={' '.join(cmd)}\nexit_code={proc.returncode}\n\n"
                    f"=== STDOUT ===\n{proc.stdout}\n\n=== STDERR ===\n{proc.stderr}\n"
                )
                tmp_path = f"/tmp/bf_diag_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}_{cmd_key}.log"
                with open(tmp_path, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                repo_path = f"data/diagnostics/command_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}_{cmd_key}.log"
                put_text(github, repo_path, payload, "Add diagnostics command output")
                append_diagnostics_event(
                    diagnostics,
                    {
                        "ts": utc_now(),
                        "type": "command",
                        "ok": proc.returncode == 0,
                        "path": repo_path,
                        "tmp_path": tmp_path,
                        "cmd": cmd,
                        "exit_code": proc.returncode,
                    },
                )
                persist_diagnostics_docs()
                st.success(f"Saved command output to {repo_path}")
            except Exception as exc:  # noqa: BLE001
                append_diagnostics_event(
                    diagnostics,
                    {
                        "ts": utc_now(),
                        "type": "command",
                        "ok": False,
                        "cmd": cmd,
                        "error": str(exc),
                    },
                )
                persist_diagnostics_docs()
                st.error(f"Command failed: {exc}")

        st.markdown("**Recent Diagnostics Events**")
        events = diagnostics.get("events", [])
        if events:
            recent_diag = list(reversed(events))[:100]
            st.dataframe(pd.DataFrame(recent_diag), use_container_width=True, hide_index=True)
            st.download_button(
                "Download diagnostics events",
                data=json.dumps(diagnostics, indent=2),
                file_name=f"diagnostics_events_{datetime.now(NY_TZ).strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                key="download_diagnostics_events",
            )
        else:
            st.caption("No diagnostics events yet")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log_error: str | None = None
        try:
            _, github, _, _ = build_clients()
        except Exception as inner:  # noqa: BLE001
            github = None
            log_error = str(inner)
        if github:
            try:
                payload, _ = github.get_file("data/ui_errors.json")
            except Exception:  # noqa: BLE001
                payload = {"errors": []}
            payload.setdefault("errors", []).append({"ts": utc_now(), "error": str(exc), "traceback": traceback.format_exc()})
            try:
                put_json(github, "data/ui_errors.json", payload, "Log UI error")
            except Exception as inner:  # noqa: BLE001
                log_error = str(inner)
        st.error("Unexpected error")
        if log_error:
            st.caption(f"UI error log persistence failed: {log_error}")
        st.exception(exc)
