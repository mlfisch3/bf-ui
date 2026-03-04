from __future__ import annotations

import json
import os
import re
import traceback
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

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
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
.history-table th, .history-table td {
  border: 1px solid #d7d7d7;
  padding: 4px;
  text-align: right;
  font-size: 0.72rem;
}
.history-table th {
  white-space: normal;
  word-break: break-word;
  line-height: 0.88rem;
  vertical-align: bottom;
}
.history-table td.ts-col {
  text-align: left;
  white-space: nowrap;
}
</style>
""",
    unsafe_allow_html=True,
)

NY_TZ = ZoneInfo("America/New_York")
THREAD_ID_INPUT_RE = re.compile(r"(?:^|\.)(\d+)(?:/)?$")
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


def parse_thread_numeric_id(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        return text
    match = THREAD_ID_INPUT_RE.search(text)
    return match.group(1) if match else None


def put_json(github: GithubClient, path: str, payload: dict[str, Any], message: str) -> None:
    try:
        current, sha = github.get_file(path)
    except Exception:  # noqa: BLE001
        current, sha = None, None
    if current == payload:
        return
    github.put_file(path, payload, message, sha)


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
    threads_doc, sha = github.get_file("data/threads.json")
    threads_doc["threads"] = threads_payload.get("threads", [])
    github.put_file("data/threads.json", threads_doc, message, sha)


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
    threads_by_id = {t["id"]: t for t in threads_payload.get("threads", [])}

    for thread_id, update_payload in sample_updates.items():
        thread = threads_by_id.get(thread_id)
        if not thread:
            continue
        payload, sha = load_sample_payload(github, thread)
        payload.setdefault("samples", []).extend(update_payload.get("samples", []))
        payload["thread_numeric_id"] = thread.get("thread_numeric_id")
        payload["title"] = thread.get("current_title") or thread.get("display_name")
        github.put_file(f"data/samples/{thread_id}.json", payload, f"Append samples {thread_id}", sha)

    runtime["last_run_summary"] = summary
    update_runtime_file(github, runtime, "Update runtime after tracker run")


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

    def set_action(text: str) -> None:
        state = config.get("tracker", {}).get("state", "stopped")
        runtime["current_action"] = text if state != "paused" else f"{text} (paused)"
        update_runtime_file(github, runtime, "Update runtime action")

    config, threads_payload, sample_updates, result = run_update(
        config=config,
        threads_payload=threads_payload,
        selected_thread_ids=selected_thread_ids,
        set_action=set_action,
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
    runtime["next_run_at"] = next_run_timestamp(int(config.get("tracker", {}).get("interval_minutes", 30)))

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
    return config, threads_payload, runtime, summary


def render_status(config: dict[str, Any], runtime: dict[str, Any]) -> None:
    state = config.get("tracker", {}).get("state", "stopped")
    action = runtime.get("current_action", "idle")
    if state == "paused" and action == "idle":
        action = "paused"
    elif state == "paused" and "(paused)" not in action:
        action = f"{action} (paused)"

    cols = st.columns([1.2, 1, 1, 1])
    with cols[0]:
        if state == "running":
            st.success("State: Running")
        elif state == "paused":
            st.warning("State: Paused")
        else:
            st.info("State: Stopped")
    with cols[1]:
        st.metric("Current action", action)
    with cols[2]:
        st.metric("Last run", to_ny_24h(runtime.get("last_run_finished_at")))
    with cols[3]:
        next_run = runtime.get("next_run_at") if state == "running" else None
        st.metric("Next run", to_ny_24h(next_run))


def load_samples(source: DataSource, thread_id: str) -> dict[str, Any]:
    return fetch_or_default(source, f"data/samples/{thread_id}.json", {"thread_id": thread_id, "samples": []})


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
        col_name = abbreviate_label(thread.get("display_name") or thread.get("current_title") or thread_id)
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
    st.markdown("".join(html), unsafe_allow_html=True)


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


def sorted_threads(threads_payload: dict[str, Any]) -> list[dict[str, Any]]:
    threads = threads_payload.get("threads", [])
    return sorted(threads, key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))


def move_thread_order(threads: list[dict[str, Any]], thread_id: str, delta: int) -> list[dict[str, Any]]:
    ordered = sorted(threads, key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))
    idx = next((i for i, t in enumerate(ordered) if t.get("id") == thread_id), None)
    if idx is None:
        return ordered
    new_idx = idx + delta
    if new_idx < 0 or new_idx >= len(ordered):
        return ordered
    ordered[idx], ordered[new_idx] = ordered[new_idx], ordered[idx]
    for i, thread in enumerate(ordered):
        thread["order"] = i
    return ordered


def run_local_update_if_due(
    github: GithubClient | None,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    runtime: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bool]:
    if not github:
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

    config, threads_payload, runtime, _ = execute_update(
        github,
        config,
        threads_payload,
        runtime,
        selected_thread_ids=None,
        reason="interval",
    )
    return config, threads_payload, runtime, True


def main() -> None:
    source, github, repo, branch = build_clients()
    read_only = github is None

    st.title("BladeForums View Tracker")
    st.caption(f"Tracker repo: {repo} ({branch})")

    try:
        config = fetch_or_default(source, "data/config.json", {"schema_version": 1, "tracker": {}, "global": {}, "subforums": []})
        threads_payload = fetch_or_default(source, "data/threads.json", {"schema_version": 1, "threads": []})
        runtime = load_runtime(source)
    except Exception as exc:  # noqa: BLE001
        if github:
            try:
                payload, sha = github.get_file("data/ui_errors.json")
            except Exception:  # noqa: BLE001
                payload, sha = {"errors": []}, None
            payload.setdefault("errors", []).append({"ts": utc_now(), "error": str(exc), "traceback": traceback.format_exc()})
            github.put_file("data/ui_errors.json", payload, "Log UI error", sha)
        st.error("Unexpected error")
        st.exception(exc)
        st.stop()

    tracker_cfg = config.setdefault("tracker", {})
    tracker_cfg.setdefault("state", "stopped")
    tracker_cfg.setdefault("interval_minutes", 30)
    tracker_cfg.setdefault("start_immediately", True)

    defaults_changed = normalize_threads_defaults(threads_payload)
    if defaults_changed and github and not read_only:
        persist_threads_doc(github, threads_payload, "Normalize thread defaults")

    state = tracker_cfg.get("state", "stopped")
    if state == "running":
        st_autorefresh(interval=15000, key="tracker_refresh")

    render_status(config, runtime)

    st.sidebar.header("Controls")
    side_tabs = st.sidebar.tabs(["Tracker", "Threads", "Subforums", "Display", "Export"])

    with side_tabs[3]:
        st.subheader("Display options")
        style = st.selectbox("Trace style", ["lines", "lines+markers", "markers"], index=1, key="disp_mode")
        line_shape = st.selectbox("Line shape", ["linear", "spline"], index=0, key="disp_line_shape")
        y_scale = st.selectbox("Y scale", ["linear", "log"], index=0, key="disp_y_scale")
        line_width = st.slider("Line width", min_value=1, max_value=6, value=2, key="disp_line_width")
        marker_size = st.slider("Marker size", min_value=4, max_value=16, value=8, key="disp_marker_size")
        graph_only = st.toggle("Graph-only thread cards", value=False, key="disp_graph_only")
        cards_per_row = 3
        if graph_only:
            cards_per_row = int(
                st.number_input(
                    "Graph-only cards per row",
                    min_value=1,
                    max_value=6,
                    value=3,
                    step=1,
                    key="disp_cards_per_row",
                )
            )
        expanded_default = st.toggle("Expand thread cards by default", value=True, key="disp_expand_cards")
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
            "graph_only": graph_only,
            "cards_per_row": cards_per_row,
            "expanded_default": expanded_default,
        }

    with side_tabs[0]:
        interval = int(tracker_cfg.get("interval_minutes", 30))
        run_immediately = bool(tracker_cfg.get("start_immediately", True))

        st.subheader("Run controls")
        run_immediately_new = st.checkbox("Run immediately on start", value=run_immediately, disabled=read_only)
        if not read_only and run_immediately_new != run_immediately:
            tracker_cfg["start_immediately"] = run_immediately_new
            put_json(github, "data/config.json", config, "Update start behavior")
            st.rerun()

        c1, c2, c3, c4 = st.columns(4)
        if c1.button("Start", disabled=read_only or state == "running"):
            tracker_cfg["state"] = "running"
            append_event(runtime, "info", "Tracker state changed to running")
            if tracker_cfg.get("start_immediately", True):
                config, threads_payload, runtime, _ = execute_update(
                    github,
                    config,
                    threads_payload,
                    runtime,
                    selected_thread_ids=None,
                    reason="start_immediate",
                )
            else:
                runtime["current_action"] = "idle"
                runtime["next_run_at"] = next_run_timestamp(int(tracker_cfg.get("interval_minutes", 30)))
            put_json(github, "data/config.json", config, "Set tracker state running")
            update_runtime_file(github, runtime, "Tracker running")
            st.rerun()

        if c2.button("Pause", disabled=read_only or state != "running"):
            tracker_cfg["state"] = "paused"
            runtime["current_action"] = "paused"
            append_event(runtime, "info", "Tracker state changed to paused")
            put_json(github, "data/config.json", config, "Set tracker state paused")
            update_runtime_file(github, runtime, "Tracker paused")
            st.rerun()

        if c3.button("Resume", disabled=read_only or state != "paused"):
            tracker_cfg["state"] = "running"
            runtime["current_action"] = "idle"
            runtime["next_run_at"] = next_run_timestamp(int(tracker_cfg.get("interval_minutes", 30)))
            append_event(runtime, "info", "Tracker state changed to running")
            put_json(github, "data/config.json", config, "Set tracker state running")
            update_runtime_file(github, runtime, "Tracker resumed")
            st.rerun()

        if c4.button("Stop", disabled=read_only or state == "stopped"):
            tracker_cfg["state"] = "stopped"
            runtime["current_action"] = "idle"
            runtime["next_run_at"] = None
            append_event(runtime, "info", "Tracker state changed to stopped")
            put_json(github, "data/config.json", config, "Set tracker state stopped")
            update_runtime_file(github, runtime, "Tracker stopped")
            st.rerun()

        st.divider()
        interval_new = st.number_input(
            "Minutes between updates",
            min_value=5,
            max_value=240,
            value=interval,
            step=5,
            disabled=read_only,
        )
        if not read_only and interval_new != interval:
            tracker_cfg["interval_minutes"] = int(interval_new)
            put_json(github, "data/config.json", config, "Update tracker interval")
            if tracker_cfg.get("state") == "running":
                runtime["next_run_at"] = next_run_timestamp(int(interval_new))
                update_runtime_file(github, runtime, "Reschedule next run")
            st.rerun()

        max_rate = int(config.get("global", {}).get("max_requests_per_minute", 12))
        max_rate_new = st.number_input(
            "Max requests per minute",
            min_value=1,
            max_value=19,
            value=max_rate,
            step=1,
            disabled=read_only,
        )
        if not read_only and max_rate_new != max_rate:
            config.setdefault("global", {})["max_requests_per_minute"] = int(max_rate_new)
            put_json(github, "data/config.json", config, "Update rate limit")
            st.rerun()

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
            st.rerun()

    with side_tabs[1]:
        st.subheader("Add thread")
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
                    duplicate = any(
                        t.get("subforum_key") == subforum_key and str(t.get("thread_numeric_id")) == str(numeric)
                        for t in threads
                    )
                    if duplicate:
                        st.warning("Thread already exists")
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
                        st.rerun()

    with side_tabs[2]:
        st.subheader("Subforum retrieval limits")
        rows = [{"Subforum": s["name"], "Max pages": int(s.get("max_pages_per_update", 3))} for s in config.get("subforums", [])]
        edited = st.data_editor(
            pd.DataFrame(rows),
            hide_index=True,
            disabled=read_only,
            num_rows="fixed",
            column_config={
                "Subforum": st.column_config.TextColumn("Subforum", disabled=True),
                "Max pages": st.column_config.NumberColumn("Max pages", min_value=1, max_value=10),
            },
        )
        if st.button("Save limits", disabled=read_only):
            for idx, row in edited.iterrows():
                config["subforums"][idx]["max_pages_per_update"] = int(row["Max pages"])
            put_json(github, "data/config.json", config, "Update subforum limits")
            st.rerun()

    with side_tabs[4]:
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

    config, threads_payload, runtime, did_run = run_local_update_if_due(github, config, threads_payload, runtime)
    if did_run:
        st.rerun()

    main_tabs = st.tabs(["Thread Cards", "History Table", "Runtime Events"])

    def mutate_threads(mutator: callable, message: str) -> None:
        threads_doc, sha = github.get_file("data/threads.json")
        mutator(threads_doc)
        threads_list = sorted(threads_doc.get("threads", []), key=lambda t: (t.get("order", 10_000), t.get("created_at", ""), t.get("id", "")))
        for i, thread in enumerate(threads_list):
            thread["order"] = i
        threads_doc["threads"] = threads_list
        github.put_file("data/threads.json", threads_doc, message, sha)
        st.rerun()

    with main_tabs[0]:
        threads = sorted_threads(threads_payload)
        if not threads:
            st.info("No threads configured")
        else:
            subforum_name_map = {x["key"]: x["name"] for x in config.get("subforums", [])}

            def render_thread(idx: int, thread: dict[str, Any]) -> None:
                thread_id = thread["id"]
                display_name = thread.get("display_name") or f"Thread {thread_id}"
                current_title = thread.get("current_title") or thread.get("last_seen_title") or "N/A"
                current_color = thread.get("current_title_color", "#111111")
                status = thread.get("status", "active")

                with st.expander(f"{display_name} ({status})", expanded=bool(chart_opts["expanded_default"])):
                    top_controls = st.columns([1.2, 1, 1, 1, 1, 1])
                    if top_controls[0].button("Up", key=f"move_up_{thread_id}", disabled=read_only or idx == 0):
                        mutate_threads(
                            lambda doc: doc.update({"threads": move_thread_order(doc.get("threads", []), thread_id, -1)}),
                            "Move thread up",
                        )
                    if top_controls[1].button("Down", key=f"move_down_{thread_id}", disabled=read_only or idx == len(threads) - 1):
                        mutate_threads(
                            lambda doc: doc.update({"threads": move_thread_order(doc.get("threads", []), thread_id, 1)}),
                            "Move thread down",
                        )
                    track_now = status == "active"
                    track_new = top_controls[2].toggle("track", value=track_now, key=f"track_{thread_id}", disabled=read_only)
                    if track_new != track_now and not read_only:
                        mutate_threads(
                            lambda doc: [
                                t.update({"status": "active" if track_new else "paused"})
                                for t in doc.get("threads", [])
                                if t.get("id") == thread_id
                            ],
                            "Toggle thread tracking",
                        )
                    if top_controls[3].button("Refresh", key=f"refresh_{thread_id}", disabled=read_only or not thread.get("thread_numeric_id")):
                        execute_update(
                            github,
                            config,
                            threads_payload,
                            runtime,
                            selected_thread_ids={thread_id},
                            reason=f"refresh_thread_{thread_id}",
                        )
                        st.rerun()
                    if top_controls[4].button("Reset", key=f"reset_{thread_id}", disabled=read_only):
                        payload, sha = load_sample_payload(github, thread)
                        payload["samples"] = []
                        github.put_file(f"data/samples/{thread_id}.json", payload, "Reset thread samples", sha)
                        st.rerun()
                    if top_controls[5].button("Remove", key=f"remove_{thread_id}", disabled=read_only):
                        mutate_threads(
                            lambda doc: doc.update({"threads": [t for t in doc.get("threads", []) if t.get("id") != thread_id]}),
                            "Remove thread",
                        )

                    include_now = bool(thread.get("include_in_adhoc", True))
                    include_new = st.toggle(
                        "Include in selected ad hoc refresh",
                        value=include_now,
                        key=f"adhoc_{thread_id}",
                        disabled=read_only,
                    )
                    if include_new != include_now and not read_only:
                        mutate_threads(
                            lambda doc: [
                                t.update({"include_in_adhoc": bool(include_new)})
                                for t in doc.get("threads", [])
                                if t.get("id") == thread_id
                            ],
                            "Toggle ad hoc inclusion",
                        )

                    current_id = str(thread.get("thread_numeric_id") or "")
                    new_id = st.text_input("Thread URL or numeric ID", value=current_id, key=f"thread_id_edit_{thread_id}")
                    if st.button("Save thread ID", key=f"save_thread_id_{thread_id}", disabled=read_only):
                        numeric = parse_thread_numeric_id(new_id)
                        if not numeric:
                            st.error("Invalid URL/ID")
                        else:
                            mutate_threads(
                                lambda doc: [
                                    t.update({"thread_numeric_id": str(numeric)})
                                    for t in doc.get("threads", [])
                                    if t.get("id") == thread_id
                                ],
                                "Update thread numeric id",
                            )

                    if not chart_opts["graph_only"]:
                        st.write(
                            f"**Current title:** <span style='color:{current_color};'>{current_title}</span>",
                            unsafe_allow_html=True,
                        )
                        st.caption(subforum_name_map.get(thread.get("subforum_key"), thread.get("subforum_key", "Unknown")))
                        st.write(f"Last views: `{thread.get('last_view_count', 'N/A')}`")
                        if thread.get("last_found_page") is not None:
                            st.write(
                                f"Last location: page {thread.get('last_found_page')} | threads above: {thread.get('last_found_above', 'N/A')}"
                            )
                        st.write(f"Last seen: {to_ny_24h(thread.get('last_seen_at'))}")
                        render_title_legend(thread)

                    payload = load_samples(source, thread_id)
                    samples = payload.get("samples", [])
                    if not samples:
                        st.info("No samples recorded")
                        return

                    df = pd.DataFrame(samples)
                    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True).dt.tz_convert(NY_TZ)
                    df = df.dropna(subset=["ts", "views"]).copy()
                    if df.empty:
                        st.info("No valid samples")
                        return

                    if "title_color" not in df.columns:
                        df["title_color"] = "#1f77b4"
                    df["title_color"] = df["title_color"].fillna("#1f77b4")

                    fig = go.Figure()
                    fig.add_trace(
                        go.Scatter(
                            x=df["ts"],
                            y=df["views"],
                            mode=chart_opts["mode"],
                            line={"shape": chart_opts["line_shape"], "width": chart_opts["line_width"], "color": "#444444"},
                            marker={"size": chart_opts["marker_size"], "color": df["title_color"]},
                            name="Views",
                        )
                    )
                    dtick_ms = choose_dtick_ms(df["ts"])
                    fig.update_layout(
                        height=640,
                        margin={"l": 10, "r": 10, "t": 45, "b": 10},
                        title={"text": current_title, "x": 0.02, "xanchor": "left"},
                        xaxis_title="Timestamp (America/New_York)",
                        yaxis_title="Views",
                    )
                    xaxis_cfg: dict[str, Any] = {
                        "tickformat": "%H:%M\n%Y-%m-%d",
                        "hoverformat": "%Y-%m-%d %H:%M:%S",
                    }
                    if dtick_ms is not None:
                        xaxis_cfg["dtick"] = dtick_ms
                    fig.update_xaxes(**xaxis_cfg)
                    fig.update_yaxes(type=chart_opts["y_scale"])
                    if chart_opts["y_min"] is not None or chart_opts["y_max"] is not None:
                        fig.update_yaxes(range=[chart_opts["y_min"], chart_opts["y_max"]])
                    st.plotly_chart(fig, use_container_width=True)

            if chart_opts["graph_only"]:
                per_row = max(1, int(chart_opts.get("cards_per_row", 3)))
                for start in range(0, len(threads), per_row):
                    row_items = threads[start : start + per_row]
                    cols = st.columns(per_row)
                    for offset, thread in enumerate(row_items):
                        with cols[offset]:
                            render_thread(start + offset, thread)
            else:
                for idx, thread in enumerate(threads):
                    render_thread(idx, thread)

    with main_tabs[1]:
        history_df, color_lookup = build_history_table(source, sorted_threads(threads_payload))
        render_history_html(history_df, color_lookup)

    with main_tabs[2]:
        with st.expander("Runtime Events", expanded=False):
            events = runtime.get("events", [])
            if events:
                events_df = pd.DataFrame(events[-80:])
                events_df["ts"] = pd.to_datetime(events_df["ts"], errors="coerce", utc=True).dt.tz_convert(NY_TZ)
                events_df["ts"] = events_df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
                st.dataframe(events_df[["ts", "level", "message"]], use_container_width=True, hide_index=True)
            else:
                st.caption("No runtime events yet")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        _, github, _, _ = build_clients()
        if github:
            try:
                payload, sha = github.get_file("data/ui_errors.json")
            except Exception:  # noqa: BLE001
                payload, sha = {"errors": []}, None
            payload.setdefault("errors", []).append({"ts": utc_now(), "error": str(exc), "traceback": traceback.format_exc()})
            github.put_file("data/ui_errors.json", payload, "Log UI error", sha)
        st.error("Unexpected error")
        st.exception(exc)
