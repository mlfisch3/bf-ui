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
.small-header {
  font-size: 0.74rem;
  line-height: 0.95rem;
  white-space: normal;
  word-break: break-word;
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


def to_ny_24h(value: str | None) -> str:
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(NY_TZ).strftime("%Y-%m-%d %H:%M:%S")


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
    if match:
        return match.group(1)
    return None


def log_ui_error(github: GithubClient | None, exc: Exception) -> None:
    st.error("Unexpected error")
    st.exception(exc)
    if not github:
        return
    try:
        payload, sha = github.get_file("data/ui_errors.json")
    except Exception:  # noqa: BLE001
        payload, sha = {"errors": []}, None
    payload.setdefault("errors", []).append(
        {
            "ts": utc_now(),
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
    )
    github.put_file("data/ui_errors.json", payload, "Log UI error", sha)


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
    runtime.setdefault("events", []).append(
        {
            "ts": utc_now(),
            "level": level,
            "message": message,
        }
    )
    runtime["events"] = runtime["events"][-200:]


def update_runtime_file(github: GithubClient, runtime: dict[str, Any], message: str) -> None:
    put_json(github, "data/runtime.json", runtime, message)


def set_tracker_state(
    github: GithubClient,
    config: dict[str, Any],
    runtime: dict[str, Any],
    new_state: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    tracker = config.setdefault("tracker", {})
    tracker["state"] = new_state
    if new_state != "running":
        runtime["next_run_at"] = None
        runtime["current_action"] = "idle"
    append_event(runtime, "info", f"Tracker state changed to {new_state}")
    put_json(github, "data/config.json", config, f"Set tracker state {new_state}")
    update_runtime_file(github, runtime, f"Tracker {new_state}")
    return config, runtime


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
    title = title.strip() if title else "(Unknown Title)"
    color_map = thread.setdefault("title_color_map", {})
    title_order = thread.setdefault("title_history", [])
    if title not in color_map:
        color_map[title] = TITLE_COLORS[len(title_order) % len(TITLE_COLORS)]
        title_order.append(title)
    return color_map[title]


def persist_update_results(
    github: GithubClient,
    threads_payload: dict[str, Any],
    sample_updates: dict[str, dict[str, Any]],
    result_summary: dict[str, Any],
    runtime: dict[str, Any],
) -> None:
    threads_current, threads_sha = github.get_file("data/threads.json")
    threads_current["threads"] = threads_payload.get("threads", [])
    github.put_file("data/threads.json", threads_current, "Update thread stats", threads_sha)

    by_thread = {t["id"]: t for t in threads_payload.get("threads", [])}
    for thread_id, new_payload in sample_updates.items():
        thread = by_thread.get(thread_id)
        if not thread:
            continue
        payload, sha = load_sample_payload(github, thread)
        payload.setdefault("samples", []).extend(new_payload.get("samples", []))
        payload["thread_numeric_id"] = thread.get("thread_numeric_id")
        payload["title"] = thread.get("current_title") or thread.get("display_name")
        github.put_file(
            f"data/samples/{thread_id}.json",
            payload,
            f"Append samples for {thread_id}",
            sha,
        )

    runtime["last_run_summary"] = result_summary
    update_runtime_file(github, runtime, "Update runtime after tracker run")


def execute_update(
    github: GithubClient,
    config: dict[str, Any],
    threads_payload: dict[str, Any],
    runtime: dict[str, Any],
    selected_thread_ids: set[str] | None,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    runtime["current_action"] = "updating"
    runtime["last_run_started_at"] = utc_now()
    runtime["last_run_result"] = "running"
    append_event(runtime, "info", f"Run started ({reason})")
    update_runtime_file(github, runtime, "Tracker run started")

    def set_action(text: str) -> None:
        runtime["current_action"] = text
        update_runtime_file(github, runtime, "Update runtime action")

    config, threads_payload, sample_updates, result = run_update(
        config=config,
        threads_payload=threads_payload,
        selected_thread_ids=selected_thread_ids,
        set_action=set_action,
    )

    by_id = {t["id"]: t for t in threads_payload.get("threads", [])}
    for thread_id, samples_payload in sample_updates.items():
        thread = by_id.get(thread_id)
        if not thread:
            continue
        observed_title = thread.get("last_seen_title") or thread.get("current_title") or thread.get("display_name") or "(Unknown Title)"
        color = ensure_title_color(thread, observed_title)
        thread["current_title"] = observed_title
        thread["current_title_color"] = color
        for sample in samples_payload.get("samples", []):
            sample["observed_title"] = sample.get("observed_title") or observed_title
            sample["title_color"] = ensure_title_color(thread, sample["observed_title"])

    runtime["current_action"] = "idle"
    runtime["last_run_finished_at"] = result.finished_at
    runtime["last_run_result"] = "ok" if not result.errors else "warning"
    runtime["next_run_at"] = next_run_timestamp(int(config.get("tracker", {}).get("interval_minutes", 30)))

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


def has_trackable_active_threads(threads_payload: dict[str, Any]) -> bool:
    for thread in threads_payload.get("threads", []):
        if thread.get("status", "active") == "active" and thread.get("thread_numeric_id"):
            return True
    return False


def render_status(config: dict[str, Any], runtime: dict[str, Any], trackable: bool) -> None:
    state = config.get("tracker", {}).get("state", "stopped")
    cols = st.columns([1.2, 1, 1, 1])
    with cols[0]:
        if state == "running":
            st.success("State: Running")
        elif state == "paused":
            st.warning("State: Paused")
        else:
            st.info("State: Stopped")
    with cols[1]:
        st.metric("Current action", runtime.get("current_action", "idle"))
    with cols[2]:
        st.metric("Last run", to_ny_24h(runtime.get("last_run_finished_at")))
    with cols[3]:
        nxt = runtime.get("next_run_at") if state == "running" else None
        st.metric("Next run", to_ny_24h(nxt))

    if state == "running" and not trackable:
        st.warning("Tracker is running, but no active threads have a thread numeric ID.")


def load_samples(source: DataSource, thread_id: str) -> dict[str, Any]:
    return fetch_or_default(source, f"data/samples/{thread_id}.json", {"thread_id": thread_id, "samples": []})


def abbreviate_label(label: str, width: int = 12) -> str:
    text = " ".join(str(label).split()).strip()
    if len(text) <= width:
        return text
    return text[: width - 1] + "…"


def build_history_table(
    source: DataSource,
    threads: list[dict[str, Any]],
) -> tuple[pd.DataFrame, dict[tuple[str, str], str]]:
    data_rows: list[dict[str, Any]] = []
    color_lookup: dict[tuple[str, str], str] = {}
    for thread in threads:
        if not thread.get("id"):
            continue
        col = abbreviate_label(thread.get("display_name") or thread.get("current_title") or thread["id"])
        payload = load_samples(source, thread["id"])
        for sample in payload.get("samples", []):
            ts_label = to_ny_24h(sample.get("ts"))
            value = sample.get("views")
            if value is None:
                continue
            data_rows.append({"ts": ts_label, "thread_col": col, "value": int(value)})
            if sample.get("title_color"):
                color_lookup[(ts_label, col)] = str(sample["title_color"])

    if not data_rows:
        return pd.DataFrame(), color_lookup

    df_long = pd.DataFrame(data_rows)
    pivot = df_long.pivot_table(index="ts", columns="thread_col", values="value", aggfunc="last")
    pivot = pivot.sort_index(ascending=False)
    return pivot, color_lookup


def style_history(df: pd.DataFrame, color_lookup: dict[tuple[str, str], str]) -> Any:
    def style_cell(v: Any, ts: str, col: str) -> str:
        if pd.isna(v):
            return ""
        color = color_lookup.get((ts, col), "#111111")
        return f"color: {color}; font-weight: 600;"

    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for ts in df.index:
        for col in df.columns:
            styles.loc[ts, col] = style_cell(df.loc[ts, col], ts, col)
    return df.style.apply(lambda _: styles, axis=None)


def render_title_legend(thread: dict[str, Any]) -> None:
    titles = thread.get("title_history", [])
    colors = thread.get("title_color_map", {})
    if not titles:
        return
    st.markdown("**Observed titles (in order):**")
    for idx, t in enumerate(titles, start=1):
        color = colors.get(t, "#111111")
        st.markdown(f"<span style='color:{color};'>{idx}. {t}</span>", unsafe_allow_html=True)


def render_thread_card(
    source: DataSource,
    thread: dict[str, Any],
    subforum_name: str,
    read_only: bool,
    chart_opts: dict[str, Any],
    on_pause_resume: callable,
    on_reset: callable,
    on_remove: callable,
    on_set_numeric_id: callable,
    on_toggle_adhoc: callable,
    on_refresh_one: callable,
) -> None:
    tid = thread["id"]
    status = thread.get("status", "active")
    display_name = thread.get("display_name") or f"Thread {tid}"
    current_title = thread.get("current_title") or thread.get("last_seen_title") or thread.get("display_name") or "N/A"
    current_title_color = thread.get("current_title_color", "#111111")

    with st.expander(f"{display_name} ({status})", expanded=False):
        left, right = st.columns([2, 3])
        with left:
            st.write(f"**Current title:** <span style='color:{current_title_color};'>{current_title}</span>", unsafe_allow_html=True)
            st.caption(subforum_name)
            st.write(f"Thread numeric ID: `{thread.get('thread_numeric_id') or 'MISSING'}`")
            st.write(f"Last views: `{thread.get('last_view_count', 'N/A')}`")
            if thread.get("last_found_page") is not None:
                st.write(
                    f"Last location: page {thread.get('last_found_page')} | threads above: {thread.get('last_found_above', 'N/A')}"
                )
            st.write(f"Last seen: {to_ny_24h(thread.get('last_seen_at'))}")

            include_adhoc = bool(thread.get("include_in_adhoc", True))
            include_adhoc_new = st.toggle(
                "Include in selected ad hoc refresh",
                value=include_adhoc,
                key=f"adhoc_{tid}",
                disabled=read_only,
            )
            if include_adhoc_new != include_adhoc and not read_only:
                on_toggle_adhoc(tid, include_adhoc_new)

            c1, c2, c3, c4 = st.columns(4)
            if status == "active":
                if c1.button("Pause", key=f"pause_{tid}", disabled=read_only):
                    on_pause_resume(tid, "paused")
            else:
                if c1.button("Resume", key=f"resume_{tid}", disabled=read_only):
                    on_pause_resume(tid, "active")
            if c2.button("Refresh", key=f"refresh_{tid}", disabled=read_only or not thread.get("thread_numeric_id")):
                on_refresh_one(tid)
            if c3.button("Reset", key=f"reset_{tid}", disabled=read_only):
                on_reset(thread)
            if c4.button("Remove", key=f"remove_{tid}", disabled=read_only):
                on_remove(tid)

            current_id = str(thread.get("thread_numeric_id") or "")
            new_id = st.text_input("Edit thread URL or numeric ID", value=current_id, key=f"set_id_{tid}")
            if st.button("Save thread ID", key=f"save_id_{tid}", disabled=read_only):
                numeric = parse_thread_numeric_id(new_id)
                if not numeric:
                    st.error("Invalid URL/ID")
                else:
                    on_set_numeric_id(tid, numeric)

            render_title_legend(thread)

        with right:
            payload = load_samples(source, tid)
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
                    customdata=df[["title_color"]],
                )
            )
            fig.update_layout(
                height=520,
                margin={"l": 10, "r": 10, "t": 35, "b": 10},
                xaxis_title="Timestamp (America/New_York)",
                yaxis_title="Views",
            )
            fig.update_xaxes(tickformat="%Y-%m-%d %H:%M", hoverformat="%Y-%m-%d %H:%M:%S")
            fig.update_yaxes(type=chart_opts["y_scale"])
            if chart_opts["y_min"] is not None or chart_opts["y_max"] is not None:
                fig.update_yaxes(range=[chart_opts["y_min"], chart_opts["y_max"]])
            st.plotly_chart(fig, use_container_width=True)


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
        log_ui_error(github, exc)
        st.stop()

    tracker_cfg = config.setdefault("tracker", {})
    tracker_cfg.setdefault("state", "stopped")
    tracker_cfg.setdefault("interval_minutes", 30)
    tracker_cfg.setdefault("start_immediately", True)

    for thread in threads_payload.get("threads", []):
        thread.setdefault("include_in_adhoc", True)

    state = tracker_cfg.get("state", "stopped")
    if state == "running":
        st_autorefresh(interval=15000, key="tracker_refresh")

    trackable = has_trackable_active_threads(threads_payload)
    render_status(config, runtime, trackable)

    st.sidebar.header("Controls")
    tabs = st.sidebar.tabs(["Tracker", "Threads", "Subforums", "Display", "Export"])

    with tabs[0]:
        interval = int(tracker_cfg.get("interval_minutes", 30))
        run_immediately = bool(tracker_cfg.get("start_immediately", True))

        st.subheader("Run Controls")
        run_immediately_new = st.checkbox(
            "Run immediately on start",
            value=run_immediately,
            disabled=read_only,
            key="run_immediately",
        )
        if not read_only and run_immediately_new != run_immediately:
            tracker_cfg["start_immediately"] = run_immediately_new
            put_json(github, "data/config.json", config, "Update start behavior")
            st.rerun()

        c1, c2, c3, c4 = st.columns(4)
        if c1.button("Start", disabled=read_only or state == "running", key="btn_start"):
            config, runtime = set_tracker_state(github, config, runtime, "running")
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
                runtime["next_run_at"] = next_run_timestamp(int(tracker_cfg.get("interval_minutes", 30)))
                update_runtime_file(github, runtime, "Schedule next run")
            st.rerun()

        if c2.button("Pause", disabled=read_only or state != "running", key="btn_pause"):
            config, runtime = set_tracker_state(github, config, runtime, "paused")
            st.rerun()

        if c3.button("Resume", disabled=read_only or state != "paused", key="btn_resume"):
            config, runtime = set_tracker_state(github, config, runtime, "running")
            runtime["next_run_at"] = next_run_timestamp(int(tracker_cfg.get("interval_minutes", 30)))
            update_runtime_file(github, runtime, "Resume tracker")
            st.rerun()

        if c4.button("Stop", disabled=read_only or state == "stopped", key="btn_stop"):
            config, runtime = set_tracker_state(github, config, runtime, "stopped")
            st.rerun()

        st.divider()
        interval_new = st.number_input(
            "Minutes between updates",
            min_value=5,
            max_value=240,
            value=interval,
            step=5,
            disabled=read_only,
            key="interval_minutes",
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
            key="max_rpm",
        )
        if not read_only and max_rate_new != max_rate:
            config.setdefault("global", {})["max_requests_per_minute"] = int(max_rate_new)
            put_json(github, "data/config.json", config, "Update rate limit")
            st.rerun()

        st.divider()
        st.subheader("Ad hoc update")
        selected_threads = [
            t for t in threads_payload.get("threads", [])
            if t.get("status", "active") == "active"
            and t.get("thread_numeric_id")
            and bool(t.get("include_in_adhoc", True))
        ]
        st.write("Selected threads:")
        if selected_threads:
            for t in selected_threads:
                st.write(f"- {t.get('display_name') or t.get('current_title') or t['id']}")
        else:
            st.caption("No threads currently selected")

        if st.button(
            "Refresh selected threads",
            disabled=read_only or runtime.get("current_action") == "updating" or not selected_threads,
            key="refresh_selected_threads",
        ):
            selected_ids = {t["id"] for t in selected_threads}
            config, threads_payload, runtime, summary = execute_update(
                github,
                config,
                threads_payload,
                runtime,
                selected_thread_ids=selected_ids,
                reason="adhoc_selected",
            )
            if summary.get("errors"):
                st.warning(f"Update finished with {len(summary['errors'])} errors")
            else:
                st.success("Update finished")
            st.rerun()

        all_active_trackable = [
            t for t in threads_payload.get("threads", [])
            if t.get("status", "active") == "active" and t.get("thread_numeric_id")
        ]
        if st.button(
            "Refresh all active threads",
            disabled=read_only or runtime.get("current_action") == "updating" or not all_active_trackable,
            key="refresh_all_active",
        ):
            config, threads_payload, runtime, summary = execute_update(
                github,
                config,
                threads_payload,
                runtime,
                selected_thread_ids=None,
                reason="adhoc_all_active",
            )
            if summary.get("errors"):
                st.warning(f"Update finished with {len(summary['errors'])} errors")
            else:
                st.success("Update finished")
            st.rerun()

    with tabs[1]:
        st.subheader("Add Thread")
        with st.form("add_thread_form"):
            id_or_url = st.text_input("Thread URL or numeric ID")
            display_name = st.text_input("Display name (optional)")
            subforums = config.get("subforums", [])
            subforum_map = {x["key"]: x["name"] for x in subforums}
            subforum_key = st.selectbox(
                "Subforum",
                options=list(subforum_map.keys()),
                format_func=lambda x: subforum_map[x],
            )
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
                        tid = thread_id_for(f"{numeric}-{subforum_key}", subforum_key)
                        threads.append(
                            {
                                "id": tid,
                                "display_name": label,
                                "thread_numeric_id": str(numeric),
                                "subforum_key": subforum_key,
                                "status": "active",
                                "include_in_adhoc": True,
                                "created_at": utc_now(),
                            }
                        )
                        threads_doc["threads"] = threads
                        github.put_file("data/threads.json", threads_doc, "Add tracked thread", sha)
                        st.success("Thread added")
                        st.rerun()

    with tabs[2]:
        st.subheader("Subforum retrieval limits")
        subforum_rows = [
            {"Subforum": s["name"], "Max pages": int(s.get("max_pages_per_update", 3))}
            for s in config.get("subforums", [])
        ]
        table = pd.DataFrame(subforum_rows)
        edited = st.data_editor(
            table,
            hide_index=True,
            disabled=read_only,
            num_rows="fixed",
            column_config={
                "Subforum": st.column_config.TextColumn("Subforum", disabled=True),
                "Max pages": st.column_config.NumberColumn("Max pages", min_value=1, max_value=10),
            },
        )
        if st.button("Save limits", disabled=read_only, key="save_limits"):
            for idx, row in edited.iterrows():
                config["subforums"][idx]["max_pages_per_update"] = int(row["Max pages"])
            put_json(github, "data/config.json", config, "Update subforum limits")
            st.success("Saved")
            st.rerun()

    with tabs[3]:
        st.subheader("Display options")
        style = st.selectbox("Trace style", ["lines", "lines+markers", "markers"], index=1, key="disp_mode")
        line_shape = st.selectbox("Line shape", ["linear", "spline"], index=0, key="disp_line_shape")
        y_scale = st.selectbox("Y scale", ["linear", "log"], index=0, key="disp_y_scale")
        line_width = st.slider("Line width", min_value=1, max_value=6, value=2, key="disp_line_width")
        marker_size = st.slider("Marker size", min_value=4, max_value=16, value=8, key="disp_marker_size")
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
        }

    with tabs[4]:
        st.subheader("Export")
        export_threads = []
        for thread in threads_payload.get("threads", []):
            export_threads.append(
                {
                    "thread": thread,
                    "samples": load_samples(source, thread["id"]).get("samples", []),
                }
            )
        export_payload = {"generated_at": utc_now(), "threads": export_threads}
        st.download_button(
            "Download JSON",
            data=json.dumps(export_payload, indent=2),
            file_name="bladeforums_views.json",
            mime="application/json",
            key="download_json",
        )

        rows = []
        for item in export_threads:
            thread = item["thread"]
            for sample in item["samples"]:
                rows.append(
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
        csv_bytes = pd.DataFrame(rows).to_csv(index=False).encode("utf-8") if rows else b""
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="bladeforums_views.csv",
            mime="text/csv",
            key="download_csv",
        )

    # Apply interval run only after controls are handled so Pause acts immediately.
    if (
        github
        and tracker_cfg.get("state") == "running"
        and trackable
        and runtime.get("current_action") != "updating"
        and due_for_run(runtime.get("next_run_at"))
    ):
        config, threads_payload, runtime, _ = execute_update(
            github,
            config,
            threads_payload,
            runtime,
            selected_thread_ids=None,
            reason="interval",
        )
        st.rerun()

    with st.expander("Runtime Events", expanded=False):
        events = runtime.get("events", [])
        if events:
            df = pd.DataFrame(events[-50:])
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True).dt.tz_convert(NY_TZ)
            df["ts"] = df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(df[["ts", "level", "message"]], use_container_width=True, hide_index=True)
        else:
            st.caption("No runtime events yet")

    main_tabs = st.tabs(["Thread Cards", "History Table"])

    def update_threads_doc(mutator: callable, commit_message: str) -> None:
        threads_doc, sha = github.get_file("data/threads.json")
        mutator(threads_doc)
        github.put_file("data/threads.json", threads_doc, commit_message, sha)
        st.rerun()

    with main_tabs[0]:
        st.subheader("Tracked Threads")
        subforum_name_map = {x["key"]: x["name"] for x in config.get("subforums", [])}

        threads = threads_payload.get("threads", [])
        if not threads:
            st.info("No threads configured")
        else:
            for thread in threads:
                render_thread_card(
                    source=source,
                    thread=thread,
                    subforum_name=subforum_name_map.get(thread.get("subforum_key"), thread.get("subforum_key", "Unknown")),
                    read_only=read_only,
                    chart_opts=chart_opts,
                    on_pause_resume=lambda tid, status: update_threads_doc(
                        lambda doc: [t.update({"status": status}) for t in doc.get("threads", []) if t.get("id") == tid],
                        "Update thread status",
                    ),
                    on_reset=lambda t: (
                        github.put_file(
                            f"data/samples/{t['id']}.json",
                            {
                                "thread_id": t["id"],
                                "thread_numeric_id": t.get("thread_numeric_id"),
                                "title": t.get("display_name"),
                                "samples": [],
                            },
                            "Reset thread samples",
                            load_sample_payload(github, t)[1],
                        ),
                        st.rerun(),
                    ),
                    on_remove=lambda tid: update_threads_doc(
                        lambda doc: doc.update({"threads": [x for x in doc.get("threads", []) if x.get("id") != tid]}),
                        "Remove thread",
                    ),
                    on_set_numeric_id=lambda tid, nid: update_threads_doc(
                        lambda doc: [x.update({"thread_numeric_id": str(nid)}) for x in doc.get("threads", []) if x.get("id") == tid],
                        "Set thread numeric id",
                    ),
                    on_toggle_adhoc=lambda tid, val: update_threads_doc(
                        lambda doc: [x.update({"include_in_adhoc": bool(val)}) for x in doc.get("threads", []) if x.get("id") == tid],
                        "Toggle ad hoc inclusion",
                    ),
                    on_refresh_one=lambda tid: (
                        execute_update(
                            github,
                            config,
                            threads_payload,
                            runtime,
                            selected_thread_ids={tid},
                            reason=f"refresh_thread_{tid}",
                        ),
                        st.rerun(),
                    ),
                )

    with main_tabs[1]:
        st.subheader("History Table")
        threads = threads_payload.get("threads", [])
        history_df, color_lookup = build_history_table(source, threads)
        if history_df.empty:
            st.info("No samples available")
        else:
            styled = style_history(history_df, color_lookup)
            st.dataframe(styled, use_container_width=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        _, github, _, _ = build_clients()
        log_ui_error(github, exc)
