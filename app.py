from __future__ import annotations

import json
import os
import traceback
import uuid
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from ui.data_client import DataSource, fetch_json
from ui.github_client import GithubClient, GithubConfig
from ui.models import thread_id_for, utc_now


st.set_page_config(page_title="BladeForums View Tracker", layout="wide", initial_sidebar_state="collapsed")


def widget_key(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def handle_error(exc: Exception, github: GithubClient | None) -> None:
    st.error("Unexpected error. Details are shown below.")
    st.exception(exc)
    if not github:
        return
    try:
        log_ui_error(github, exc)
    except Exception:  # noqa: BLE001
        return


def log_ui_error(github: GithubClient, exc: Exception) -> None:
    payload = {
        "ts": utc_now(),
        "error": str(exc),
        "traceback": traceback.format_exc(),
    }
    try:
        existing, sha = github.get_file("data/ui_errors.json")
    except Exception:  # noqa: BLE001
        existing, sha = {"errors": []}, None
    existing.setdefault("errors", [])
    existing["errors"].append(payload)
    github.put_file("data/ui_errors.json", existing, "Log UI error", sha)


def get_setting(key: str, default: str | None = None) -> str | None:
    if key in st.secrets:
        return st.secrets.get(key)
    return os.getenv(key, default)


def build_clients() -> tuple[DataSource, GithubClient | None, str | None]:
    repo = get_setting("TRACKER_REPO")
    branch = get_setting("TRACKER_BRANCH", "main")
    token = get_setting("GITHUB_TOKEN")
    if not repo:
        st.error("TRACKER_REPO is not configured.")
        st.stop()
    raw_base = f"https://raw.githubusercontent.com/{repo}/{branch}"
    data_source = DataSource(raw_base=raw_base)
    github = None
    if token:
        github = GithubClient(GithubConfig(repo=repo, branch=branch, token=token))
    return data_source, github, repo


@st.cache_data(ttl=60)
def load_config(source: DataSource) -> dict[str, Any]:
    return fetch_json(source, "data/config.json")


@st.cache_data(ttl=60)
def load_threads(source: DataSource) -> dict[str, Any]:
    return fetch_json(source, "data/threads.json")


@st.cache_data(ttl=60)
def load_samples(source: DataSource, thread_id: str) -> dict[str, Any]:
    return fetch_json(source, f"data/samples/{thread_id}.json")


@st.cache_data(ttl=60)
def load_last_run(source: DataSource) -> dict[str, Any]:
    try:
        return fetch_json(source, "data/last_run.json")
    except Exception:  # noqa: BLE001
        return {}


def refresh_cache() -> None:
    st.cache_data.clear()


def main() -> None:
    st.title("BladeForums View Tracker")
    source, github, repo = build_clients()
    read_only = github is None

    try:
        config = load_config(source)
        threads_payload = load_threads(source)
        last_run = load_last_run(source)
        subforums = {item["key"]: item for item in config.get("subforums", [])}
        config.setdefault("tracker", {})
        tracker_cfg = config["tracker"]
        tracker_cfg.setdefault("state", "stopped")
        tracker_cfg.setdefault("interval_minutes", 30)
        tracker_cfg.setdefault("start_immediately", True)
        tracker_cfg.setdefault("run_on_next", False)
        tracker_cfg.setdefault("force_run", False)
        tracker_cfg.setdefault("force_thread_ids", [])
        tracker_cfg.setdefault("kill_switch", False)
        tracker_state = tracker_cfg.get("state", "stopped")
        kill_switch = bool(tracker_cfg.get("kill_switch"))

        st.sidebar.header("Controls")
        st.sidebar.caption(f"Tracker repo: {repo}")
        tabs = st.sidebar.tabs(["Tracker", "Threads", "Subforums", "Display", "Export"])

        with tabs[0]:
            st.subheader("Tracker Status")
            state_label = tracker_state.capitalize()
            if kill_switch:
                st.error("Kill switch is ON. Tracker will not run.")
            elif tracker_state == "running":
                st.success(f"Tracker: {state_label}")
            elif tracker_state == "paused":
                st.warning(f"Tracker: {state_label}")
            else:
                st.info(f"Tracker: {state_label}")

            if tracker_cfg.get("force_run"):
                st.warning("Ad hoc update queued.")
            elif tracker_cfg.get("run_on_next"):
                st.info("Tracker will run on the next workflow tick.")

            next_run = last_run.get("next_run_at")
            if next_run:
                st.caption(f"Next scheduled run: {next_run}")
            else:
                st.caption("Next scheduled run: not scheduled")

            st.write("Start/Stop/Pause")
            start_immediately = st.checkbox(
                "Run immediately on start",
                value=bool(tracker_cfg.get("start_immediately", True)),
                disabled=read_only,
            )
            if not read_only and start_immediately != tracker_cfg.get("start_immediately", True):
                tracker_cfg["start_immediately"] = bool(start_immediately)
                update_json_file(github, "data/config.json", config, "Update start preference")
                refresh_cache()
                st.success("Start preference updated")
            cols = st.columns(3)
            if cols[0].button("Start", disabled=read_only):
                run_now = bool(tracker_cfg.get("start_immediately", True))
                set_tracker_state(github, config, "running", run_on_next=run_now)
                refresh_cache()
                st.success("Tracker started")
            if cols[1].button("Pause", disabled=read_only):
                set_tracker_state(github, config, "paused")
                refresh_cache()
                st.warning("Tracker paused")
            if cols[2].button("Stop", disabled=read_only):
                set_tracker_state(github, config, "stopped")
                refresh_cache()
                st.info("Tracker stopped")

            st.divider()
            st.subheader("Intervals")
            interval = int(tracker_cfg.get("interval_minutes", 30))
            new_interval = st.number_input(
                "Minutes between updates",
                min_value=5,
                max_value=240,
                value=interval,
                step=5,
                disabled=read_only,
            )
            if not read_only and new_interval != interval:
                if st.button("Update interval", key="update_interval"):
                    tracker_cfg["interval_minutes"] = int(new_interval)
                    update_json_file(github, "data/config.json", config, "Update interval")
                    refresh_cache()
                    st.success("Interval updated")

            max_rate = config.get("global", {}).get("max_requests_per_minute", 12)
            new_rate = st.number_input(
                "Max requests per minute",
                min_value=1,
                max_value=19,
                value=int(max_rate),
                step=1,
                disabled=read_only,
            )
            if not read_only and new_rate != max_rate:
                if st.button("Update rate limit", key="update_rate"):
                    config["global"]["max_requests_per_minute"] = int(new_rate)
                    update_json_file(github, "data/config.json", config, "Update rate limit")
                    refresh_cache()
                    st.success("Updated rate limit")

            st.divider()
            st.subheader("Ad Hoc Update")
            thread_options = {t["title"]: t for t in threads_payload.get("threads", [])}
            selected_titles = st.multiselect(
                "Threads to update now (empty = all)",
                options=list(thread_options.keys()),
                disabled=read_only,
            )
            if st.button("Run update now", disabled=read_only):
                force_ids = [thread_options[t]["id"] for t in selected_titles] if selected_titles else []
                trigger_ad_hoc_update(github, config, force_ids)
                refresh_cache()
                st.success("Update triggered")

            st.divider()
            st.subheader("Kill Switch")
            kill = st.checkbox("Enable kill switch", value=kill_switch, disabled=read_only)
            if not read_only and kill != kill_switch:
                tracker_cfg["kill_switch"] = bool(kill)
                update_json_file(github, "data/config.json", config, "Update kill switch")
                refresh_cache()
                st.warning("Kill switch updated")

            st.divider()
            st.subheader("Reset Tracker (Destructive)")
            confirm = st.text_input("Type RESET to enable", value="", disabled=read_only)
            if st.button("Reset all samples", disabled=read_only or confirm != "RESET"):
                reset_all_samples(github, threads_payload)
                refresh_cache()
                st.success("All samples reset")

        with tabs[1]:
            st.subheader("Add Thread")
            with st.form("add_thread"):
                title = st.text_input("Thread title (exact match)")
                subforum_key = st.selectbox(
                    "Subforum",
                    options=[key for key in subforums.keys()],
                    format_func=lambda key: subforums[key]["name"],
                )
                submitted = st.form_submit_button("Add thread", disabled=read_only)
                if submitted:
                    if not title.strip():
                        st.error("Title is required")
                    else:
                        add_thread(github, title.strip(), subforum_key)
                        refresh_cache()
                        st.success("Thread added")

        with tabs[2]:
            st.subheader("Subforum Page Limits")
            data = [
                {"Subforum": item["name"], "Max pages": int(item.get("max_pages_per_update", 3))}
                for item in config.get("subforums", [])
            ]
            df = pd.DataFrame(data)
            edited = st.data_editor(
                df,
                hide_index=True,
                num_rows="fixed",
                disabled=read_only,
                column_config={
                    "Subforum": st.column_config.TextColumn("Subforum", disabled=True),
                    "Max pages": st.column_config.NumberColumn("Max pages", min_value=1, max_value=10),
                },
            )
            if st.button("Save subforum limits", disabled=read_only):
                for idx, row in edited.iterrows():
                    config["subforums"][idx]["max_pages_per_update"] = int(row["Max pages"])
                update_json_file(github, "data/config.json", config, "Update subforum settings")
                refresh_cache()
                st.success("Updated subforum limits")

        with tabs[3]:
            st.subheader("Display Options")
            chart_mode = st.selectbox(
                "Chart style",
                ["Lines", "Lines + markers", "Markers"],
                index=0,
            )
            line_shape = st.selectbox("Line shape", ["linear", "spline"], index=0)
            y_scale = st.selectbox("Y-axis scale", ["linear", "log"], index=0)
            auto_y = st.checkbox("Auto-scale Y axis", value=True)
            if not auto_y:
                y_min = st.number_input("Y min", value=0)
                y_max = st.number_input("Y max", value=0)
            else:
                y_min = None
                y_max = None
            if y_scale == "log" and y_min is not None and y_min <= 0:
                st.warning("Log scale requires Y min > 0.")

        with tabs[4]:
            st.subheader("Export")
            export_payload = build_export(source, threads_payload.get("threads", []))
            st.download_button(
                "Download JSON",
                data=json.dumps(export_payload, indent=2),
                file_name="bladeforums_views.json",
                mime="application/json",
                key=widget_key("download_json"),
            )
            csv_bytes = build_csv(export_payload)
            st.download_button(
                "Download CSV",
                data=csv_bytes,
                file_name="bladeforums_views.csv",
                mime="text/csv",
                key=widget_key("download_csv"),
            )

        st.header("Tracked Threads")
        if last_run:
            note = last_run.get("note")
            if note:
                st.caption(f"Last run: {last_run.get('finished_at', 'unknown')} — {note}")
            else:
                st.caption(f"Last run: {last_run.get('finished_at', 'unknown')}")
        threads = threads_payload.get("threads", [])
        if not threads:
            st.info("No threads are being tracked yet.")
            return

        for thread in threads:
            thread_id = thread.get("id") or thread_id_for(thread["title"], thread["subforum_key"])
            status = thread.get("status", "active")
            last_view = thread.get("last_view_count")
            subforum_name = subforums.get(thread["subforum_key"], {}).get("name", thread["subforum_key"])
            last_page = thread.get("last_found_page")
            last_above = thread.get("last_found_above")

            with st.container(border=True):
                cols = st.columns([2, 3])
                with cols[0]:
                    st.subheader(thread["title"])
                    st.caption(f"{subforum_name} | Status: {status}")
                    st.write(f"Last view count: {last_view if last_view is not None else 'N/A'}")
                    if last_page is not None:
                        st.write(
                            f"Last found on page {last_page} with {last_above if last_above is not None else 'N/A'} threads above"
                        )
                    controls = st.columns(3)
                    if status == "active":
                        if controls[0].button("Pause", key=f"pause_{thread_id}", disabled=read_only):
                            update_thread_status(github, thread_id, "paused")
                            refresh_cache()
                            st.success("Paused")
                    else:
                        if controls[0].button("Resume", key=f"resume_{thread_id}", disabled=read_only):
                            update_thread_status(github, thread_id, "active")
                            refresh_cache()
                            st.success("Resumed")

                    if controls[1].button("Reset", key=f"reset_{thread_id}", disabled=read_only):
                        reset_thread_samples(github, thread_id, thread["title"])
                        refresh_cache()
                        st.success("Reset samples")

                    if controls[2].button("Remove", key=f"remove_{thread_id}", disabled=read_only):
                        remove_thread(github, thread_id)
                        refresh_cache()
                        st.success("Removed")

                with cols[1]:
                    samples = load_samples(source, thread_id).get("samples", [])
                    if samples:
                        df = pd.DataFrame(samples)
                        df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
                        df["ts"] = df["ts"].dt.tz_convert("US/Eastern")
                        if chart_mode == "Markers":
                            fig = px.scatter(df, x="ts", y="views", title="Views over time", height=220)
                        else:
                            fig = px.line(
                                df,
                                x="ts",
                                y="views",
                                title="Views over time",
                                height=220,
                                markers=chart_mode == "Lines + markers",
                                line_shape=line_shape,
                            )
                        fig.update_yaxes(type=y_scale)
                        if y_min is not None or y_max is not None:
                            fig.update_yaxes(range=[y_min, y_max])
                        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No samples recorded yet.")
    except Exception as exc:  # noqa: BLE001
        handle_error(exc, github)
        st.stop()

    st.header("Export")
    export_payload = build_export(source, threads)
    st.download_button(
        "Download JSON",
        data=json.dumps(export_payload, indent=2),
        file_name="bladeforums_views.json",
        mime="application/json",
    )
    csv_bytes = build_csv(export_payload)
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name="bladeforums_views.csv",
        mime="text/csv",
    )


def update_json_file(github: GithubClient, path: str, payload: dict[str, Any], message: str) -> None:
    existing, sha = github.get_file(path)
    if existing == payload:
        return
    github.put_file(path, payload, message, sha)


def set_tracker_state(
    github: GithubClient, config: dict[str, Any], state: str, run_on_next: bool = False
) -> None:
    config.setdefault("tracker", {})
    config["tracker"]["state"] = state
    config["tracker"]["run_on_next"] = bool(run_on_next)
    update_json_file(github, "data/config.json", config, "Update tracker state")


def trigger_ad_hoc_update(
    github: GithubClient, config: dict[str, Any], force_thread_ids: list[str]
) -> None:
    config.setdefault("tracker", {})
    config["tracker"]["force_run"] = True
    config["tracker"]["force_thread_ids"] = force_thread_ids
    update_json_file(github, "data/config.json", config, "Trigger ad hoc update")
    github.dispatch_workflow("track.yml", "main")


def reset_all_samples(github: GithubClient, threads_payload: dict[str, Any]) -> None:
    for thread in threads_payload.get("threads", []):
        thread_id = thread.get("id") or thread_id_for(thread["title"], thread["subforum_key"])
        samples_payload = {"thread_id": thread_id, "title": thread["title"], "samples": []}
        try:
            _, sha = github.get_file(f"data/samples/{thread_id}.json")
        except Exception:  # noqa: BLE001
            sha = None
        github.put_file(
            f"data/samples/{thread_id}.json",
            samples_payload,
            "Reset all samples",
            sha,
        )


def add_thread(github: GithubClient, title: str, subforum_key: str) -> None:
    threads_payload, sha = github.get_file("data/threads.json")
    threads = threads_payload.get("threads", [])
    for thread in threads:
        if thread["title"] == title and thread["subforum_key"] == subforum_key:
            return
    thread_id = thread_id_for(title, subforum_key)
    threads.append(
        {
            "id": thread_id,
            "title": title,
            "subforum_key": subforum_key,
            "status": "active",
            "created_at": utc_now(),
        }
    )
    threads_payload["threads"] = threads
    github.put_file("data/threads.json", threads_payload, "Add tracked thread", sha)

    samples_payload = {"thread_id": thread_id, "title": title, "samples": []}
    try:
        _, sample_sha = github.get_file(f"data/samples/{thread_id}.json")
    except Exception:  # noqa: BLE001
        sample_sha = None
    github.put_file(
        f"data/samples/{thread_id}.json",
        samples_payload,
        "Initialize samples",
        sample_sha,
    )


def update_thread_status(github: GithubClient, thread_id: str, status: str) -> None:
    threads_payload, sha = github.get_file("data/threads.json")
    for thread in threads_payload.get("threads", []):
        if thread.get("id") == thread_id:
            thread["status"] = status
            break
    github.put_file("data/threads.json", threads_payload, "Update thread status", sha)


def reset_thread_samples(github: GithubClient, thread_id: str, title: str) -> None:
    samples_payload = {"thread_id": thread_id, "title": title, "samples": []}
    try:
        _, sha = github.get_file(f"data/samples/{thread_id}.json")
    except Exception:  # noqa: BLE001
        sha = None
    github.put_file(
        f"data/samples/{thread_id}.json",
        samples_payload,
        "Reset samples",
        sha,
    )


def remove_thread(github: GithubClient, thread_id: str) -> None:
    threads_payload, sha = github.get_file("data/threads.json")
    threads = [t for t in threads_payload.get("threads", []) if t.get("id") != thread_id]
    threads_payload["threads"] = threads
    github.put_file("data/threads.json", threads_payload, "Remove tracked thread", sha)


def build_export(source: DataSource, threads: list[dict[str, Any]]) -> dict[str, Any]:
    export_threads = []
    for thread in threads:
        thread_id = thread.get("id") or thread_id_for(thread["title"], thread["subforum_key"])
        samples = load_samples(source, thread_id).get("samples", [])
        export_threads.append({"thread": thread, "samples": samples})
    return {"generated_at": utc_now(), "threads": export_threads}


def build_csv(payload: dict[str, Any]) -> bytes:
    rows = []
    for entry in payload.get("threads", []):
        thread = entry["thread"]
        for sample in entry.get("samples", []):
            rows.append(
                {
                    "thread_id": thread.get("id"),
                    "title": thread.get("title"),
                    "subforum_key": thread.get("subforum_key"),
                    "status": thread.get("status"),
                    "timestamp": sample.get("ts"),
                    "views": sample.get("views"),
                }
            )
    if not rows:
        return b""
    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8")


if __name__ == "__main__":
    main()
