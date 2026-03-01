from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from ui.data_client import DataSource, fetch_json
from ui.github_client import GithubClient, GithubConfig
from ui.models import thread_id_for, utc_now


st.set_page_config(page_title="BladeForums View Tracker", layout="wide")


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


def refresh_cache() -> None:
    st.cache_data.clear()


def main() -> None:
    st.title("BladeForums View Tracker")
    source, github, repo = build_clients()
    read_only = github is None

    config = load_config(source)
    threads_payload = load_threads(source)
    subforums = {item["key"]: item for item in config.get("subforums", [])}

    st.sidebar.header("Settings")
    st.sidebar.caption(f"Tracker repo: {repo}")
    max_rate = config.get("global", {}).get("max_requests_per_minute", 12)
    new_rate = st.sidebar.number_input(
        "Max requests per minute",
        min_value=1,
        max_value=19,
        value=int(max_rate),
        step=1,
        disabled=read_only,
    )
    if not read_only and new_rate != max_rate:
        if st.sidebar.button("Update rate limit", key="update_rate"):
            config["global"]["max_requests_per_minute"] = int(new_rate)
            update_json_file(github, "data/config.json", config, "Update rate limit")
            refresh_cache()
            st.sidebar.success("Updated rate limit")

    st.sidebar.subheader("Subforums")
    for subforum in config.get("subforums", []):
        label = f"{subforum['name']}"
        max_pages = int(subforum.get("max_pages_per_update", 3))
        new_pages = st.sidebar.number_input(
            label,
            min_value=1,
            max_value=10,
            value=max_pages,
            step=1,
            disabled=read_only,
        )
        if not read_only and new_pages != max_pages:
            if st.sidebar.button(
                f"Save pages for {subforum['key']}",
                key=f"save_pages_{subforum['key']}",
            ):
                subforum["max_pages_per_update"] = int(new_pages)
                update_json_file(github, "data/config.json", config, "Update subforum settings")
                refresh_cache()
                st.sidebar.success("Updated subforum settings")

    st.header("Add Thread")
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

    st.header("Tracked Threads")
    threads = threads_payload.get("threads", [])
    if not threads:
        st.info("No threads are being tracked yet.")
        return

    for thread in threads:
        thread_id = thread.get("id") or thread_id_for(thread["title"], thread["subforum_key"])
        status = thread.get("status", "active")
        last_view = thread.get("last_view_count")
        subforum_name = subforums.get(thread["subforum_key"], {}).get("name", thread["subforum_key"])

        with st.container(border=True):
            st.subheader(thread["title"])
            st.caption(f"{subforum_name} | Status: {status}")
            st.write(f"Last view count: {last_view if last_view is not None else 'N/A'}")

            cols = st.columns(4)
            if status == "active":
                if cols[0].button("Pause", key=f"pause_{thread_id}", disabled=read_only):
                    update_thread_status(github, thread_id, "paused")
                    refresh_cache()
                    st.success("Paused")
            else:
                if cols[0].button("Resume", key=f"resume_{thread_id}", disabled=read_only):
                    update_thread_status(github, thread_id, "active")
                    refresh_cache()
                    st.success("Resumed")

            if cols[1].button("Reset", key=f"reset_{thread_id}", disabled=read_only):
                reset_thread_samples(github, thread_id, thread["title"])
                refresh_cache()
                st.success("Reset samples")

            if cols[2].button("Remove", key=f"remove_{thread_id}", disabled=read_only):
                remove_thread(github, thread_id)
                refresh_cache()
                st.success("Removed")

            samples = load_samples(source, thread_id).get("samples", [])
            if samples:
                df = pd.DataFrame(samples)
                df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
                fig = px.line(df, x="ts", y="views", title="Views over time")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No samples recorded yet.")

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
