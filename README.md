# BladeForums Thread View Tracker (Streamlit UI)

This repository provides the Streamlit UI for managing tracked threads and viewing view-count history.

## Configuration

Set these in Streamlit secrets or environment variables:

- `TRACKER_REPO`: `owner/repo` for the tracker repo (required for writes).
- `TRACKER_BRANCH`: branch name (default: `main`).
- `GITHUB_TOKEN`: GitHub token with `Contents: Read and write` on the tracker repo.

The UI reads data from the tracker repo via GitHub raw URLs and performs immediate updates in-session. If `GITHUB_TOKEN` is not set, the UI runs in read-only mode.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```
