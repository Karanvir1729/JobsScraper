import os
import io
import time
from datetime import datetime
from pathlib import Path

import streamlit as st
import yaml

import sys
import subprocess
from scrapy.utils.project import get_project_settings


DEFAULT_CONFIG_PATH = Path("config/sources.yml")
EXAMPLE_CONFIG_PATH = Path("config/sources.example.yml")
OUTPUT_DIR = Path("output")


def ensure_paths():
    OUTPUT_DIR.mkdir(exist_ok=True)
    DEFAULT_CONFIG_PATH.parent.mkdir(exist_ok=True)
    if not DEFAULT_CONFIG_PATH.exists() and EXAMPLE_CONFIG_PATH.exists():
        DEFAULT_CONFIG_PATH.write_text(EXAMPLE_CONFIG_PATH.read_text(), encoding="utf-8")


def load_config_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return "sources: []\n"


def save_config_text(path: Path, text: str) -> None:
    # Validate YAML before saving to help the user
    yaml.safe_load(text)
    path.write_text(text, encoding="utf-8")


def run_scrape(config_path: Path, time_limit_sec: int, max_items: int | None,
               concurrent_requests: int, download_delay: float) -> Path:
    """Run the Scrapy spider in a subprocess to avoid Twisted signal issues."""
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = OUTPUT_DIR / f"providers-{ts}.csv"

    cmd = [
        sys.executable,
        str(Path(__file__).parent / "run_spider.py"),
        "--config", str(config_path),
        "--csv", str(csv_path),
        "--timeout", str(int(time_limit_sec)),
        "--concurrent", str(int(concurrent_requests)),
        "--delay", str(float(download_delay)),
    ]
    if max_items:
        cmd += ["--max-items", str(int(max_items))]

    # Run and stream output to Streamlit logs if needed
    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        st.error(f"Crawler failed. stderr:\n{completed.stderr}")
    else:
        # Show a bit of last lines of stdout for context
        tail = "\n".join(completed.stdout.splitlines()[-10:])
        if tail:
            st.code(tail)

    return csv_path


def main():
    st.set_page_config(page_title="Canada Home Services Scraper", layout="wide")
    ensure_paths()

    st.title("Canada Home Services Scraper")
    st.caption("Config-driven Scrapy crawler with Streamlit UI. Edit sources and run.")

    with st.sidebar:
        st.header("Run Settings")
        time_limit = st.number_input("Max runtime (seconds)", min_value=15, max_value=60*60, value=180, step=15)
        max_items = st.number_input("Max items (optional)", min_value=0, max_value=100000, value=0, step=50,
                                    help="Stop after N items. Leave 0 to ignore.")
        concurrent_requests = st.slider("Concurrent requests", min_value=2, max_value=32, value=8)
        download_delay = st.slider("Download delay (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
        st.divider()
        st.caption("Respect each source's robots.txt and terms.")

    st.subheader("Sources Configuration (YAML)")
    config_text = load_config_text(DEFAULT_CONFIG_PATH)
    current_text = st.session_state.get("config_text", config_text)
    config_editor = st.text_area(
        "Edit and Save to use these sources",
        value=current_text,
        height=360,
        label_visibility="collapsed",
        key="config_text",
    )
    cols = st.columns([1,1,1])
    with cols[0]:
        if st.button("Save Config", type="primary"):
            try:
                save_config_text(DEFAULT_CONFIG_PATH, st.session_state.get("config_text", config_editor))
                st.success("Config saved.")
            except Exception as e:
                st.error(f"Failed to save config: {e}")
    with cols[1]:
        if st.button("Load Example"):
            if EXAMPLE_CONFIG_PATH.exists():
                st.session_state["config_text"] = EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8")
                st.experimental_rerun()
            else:
                st.info("No example config shipped.")
    with cols[2]:
        if st.button("Revert to Disk"):
            st.session_state["config_text"] = load_config_text(DEFAULT_CONFIG_PATH)
            st.experimental_rerun()

    st.subheader("Run Scraper")
    run_clicked = st.button("Run", type="primary")

    if run_clicked:
        try:
            # Persist current editor contents before running
            save_config_text(DEFAULT_CONFIG_PATH, st.session_state.get("config_text", config_editor))
        except Exception as e:
            st.error(f"Config invalid: {e}")
            return

        with st.spinner("Running scraper... This will block until it finishes or times out."):
            start = time.time()
            csv_path = run_scrape(
                DEFAULT_CONFIG_PATH,
                int(time_limit),
                int(max_items) if max_items > 0 else None,
                int(concurrent_requests),
                float(download_delay),
            )
            elapsed = time.time() - start

        if csv_path.exists():
            st.success(f"Done in {elapsed:.1f}s. CSV generated: {csv_path}")
            st.download_button("Download CSV", data=csv_path.read_bytes(), file_name=csv_path.name, mime="text/csv")
        else:
            st.warning("No CSV produced. Check logs/selectors and try again.")


if __name__ == "__main__":
    main()
