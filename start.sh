#!/usr/bin/env bash
set -euo pipefail

# Simple bootstrap script to run the Streamlit UI
# - Creates a local venv at .venv if missing
# - Installs requirements
# - Launches the app

cd "$(dirname "$0")"

PY=${PYTHON:-python3}
if [ ! -d .venv ]; then
  echo "Creating virtualenv in .venv" >&2
  "$PY" -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "Starting Streamlit app..." >&2
exec streamlit run app.py

