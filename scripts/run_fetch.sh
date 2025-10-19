#!/usr/bin/env bash
set -euo pipefail
# run business logic in the container or locally (requires project PYTHONPATH to include ./src)
python - <<'PY'
from src.fetch_top_pages import fetch_top_pages
print(fetch_top_pages())
PY
