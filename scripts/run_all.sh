#!/usr/bin/env bash
set -euo pipefail

date_str=""
do_checks=0

usage() {
  echo "usage: scripts/run_all.sh --date YYYY-MM-DD [--checks]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_str="${2:-}"
      shift 2
      ;;
    --checks)
      do_checks=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[run_all] unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${date_str}" ]]; then
  usage
  exit 2
fi

# valida formato ISO e se é data válida
python - <<'PY' "${date_str}"
import sys
from datetime import date
date.fromisoformat(sys.argv[1])
PY

echo "[run_all] start | date=${date_str}"

bash scripts/run_ref.sh
PYTHONPATH=src python -m uv run python -m etl.cli --date "${date_str}"
bash scripts/run_enrich.sh --date "${date_str}"
bash scripts/run_marts.sh --date "${date_str}"

if [[ "${do_checks}" -eq 1 ]]; then
  bash scripts/checks.sh --date "${date_str}"
fi

echo "[run_all] done | date=${date_str}"
