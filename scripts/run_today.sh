#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_today] $*"
}

usage() {
  echo "usage: scripts/run_today.sh [--date YYYY-MM-DD]"
}

resolve_date() {
  if TZ=America/Sao_Paulo date +%F >/dev/null 2>&1; then
    TZ=America/Sao_Paulo date +%F
    return
  fi

  python - <<'PY'
import datetime
import os
import time

os.environ.setdefault("TZ", "America/Sao_Paulo")
if hasattr(time, "tzset"):
    time.tzset()

print(datetime.date.today().isoformat())
PY
}

date_str=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_str="${2:-}"
      shift 2
      ;;
    --date=*)
      date_str="${1#*=}"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "unknown arg: $1"
      exit 2
      ;;
  esac
done

if [[ -z "$date_str" ]]; then
  date_str="$(resolve_date)"
fi

if [[ -z "$date_str" ]]; then
  log "failed to resolve date"
  exit 1
fi

if [[ ! "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  log "invalid --date format: ${date_str} (expected YYYY-MM-DD)"
  exit 2
fi

mkdir -p data/logs "data/reports/${date_str}"

log "start | date=${date_str}"

bash scripts/run_all.sh --date "${date_str}" 2>&1 | tee "data/logs/run_all_${date_str}.log"

bash scripts/checks.sh --date "${date_str}" 2>&1 | tee "data/logs/checks_${date_str}.log"

bash scripts/report_day.sh --date "${date_str}" 2>&1 | tee "data/logs/report_${date_str}.log"

log "done | date=${date_str}"
