#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_enrich] $*"
}

usage() {
  echo "usage: scripts/run_enrich.sh --date YYYY-MM-DD"
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

if [[ -z "${date_str}" ]]; then
  usage
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

run_enrich() {
  shopt -s nullglob
  local files=(sql/enrich/*.sql)

  if [ ${#files[@]} -eq 0 ]; then
    log "no sql/enrich files"
    exit 1
  fi

  for f in "${files[@]}"; do
    log "run $f | date=$date_str"
    "$script_dir/run_sql.sh" "$f" --var "DATE=$date_str"
  done

  log "done | files=${#files[@]}"
}

run_enrich
