#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_marts] $*"
}

usage() {
  echo "usage: scripts/run_marts.sh --date YYYY-MM-DD"
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

run_marts() {
  local files=(
    sql/marts/10_focos_diario_municipio.sql
    sql/marts/11_focos_mensal_municipio.sql
    sql/marts/20_focos_diario_uf.sql
    sql/marts/21_focos_mensal_uf.sql
    sql/marts/30_focos_diario_uf_trend.sql
  )

  for f in "${files[@]}"; do
    if [ ! -f "$f" ]; then
      log "missing file: $f"
      exit 1
    fi
  done

  for f in "${files[@]}"; do
    log "run $f | date=$date_str"
    "$script_dir/run_sql.sh" "$f" --var "DATE=$date_str"
  done

  log "done | files=${#files[@]}"
}

run_marts
