#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_marts] $*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

run_marts() {
  shopt -s nullglob
  local files=(sql/marts/*.sql)

  if [ ${#files[@]} -eq 0 ]; then
    log "no sql/marts files"
    exit 1
  fi

  for f in "${files[@]}"; do
    log "run $f"
    "$script_dir/run_sql.sh" "$f"
  done

  log "done | files=${#files[@]}"
}

run_marts
