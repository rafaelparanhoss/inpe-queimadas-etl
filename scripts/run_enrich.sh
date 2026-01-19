#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_enrich] $*"
}

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
    log "run $f"
    "$script_dir/run_sql.sh" "$f"
  done

  log "done | files=${#files[@]}"
}

run_enrich
