#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_ref] $*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

run_ref() {
  shopt -s nullglob
  local files=(sql/ref/*.sql)

  if [ ${#files[@]} -eq 0 ]; then
    log "no sql/ref files"
    exit 1
  fi

  for f in "${files[@]}"; do
    log "run $f"
    "$script_dir/run_sql.sh" "$f"
  done

  log "done | files=${#files[@]}"
}

run_ref
