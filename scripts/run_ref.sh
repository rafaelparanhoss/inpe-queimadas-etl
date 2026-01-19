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
  local schema_file="sql/ref/01_ref_schema.sql"

  if [ ${#files[@]} -eq 0 ]; then
    log "no sql/ref files"
    exit 1
  fi

  if [ -f "$schema_file" ]; then
    log "run $schema_file"
    "$script_dir/run_sql.sh" "$schema_file"
  fi

  if [ -f "$script_dir/ensure_ref_ibge.sh" ]; then
    log "ensure ref ibge"
    bash "$script_dir/ensure_ref_ibge.sh"
  fi

  for f in "${files[@]}"; do
    if [ "$f" = "$schema_file" ]; then
      continue
    fi
    log "run $f"
    "$script_dir/run_sql.sh" "$f"
  done

  log "done | files=${#files[@]}"
}

run_ref
