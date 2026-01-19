#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_sql] $*"
}

usage() {
  echo "usage: scripts/run_sql.sh <relative_sql_path>"
}

repo_root() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  cd "$script_dir/.."
}

exec_psql_file() {
  local path="$1"
  local db_container="${DB_CONTAINER:-geoetl_postgis}"
  local db_user="${DB_USER:-geoetl}"
  local db_name="${DB_NAME:-geoetl}"
  local tty_flag="-it"
  local use_winpty="false"

  if [ ! -t 0 ]; then
    tty_flag="-i"
  else
    if command -v winpty >/dev/null 2>&1; then
      use_winpty="true"
    fi
  fi

  if [ "$use_winpty" = "true" ]; then
    MSYS_NO_PATHCONV=1 winpty docker exec $tty_flag -e PAGER=cat "$db_container" \
      psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -f "/work/$path"
  else
    MSYS_NO_PATHCONV=1 docker exec $tty_flag -e PAGER=cat "$db_container" \
      psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -f "/work/$path"
  fi
}

run_sql() {
  local path="$1"

  if [ ! -f "$path" ]; then
    log "file not found: $path"
    exit 1
  fi

  log "apply $path"
  exec_psql_file "$path"
}

main() {
  if [ $# -ne 1 ]; then
    usage
    exit 1
  fi

  repo_root
  run_sql "$1"
}

main "$@"
