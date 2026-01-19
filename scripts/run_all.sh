#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[run_all] $*"
}

usage() {
  echo "usage: scripts/run_all.sh --date YYYY-MM-DD"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

load_env() {
  if [ -f "$repo_root/.env.local" ]; then
    log "load .env.local"
    set -a
    . "$repo_root/.env.local"
    set +a
  fi
}

parse_args() {
  local date=""

  while [ $# -gt 0 ]; do
    case "$1" in
      --date)
        date="${2:-}"
        shift 2
        ;;
      --date=*)
        date="${1#*=}"
        shift 1
        ;;
      *)
        usage
        exit 1
        ;;
    esac
  done

  if [ -z "$date" ]; then
    usage
    exit 1
  fi

  echo "$date"
}

psql_value() {
  local sql="$1"
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
      psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -t -A -c "$sql" \
      | tr -d '\r' | xargs
  else
    MSYS_NO_PATHCONV=1 docker exec $tty_flag -e PAGER=cat "$db_container" \
      psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -t -A -c "$sql" \
      | tr -d '\r' | xargs
  fi
}

run_validations() {
  local n_curated
  local n_distinct
  local n_dup
  local pct_com_mun
  local n_marts

  n_curated="$(psql_value "select count(*) from curated.inpe_focos_enriched;")"
  n_distinct="$(psql_value "select count(distinct event_hash) from curated.inpe_focos_enriched;")"
  n_dup="$(psql_value "select count(*) - count(distinct event_hash) from curated.inpe_focos_enriched;")"
  pct_com_mun="$(psql_value "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) from curated.inpe_focos_enriched;")"
  n_marts="$(psql_value "select coalesce(sum(n_focos), 0) from marts.focos_diario_municipio;")"

  log "validation | curated=$n_curated distinct=$n_distinct dup=$n_dup pct_com_mun=$pct_com_mun"
  log "validation | n_marts=$n_marts n_base=$n_curated"

  if [ "${n_curated:-0}" -eq 0 ]; then
    log "validation failed | curated is empty"
    exit 1
  fi

  if [ "${n_dup:-0}" -gt 0 ]; then
    log "validation failed | duplicates=$n_dup"
    exit 1
  fi
}

main() {
  local date
  date="$(parse_args "$@")"

  load_env

  export PYTHONPATH=src
  log "start | date=$date"

  bash "$script_dir/run_ref.sh"
  python -m uv run python -m etl.cli --date "$date"
  bash "$script_dir/run_enrich.sh"
  bash "$script_dir/run_marts.sh"

  run_validations
  log "done"
}

main "$@"
