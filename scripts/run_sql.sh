#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "[run_sql] usage: run_sql.sh <sql_rel_path> [--var KEY=VALUE]..." >&2
}

sql_rel="${1:-}"
if [[ -z "$sql_rel" ]]; then
  usage
  exit 2
fi
shift

psql_vars=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --var)
      kv="${2:-}"
      shift 2
      ;;
    --var=*)
      kv="${1#*=}"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[run_sql] unknown arg: $1" >&2
      exit 2
      ;;
  esac

  if [[ -z "${kv:-}" || "${kv}" != *=* ]]; then
    echo "[run_sql] invalid --var, expected KEY=VALUE" >&2
    exit 2
  fi

  psql_vars+=("-v" "$kv")
  unset kv

done

if [[ ! -f "$sql_rel" ]]; then
  echo "[run_sql] file not found: ${sql_rel}" >&2
  exit 3
fi

container="${DB_CONTAINER:-geoetl_postgis}"
db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"

echo "[run_sql] apply ${sql_rel} (stdin)"

MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" \
  docker exec -e PAGER=cat -i "$container" \
  psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 "${psql_vars[@]}" \
  < "$sql_rel"
