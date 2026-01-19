#!/usr/bin/env bash
set -euo pipefail

sql_rel="${1:-}"
if [[ -z "$sql_rel" ]]; then
  echo "[run_sql] usage: run_sql.sh <sql_rel_path>" >&2
  exit 2
fi

if [[ ! -f "$sql_rel" ]]; then
  echo "[run_sql] file not found: ${sql_rel}" >&2
  exit 3
fi

echo "[run_sql] apply ${sql_rel} (stdin)"

MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" \
  docker exec -e PAGER=cat -i geoetl_postgis \
  psql -U "${DB_USER:-geoetl}" -d "${DB_NAME:-geoetl}" -v ON_ERROR_STOP=1 \
  < "${sql_rel}"
