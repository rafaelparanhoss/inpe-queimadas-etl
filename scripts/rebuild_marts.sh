#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[rebuild_marts] $*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"
container="${DB_CONTAINER:-geoetl_postgis}"

fetch_dates() {
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 -t -A -c \
    "select distinct coalesce(view_ts::date, file_date) as day from curated.inpe_focos_enriched order by 1;" \
    | tr -d '\r'
}

log "start"

dates="$(fetch_dates)"
if [[ -z "$dates" ]]; then
  log "no dates found"
  exit 1
fi

count=0
while IFS= read -r day; do
  if [[ -z "$day" ]]; then
    continue
  fi
  log "run date=${day}"
  bash "$script_dir/run_marts.sh" --date "$day"
  count=$((count + 1))
done <<< "$dates"

log "done | dates=${count}"
