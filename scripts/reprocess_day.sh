#!/usr/bin/env bash
set -euo pipefail

date_str=""
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_str="${2:-}"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      echo "usage: bash scripts/reprocess_day.sh --date YYYY-MM-DD [--dry-run]"
      exit 0
      ;;
    *)
      echo "[reprocess_day] unknown arg: $1"
      exit 2
      ;;
  esac
done

if [[ -z "${date_str}" ]]; then
  echo "[reprocess_day] missing --date YYYY-MM-DD"
  exit 2
fi

if [[ ! "${date_str}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[reprocess_day] invalid --date format: ${date_str} (expected YYYY-MM-DD)"
  exit 2
fi

db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"
container="${DB_CONTAINER:-geoetl_postgis}"

ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] reprocess_day | $*"; }

psql_exec() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 <<SQL
${sql}
SQL
}

psql_query() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 -t -A -F "|" -c "${sql}" \
    | tr -d '\r'
}

sql_counts_basic() {
  cat <<SQL
select
  (select count(*) from raw.inpe_focos where file_date = '${date_str}'::date) as raw_n,
  (select count(*) from curated.inpe_focos_enriched where file_date = '${date_str}'::date) as curated_n;
SQL
}

sql_counts_full() {
  cat <<SQL
select
  (select count(*) from raw.inpe_focos where file_date = '${date_str}'::date) as raw_n,
  (select count(*) from curated.inpe_focos_enriched where file_date = '${date_str}'::date) as curated_n,
  (select coalesce(sum(n_focos),0) from marts.focos_diario_municipio where day = '${date_str}'::date) as marts_day_sum;
SQL
}

sql_delete_day() {
  cat <<SQL
begin;
delete from curated.inpe_focos_enriched where file_date = '${date_str}'::date;
delete from raw.inpe_focos where file_date = '${date_str}'::date;
commit;
SQL
}

log "start | date=${date_str} | dry_run=${dry_run}"

log "counts before"
if [[ "${dry_run}" -eq 1 ]]; then
  log "dry-run sql:"
  echo
  sql_counts_full
  echo
else
  psql_exec "$(sql_counts_full)"
fi

log "delete day (raw + curated)"
if [[ "${dry_run}" -eq 1 ]]; then
  log "dry-run sql:"
  echo
  sql_delete_day
  echo
else
  psql_exec "$(sql_delete_day)"
fi

log "counts after delete (marts may be stale)"
if [[ "${dry_run}" -eq 1 ]]; then
  log "dry-run sql:"
  echo
  sql_counts_basic
  echo
else
  psql_exec "$(sql_counts_basic)"
fi

if [[ "${dry_run}" -eq 1 ]]; then
  log "done"
  exit 0
fi

log "run etl cli (raw load)"
PYTHONPATH=src python -m uv run python -m etl.cli --date "${date_str}"

log "run enrich"
bash scripts/run_enrich.sh --date "${date_str}"

log "run marts"
bash scripts/run_marts.sh --date "${date_str}"

log "final checks"
counts_row="$(psql_query "$(sql_counts_full)")"
IFS="|" read -r raw_n curated_n marts_day_sum <<< "${counts_row}"
log "final counts | raw_n=${raw_n} | curated_n=${curated_n} | marts_day_sum=${marts_day_sum}"

if [[ -z "${raw_n}" || -z "${curated_n}" || -z "${marts_day_sum}" ]]; then
  log "error | missing counts in final checks"
  exit 1
fi

if [[ "${raw_n}" -ne "${curated_n}" ]]; then
  log "error | raw_n != curated_n | raw_n=${raw_n} | curated_n=${curated_n}"
  exit 1
fi

if [[ "${marts_day_sum}" -ne "${curated_n}" ]]; then
  log "error | marts_day_sum != curated_n | marts_day_sum=${marts_day_sum} | curated_n=${curated_n}"
  exit 1
fi

log "done"
