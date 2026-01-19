#!/usr/bin/env bash
set -euo pipefail

date_str=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_str="${2:-}"
      shift 2
      ;;
    -h|--help)
      echo "usage: bash scripts/checks.sh [--date YYYY-MM-DD]"
      exit 0
      ;;
    *)
      echo "[checks] unknown arg: $1"
      exit 2
      ;;
  esac
done

if [[ -n "${date_str}" && ! "${date_str}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[checks] invalid --date format: ${date_str} (expected YYYY-MM-DD)"
  exit 2
fi

db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"
container="${DB_CONTAINER:-geoetl_postgis}"

log() { echo "[checks] $*"; }

psql_exec() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 -c "${sql}"
}

log "start | date=${date_str:-all}"

log "raw counts by file_date (top 10)"
psql_exec "select file_date, count(*) as n from raw.inpe_focos group by 1 order by 2 desc limit 10;"

log "curated pct_com_mun (global)"
psql_exec "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) as pct_com_mun from curated.inpe_focos_enriched;"

if [[ -n "${date_str}" ]]; then
  log "curated pct_com_mun (date=${date_str})"
  psql_exec "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) as pct_com_mun from curated.inpe_focos_enriched where file_date = '${date_str}'::date;"
fi

log "marts uf totals (top 10)"
psql_exec "select uf, sum(n_focos) as n_focos from marts.focos_diario_uf group by 1 order by 2 desc limit 10;"

log "done"
