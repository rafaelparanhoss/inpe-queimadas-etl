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

psql_value() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 -t -A -c "${sql}" \
    | tr -d '\r' | xargs
}

log "start | date=${date_str:-all}"

ref_count="$(psql_value "select count(*) from ref.ibge_municipios;")"
log "ref ibge municipios | count=${ref_count}"

log "raw counts by file_date (top 10)"
psql_exec "select file_date, count(*) as n from raw.inpe_focos group by 1 order by 2 desc limit 10;"

log "curated pct_com_mun (global)"
pct_global="$(psql_value "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) from curated.inpe_focos_enriched;")"
log "pct_com_mun_global=${pct_global}"

pct_check="$pct_global"
if [[ -n "${date_str}" ]]; then
  log "curated pct_com_mun (date=${date_str})"
  pct_day="$(psql_value "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) from curated.inpe_focos_enriched where file_date = '${date_str}'::date;")"
  log "pct_com_mun_day=${pct_day}"
  pct_check="$pct_day"
fi

if [[ -z "${pct_check}" || "${pct_check}" = "0" || "${pct_check}" = "0.00" ]]; then
  log "error | pct_com_mun is zero"
  exit 1
fi

if [[ -n "${date_str}" ]]; then
  marts_count="$(psql_value "select count(*) from marts.focos_diario_uf where day = '${date_str}'::date;")"
else
  marts_count="$(psql_value "select count(*) from marts.focos_diario_uf;")"
fi

if [[ -z "${marts_count}" || "${marts_count}" -eq 0 ]]; then
  log "error | marts uf totals empty"
  exit 1
fi

log "marts uf totals (top 10)"
psql_exec "select uf, sum(n_focos) as n_focos from marts.focos_diario_uf group by 1 order by 2 desc limit 10;"

log "done"
