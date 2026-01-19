#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[report_day] $*"
}

usage() {
  echo "usage: scripts/report_day.sh --date YYYY-MM-DD"
}

date_str=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_str="${2:-}"
      shift 2
      ;;
    --date=*)
      date_str="${1#*=}"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "unknown arg: $1"
      exit 2
      ;;
  esac
done

if [[ -z "$date_str" ]]; then
  usage
  exit 2
fi

if [[ ! "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  log "invalid --date format: ${date_str} (expected YYYY-MM-DD)"
  exit 2
fi

mkdir -p "data/reports/${date_str}"

summary_path="data/reports/${date_str}/summary.txt"
top_uf_csv="data/reports/${date_str}/top_uf.csv"
top_mun_csv="data/reports/${date_str}/top_mun.csv"

db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"
container="${DB_CONTAINER:-geoetl_postgis}"

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

psql_csv() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "${container}" \
    psql -U "${db_user}" -d "${db_name}" -v ON_ERROR_STOP=1 -t -A -F "," -c "${sql}" \
    | tr -d '\r'
}

raw_count="$(psql_value "select count(*) from raw.inpe_focos where file_date = '${date_str}'::date;")"
curated_count="$(psql_value "select count(*) from curated.inpe_focos_enriched where file_date = '${date_str}'::date;")"
pct_com_mun="$(psql_value "select round(100.0 * count(*) filter (where mun_cd_mun is not null) / nullif(count(*), 0), 2) from curated.inpe_focos_enriched where file_date = '${date_str}'::date;")"

log "write summary | path=${summary_path}"
{
  echo "date: ${date_str}"
  echo "raw_count: ${raw_count}"
  echo "curated_count: ${curated_count}"
  echo "pct_com_mun: ${pct_com_mun}"
  echo ""
  echo "top_uf:"
  psql_exec "select uf, n_focos from marts.focos_diario_uf where day = '${date_str}'::date order by n_focos desc limit 10;"
  echo ""
  echo "top_mun:"
  psql_exec "select mun_cd_mun, mun_nm_mun, mun_uf, n_focos from marts.focos_diario_municipio where day = '${date_str}'::date order by n_focos desc limit 20;"
} | tee "${summary_path}"

echo "uf,n_focos" > "${top_uf_csv}"
psql_csv "select uf, n_focos from marts.focos_diario_uf where day = '${date_str}'::date order by n_focos desc limit 10;" >> "${top_uf_csv}"

echo "mun_cd_mun,mun_nm_mun,mun_uf,n_focos" > "${top_mun_csv}"
psql_csv "select mun_cd_mun, mun_nm_mun, mun_uf, n_focos from marts.focos_diario_municipio where day = '${date_str}'::date order by n_focos desc limit 20;" >> "${top_mun_csv}"

log "done"
