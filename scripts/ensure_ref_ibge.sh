#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[ensure_ref_ibge] $*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

db_user="${DB_USER:-geoetl}"
db_name="${DB_NAME:-geoetl}"
db_pass="${DB_PASSWORD:-geoetl}"
container="${DB_CONTAINER:-geoetl_postgis}"

data_dir="$repo_root/data/ref/ibge_municipios"
zip_name="BR_Municipios_2022.zip"
zip_path="$data_dir/$zip_name"
url="https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"

psql_value() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "$container" \
    psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -t -A -c "$sql" \
    | tr -d '\r' | xargs
}

psql_exec() {
  local sql="$1"
  MSYS_NO_PATHCONV=1 docker exec -i -e PAGER=cat "$container" \
    psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -c "$sql"
}

download_file() {
  local src="$1"
  local dest="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -L --retry 3 --retry-delay 2 -o "$dest" "$src"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -O "$dest" "$src"
    return
  fi

  python - <<PY
import pathlib
import urllib.request

url = "$src"
dest = pathlib.Path("$dest")
dest.parent.mkdir(parents=True, exist_ok=True)

with urllib.request.urlopen(url) as r:
    dest.write_bytes(r.read())
PY
}

file_size() {
  python - <<PY
import os
print(os.path.getsize("$1") if os.path.exists("$1") else 0)
PY
}

unpack_zip() {
  local src="$1"
  local dest="$2"

  if command -v unzip >/dev/null 2>&1; then
    unzip -o "$src" -d "$dest"
    return
  fi

  python - <<PY
import zipfile

src = "$src"
dest = "$dest"
with zipfile.ZipFile(src, 'r') as zf:
    zf.extractall(dest)
PY
}

log "check ref.ibge_municipios"
count="$(psql_value "select count(*) from ref.ibge_municipios;")"
if [[ -n "$count" && "$count" -ge 5000 ]]; then
  log "ok | count=$count"
  exit 0
fi

log "load ref.ibge_municipios | current_count=${count:-0}"

mkdir -p "$data_dir"

zip_size="$(file_size "$zip_path")"
if [[ "$zip_size" -lt 1000000 ]]; then
  rm -f "$zip_path"
fi

if [[ ! -f "$zip_path" ]]; then
  log "download | url=$url"
  download_file "$url" "$zip_path"
fi

unpack_zip "$zip_path" "$data_dir"

shp_path="$(ls -1 "$data_dir"/*.shp 2>/dev/null | head -n 1)"
if [[ -z "$shp_path" ]]; then
  log "shp not found in $data_dir"
  exit 1
fi

network="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "$container" | head -n 1)"
if [[ -z "$network" ]]; then
  log "docker network not found for $container"
  exit 1
fi

log "ogr2ogr import | shp=$(basename "$shp_path")"

MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker run --rm --network "$network" \
  -v "$data_dir:/data" \
  ghcr.io/osgeo/gdal:alpine-small-latest \
  ogr2ogr -overwrite -f "PostgreSQL" \
  PG:"host=$container port=5432 dbname=$db_name user=$db_user password=$db_pass" \
  "/data/$(basename "$shp_path")" \
  -nln ref.ibge_municipios_raw \
  -lco GEOMETRY_NAME=geom \
  -lco FID=gid \
  -nlt MULTIPOLYGON \
  -t_srs EPSG:4326

psql_exec "
truncate ref.ibge_municipios;
insert into ref.ibge_municipios (cd_mun, nm_mun, uf, area_km2, geom)
select
  cd_mun::text,
  nm_mun::text,
  sigla_uf::text,
  coalesce(area_km2, st_area(geom::geography) / 1000000.0),
  geom
from ref.ibge_municipios_raw;

drop table if exists ref.ibge_municipios_raw;
"

count="$(psql_value "select count(*) from ref.ibge_municipios;")"
if [[ -z "$count" || "$count" -lt 5000 ]]; then
  log "load failed | count=${count:-0}"
  exit 1
fi

log "done | count=$count"
