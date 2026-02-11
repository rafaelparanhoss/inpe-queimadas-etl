-- reference schemas and extensions
create extension if not exists postgis;

create schema if not exists ref_core;

-- ibge municipalities (geom in 4326)
create table if not exists ref_core.ibge_municipios (
  cd_mun text primary key,
  nm_mun text,
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326) not null
);

create index if not exists idx_ref_core_ibge_municipios_geom
  on ref_core.ibge_municipios using gist (geom);

create index if not exists idx_ref_core_ibge_municipios_uf
  on ref_core.ibge_municipios (uf);
