-- reference schemas and extensions
create extension if not exists postgis;

create schema if not exists ref;

-- ibge municipalities (geom in 4326)
create table if not exists ref.ibge_municipios (
  cd_mun text primary key,
  nm_mun text,
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326) not null
);

create index if not exists idx_ref_ibge_municipios_geom
  on ref.ibge_municipios using gist (geom);

create index if not exists idx_ref_ibge_municipios_uf
  on ref.ibge_municipios (uf);

-- conservation units (cnuc/mma)
create table if not exists ref.cnuc_uc (
  id_uc text primary key,
  nome text,
  categoria text,
  esfera text,
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326) not null
);

create index if not exists idx_ref_cnuc_uc_geom
  on ref.cnuc_uc using gist (geom);

create index if not exists idx_ref_cnuc_uc_uf
  on ref.cnuc_uc (uf);
