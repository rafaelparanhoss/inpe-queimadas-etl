-- 10_ref_geo_prepare.sql
create schema if not exists ref_core;

-- 1) indices basicos (municipios)
create index if not exists idx_ref_core_ibge_municipios_geom
  on ref_core.ibge_municipios using gist (geom);

create index if not exists idx_ref_core_ibge_municipios_cd_mun
  on ref_core.ibge_municipios (cd_mun);

create index if not exists idx_ref_core_ibge_municipios_uf
  on ref_core.ibge_municipios (uf);

analyze ref_core.ibge_municipios;

-- 2) tabela web (geometria simplificada) para dashboard
create table if not exists ref_core.ibge_municipios_web (
  cd_mun text,
  nm_mun text,
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326)
);
truncate ref_core.ibge_municipios_web;
insert into ref_core.ibge_municipios_web (cd_mun, nm_mun, uf, area_km2, geom)
select
  cd_mun,
  nm_mun,
  uf,
  area_km2,
  st_simplifypreservetopology(geom, 0.001) as geom
from ref_core.ibge_municipios
where geom is not null;

create index if not exists idx_ref_core_ibge_municipios_web_geom
  on ref_core.ibge_municipios_web using gist (geom);

create index if not exists idx_ref_core_ibge_municipios_web_cd_mun
  on ref_core.ibge_municipios_web (cd_mun);

create index if not exists idx_ref_core_ibge_municipios_web_uf
  on ref_core.ibge_municipios_web (uf);

analyze ref_core.ibge_municipios_web;

-- 3) uf com geometria (dissolve de municipios)
create table if not exists ref_core.ibge_ufs_web (
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326)
);
truncate ref_core.ibge_ufs_web;
insert into ref_core.ibge_ufs_web (uf, area_km2, geom)
select
  uf,
  sum(area_km2) as area_km2,
  st_unaryunion(st_collect(geom))::geometry(MultiPolygon, 4326) as geom
from ref_core.ibge_municipios_web
group by uf;

create index if not exists idx_ref_core_ibge_ufs_web_geom
  on ref_core.ibge_ufs_web using gist (geom);

create index if not exists idx_ref_core_ibge_ufs_web_uf
  on ref_core.ibge_ufs_web (uf);

analyze ref_core.ibge_ufs_web;
