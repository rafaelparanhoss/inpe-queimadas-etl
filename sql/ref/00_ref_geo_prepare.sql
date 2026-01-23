-- 00_ref_geo_prepare.sql
create schema if not exists ref;

-- 1) índices básicos (municípios)
create index if not exists idx_ref_ibge_municipios_geom
  on ref.ibge_municipios using gist (geom);

create index if not exists idx_ref_ibge_municipios_cd_mun
  on ref.ibge_municipios (cd_mun);

create index if not exists idx_ref_ibge_municipios_uf
  on ref.ibge_municipios (uf);

analyze ref.ibge_municipios;

-- 2) tabela web (geometria simplificada) para dashboard
drop table if exists ref.ibge_municipios_web;
create table ref.ibge_municipios_web as
select
  cd_mun,
  nm_mun,
  uf,
  area_km2,
  st_simplifypreservetopology(geom, 0.001) as geom  -- ~100m (aprox); bom p/ web
from ref.ibge_municipios
where geom is not null;

create index if not exists idx_ref_ibge_municipios_web_geom
  on ref.ibge_municipios_web using gist (geom);

create index if not exists idx_ref_ibge_municipios_web_cd_mun
  on ref.ibge_municipios_web (cd_mun);

create index if not exists idx_ref_ibge_municipios_web_uf
  on ref.ibge_municipios_web (uf);

analyze ref.ibge_municipios_web;

-- 3) UF com geometria (dissolve de municípios)
drop table if exists ref.ibge_ufs_web;
create table ref.ibge_ufs_web as
select
  uf,
  sum(area_km2) as area_km2,
  st_unaryunion(st_collect(geom))::geometry(MultiPolygon, 4326) as geom
from ref.ibge_municipios_web
group by uf;

create index if not exists idx_ref_ibge_ufs_web_geom
  on ref.ibge_ufs_web using gist (geom);

create index if not exists idx_ref_ibge_ufs_web_uf
  on ref.ibge_ufs_web (uf);

analyze ref.ibge_ufs_web;

-- 4) checagem rápida das refs de overlay (bioma/uc/ti)
-- (não falha; só ajuda a você confirmar colunas/geom)
-- biomas
create index if not exists idx_ref_biomas_4326_geom
  on ref.biomas_4326 using gist (geom);

-- ucs (ajuste se necessário após ver colunas)
-- se cnuc_uc tiver geom:
 create index if not exists idx_ref_cnuc_uc_geom on ref.cnuc_uc using gist (geom);

-- tis (se existir)
 create index if not exists idx_ref_tis_4326_geom on ref.tis_4326 using gist (geom);
