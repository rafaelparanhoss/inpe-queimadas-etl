-- 00_ref_geo_prepare.sql
create schema if not exists ref;

-- 1) indices basicos (municipios)
create index if not exists idx_ref_ibge_municipios_geom
  on ref.ibge_municipios using gist (geom);

create index if not exists idx_ref_ibge_municipios_cd_mun
  on ref.ibge_municipios (cd_mun);

create index if not exists idx_ref_ibge_municipios_uf
  on ref.ibge_municipios (uf);

analyze ref.ibge_municipios;

-- 2) tabela web (geometria simplificada) para dashboard
create table if not exists ref.ibge_municipios_web (
  cd_mun text,
  nm_mun text,
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326)
);
truncate ref.ibge_municipios_web;
insert into ref.ibge_municipios_web (cd_mun, nm_mun, uf, area_km2, geom)
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

-- 3) uf com geometria (dissolve de municipios)
create table if not exists ref.ibge_ufs_web (
  uf text,
  area_km2 double precision,
  geom geometry(MultiPolygon, 4326)
);
truncate ref.ibge_ufs_web;
insert into ref.ibge_ufs_web (uf, area_km2, geom)
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

-- 4) quick overlay indexes (optional)
do $$
begin
  if to_regclass('ref.biomas_4326') is not null then
    execute 'create index if not exists idx_ref_biomas_4326_geom on ref.biomas_4326 using gist (geom)';
  else
    raise notice 'skip optional index: ref.biomas_4326 not found';
  end if;

  if to_regclass('ref.cnuc_uc') is not null then
    execute 'create index if not exists idx_ref_cnuc_uc_geom on ref.cnuc_uc using gist (geom)';
  else
    raise notice 'skip optional index: ref.cnuc_uc not found';
  end if;

  if to_regclass('ref.tis_4326') is not null then
    execute 'create index if not exists idx_ref_tis_4326_geom on ref.tis_4326 using gist (geom)';
  else
    raise notice 'skip optional index: ref.tis_4326 not found';
  end if;
end
$$;
