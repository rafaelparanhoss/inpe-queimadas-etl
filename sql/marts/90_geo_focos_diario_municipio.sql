create schema if not exists marts;

drop view if exists marts.vw_geo_focos_diario_municipio;
create or replace view marts.geo_focos_diario_municipio as
select
  m.day,
  r.uf,
  m.mun_cd_mun as cd_mun,
  r.nm_mun,
  m.n_focos::int as n_focos,
  r.geom,
  r.area_km2
from marts.focos_diario_municipio m
join ref.ibge_municipios_web r
  on r.cd_mun = m.mun_cd_mun;
