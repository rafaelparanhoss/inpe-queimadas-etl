drop view if exists marts.vw_geo_focos_diario_uf;
create or replace view marts.geo_focos_diario_uf as
select
  u.day,
  u.uf,
  null::text as cd_mun,
  null::text as nm_mun,
  u.n_focos::int as n_focos,
  r.geom,
  r.area_km2
from marts.focos_diario_uf u
join ref.ibge_ufs_web r
  on r.uf = u.uf;
