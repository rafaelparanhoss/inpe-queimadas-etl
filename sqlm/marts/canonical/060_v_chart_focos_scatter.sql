create schema if not exists marts;

drop view if exists marts.v_chart_focos_scatter;
create view marts.v_chart_focos_scatter as
select
  coalesce(view_ts::date, file_date) as day,
  event_hash,
  file_date,
  view_ts,
  satelite,
  municipio,
  estado,
  bioma,
  lat,
  lon,
  geom,
  mun_cd_mun,
  mun_nm_mun,
  mun_uf,
  mun_area_km2,
  cd_bioma,
  cd_cnuc,
  nome_uc,
  terrai_cod,
  terrai_nom,
  etnia_nome
from marts.v_focos_enriched_full
where lat is not null
  and lon is not null;
