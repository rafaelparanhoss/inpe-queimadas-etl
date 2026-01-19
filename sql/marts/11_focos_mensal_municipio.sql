create schema if not exists marts;

drop table if exists marts.focos_mensal_municipio;

create table marts.focos_mensal_municipio as
select
  date_trunc('month', coalesce(f.view_ts::date, f.file_date))::date as month,
  f.mun_cd_mun,
  f.mun_nm_mun,
  f.mun_uf,
  max(f.mun_area_km2) as mun_area_km2,
  count(*) as n_focos,
  round(
    (100 * count(*)::numeric) / nullif(max(f.mun_area_km2)::numeric, 0),
    4
  ) as focos_por_100km2
from curated.inpe_focos_enriched f
where f.mun_cd_mun is not null
group by 1, 2, 3, 4;

create index if not exists idx_marts_focos_mensal_mun_month
  on marts.focos_mensal_municipio (month);

create index if not exists idx_marts_focos_mensal_mun_cd_month
  on marts.focos_mensal_municipio (mun_cd_mun, month);

create index if not exists idx_marts_focos_mensal_mun_uf_month
  on marts.focos_mensal_municipio (mun_uf, month);
