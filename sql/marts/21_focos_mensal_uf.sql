create schema if not exists marts;

drop table if exists marts.focos_mensal_uf;

create table marts.focos_mensal_uf as
select
  date_trunc('month', coalesce(f.view_ts::date, f.file_date))::date as month,
  f.mun_uf as uf,
  max(a.area_km2) as uf_area_km2,
  count(*) as n_focos,
  round(
    (100 * count(*)::numeric) / nullif(max(a.area_km2)::numeric, 0),
    4
  ) as focos_por_100km2
from curated.inpe_focos_enriched f
join ref.ibge_uf_area a on a.uf = f.mun_uf
where f.mun_uf is not null
group by 1, 2;

create index if not exists idx_marts_focos_mensal_uf_month
  on marts.focos_mensal_uf (month);

create index if not exists idx_marts_focos_mensal_uf_uf_month
  on marts.focos_mensal_uf (uf, month);
