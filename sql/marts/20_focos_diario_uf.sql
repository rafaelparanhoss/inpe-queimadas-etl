create schema if not exists marts;

drop table if exists marts.focos_diario_uf;

create table marts.focos_diario_uf as
select
  coalesce(f.view_ts::date, f.file_date) as day,
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

create index if not exists idx_marts_focos_diario_uf_day
  on marts.focos_diario_uf (day);

create index if not exists idx_marts_focos_diario_uf_uf_day
  on marts.focos_diario_uf (uf, day);
