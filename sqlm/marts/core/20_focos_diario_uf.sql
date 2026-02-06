create schema if not exists marts;

create table if not exists marts.focos_diario_uf (
  day date not null,
  uf text not null,
  uf_area_km2 double precision,
  n_focos bigint,
  focos_por_100km2 numeric
);

create unique index if not exists uq_marts_focos_diario_uf_day
  on marts.focos_diario_uf (day, uf);

create index if not exists idx_marts_focos_diario_uf_day
  on marts.focos_diario_uf (day);

create index if not exists idx_marts_focos_diario_uf_uf_day
  on marts.focos_diario_uf (uf, day);

delete from marts.focos_diario_uf
where day = :'DATE'::date;

insert into marts.focos_diario_uf (
  day,
  uf,
  uf_area_km2,
  n_focos,
  focos_por_100km2
)
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
join ref_core.ibge_uf_area a on a.uf = f.mun_uf
where f.mun_uf is not null
  and coalesce(f.view_ts::date, f.file_date) = :'DATE'::date
group by 1, 2;
