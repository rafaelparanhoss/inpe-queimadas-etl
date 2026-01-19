create schema if not exists marts;

create table if not exists marts.focos_mensal_uf (
  month date not null,
  uf text not null,
  uf_area_km2 double precision,
  n_focos bigint,
  focos_por_100km2 numeric
);

create unique index if not exists uq_marts_focos_mensal_uf_month
  on marts.focos_mensal_uf (month, uf);

create index if not exists idx_marts_focos_mensal_uf_month
  on marts.focos_mensal_uf (month);

create index if not exists idx_marts_focos_mensal_uf_uf_month
  on marts.focos_mensal_uf (uf, month);

delete from marts.focos_mensal_uf
where month = date_trunc('month', :'DATE'::date)::date;

insert into marts.focos_mensal_uf (
  month,
  uf,
  uf_area_km2,
  n_focos,
  focos_por_100km2
)
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
  and date_trunc('month', coalesce(f.view_ts::date, f.file_date))::date = date_trunc('month', :'DATE'::date)::date
group by 1, 2;
