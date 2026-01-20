create schema if not exists marts;

create table if not exists marts.focos_periodo_uf (
  period_start date not null,
  period_end date not null,
  uf text not null,
  uf_area_km2 double precision,
  n_focos_total bigint,
  n_focos_avg_daily numeric,
  n_focos_max_daily bigint,
  peak_day date,
  focos_por_100km2 numeric
);

create unique index if not exists uq_marts_focos_periodo_uf
  on marts.focos_periodo_uf (period_start, period_end, uf);

delete from marts.focos_periodo_uf
where period_start = :'START'::date
  and period_end = :'END'::date;

with base as (
  select
    day,
    uf,
    n_focos,
    uf_area_km2
  from marts.focos_diario_uf
  where day between :'START'::date and :'END'::date
),
stats as (
  select
    uf,
    max(uf_area_km2) as uf_area_km2,
    sum(n_focos) as n_focos_total,
    round(avg(n_focos)::numeric, 2) as n_focos_avg_daily,
    max(n_focos) as n_focos_max_daily
  from base
  group by uf
),
peaks as (
  select distinct on (uf)
    uf,
    day as peak_day
  from base
  order by uf, n_focos desc, day
)
insert into marts.focos_periodo_uf (
  period_start,
  period_end,
  uf,
  uf_area_km2,
  n_focos_total,
  n_focos_avg_daily,
  n_focos_max_daily,
  peak_day,
  focos_por_100km2
)
select
  :'START'::date as period_start,
  :'END'::date as period_end,
  s.uf,
  s.uf_area_km2,
  s.n_focos_total,
  s.n_focos_avg_daily,
  s.n_focos_max_daily,
  p.peak_day,
  round(
    (100 * s.n_focos_total::numeric) / nullif(s.uf_area_km2::numeric, 0),
    4
  ) as focos_por_100km2
from stats s
join peaks p on p.uf = s.uf;
