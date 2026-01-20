create schema if not exists marts;

create table if not exists marts.focos_periodo_mun (
  period_start date not null,
  period_end date not null,
  mun_cd_mun text not null,
  mun_nm_mun text,
  mun_uf text,
  mun_area_km2 double precision,
  n_focos_total bigint,
  n_focos_avg_daily numeric,
  n_focos_max_daily bigint,
  peak_day date,
  focos_por_100km2 numeric
);

create unique index if not exists uq_marts_focos_periodo_mun
  on marts.focos_periodo_mun (period_start, period_end, mun_cd_mun);

delete from marts.focos_periodo_mun
where period_start = :'START'::date
  and period_end = :'END'::date;

with base as (
  select
    day,
    mun_cd_mun,
    mun_nm_mun,
    mun_uf,
    mun_area_km2,
    n_focos
  from marts.focos_diario_municipio
  where day between :'START'::date and :'END'::date
),
stats as (
  select
    mun_cd_mun,
    mun_nm_mun,
    mun_uf,
    max(mun_area_km2) as mun_area_km2,
    sum(n_focos) as n_focos_total,
    round(avg(n_focos)::numeric, 2) as n_focos_avg_daily,
    max(n_focos) as n_focos_max_daily
  from base
  group by mun_cd_mun, mun_nm_mun, mun_uf
),
peaks as (
  select distinct on (mun_cd_mun)
    mun_cd_mun,
    day as peak_day
  from base
  order by mun_cd_mun, n_focos desc, day
)
insert into marts.focos_periodo_mun (
  period_start,
  period_end,
  mun_cd_mun,
  mun_nm_mun,
  mun_uf,
  mun_area_km2,
  n_focos_total,
  n_focos_avg_daily,
  n_focos_max_daily,
  peak_day,
  focos_por_100km2
)
select
  :'START'::date as period_start,
  :'END'::date as period_end,
  s.mun_cd_mun,
  s.mun_nm_mun,
  s.mun_uf,
  s.mun_area_km2,
  s.n_focos_total,
  s.n_focos_avg_daily,
  s.n_focos_max_daily,
  p.peak_day,
  round(
    (100 * s.n_focos_total::numeric) / nullif(s.mun_area_km2::numeric, 0),
    4
  ) as focos_por_100km2
from stats s
join peaks p on p.mun_cd_mun = s.mun_cd_mun;
