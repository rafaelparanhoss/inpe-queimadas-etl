create schema if not exists marts;

drop table if exists marts.focos_diario_uf_trend;

create table marts.focos_diario_uf_trend as
select
  d.day,
  d.uf,
  d.n_focos,
  d.focos_por_100km2,
  round(
    avg(d.n_focos::numeric)
      over (partition by d.uf order by d.day rows between 6 preceding and current row),
    2
  ) as ma7_n_focos,
  round(
    avg(d.n_focos::numeric)
      over (partition by d.uf order by d.day rows between 29 preceding and current row),
    2
  ) as ma30_n_focos
from marts.focos_diario_uf d;

create index if not exists idx_marts_focos_diario_uf_trend_uf_day
  on marts.focos_diario_uf_trend (uf, day);
