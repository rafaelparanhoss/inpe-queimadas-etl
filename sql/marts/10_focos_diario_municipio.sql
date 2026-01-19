create schema if not exists marts;

create table if not exists marts.focos_diario_municipio (
  day date not null,
  mun_cd_mun text not null,
  mun_nm_mun text,
  mun_uf text,
  mun_area_km2 double precision,
  n_focos bigint,
  focos_por_100km2 numeric
);

create unique index if not exists uq_marts_focos_diario_mun_day
  on marts.focos_diario_municipio (day, mun_cd_mun);

create index if not exists idx_marts_focos_diario_mun_day
  on marts.focos_diario_municipio (day);

create index if not exists idx_marts_focos_diario_mun_cd_day
  on marts.focos_diario_municipio (mun_cd_mun, day);

create index if not exists idx_marts_focos_diario_mun_uf_day
  on marts.focos_diario_municipio (mun_uf, day);

delete from marts.focos_diario_municipio
where day = :'DATE'::date;

insert into marts.focos_diario_municipio (
  day,
  mun_cd_mun,
  mun_nm_mun,
  mun_uf,
  mun_area_km2,
  n_focos,
  focos_por_100km2
)
select
  coalesce(f.view_ts::date, f.file_date) as day,
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
  and coalesce(f.view_ts::date, f.file_date) = :'DATE'::date
group by 1, 2, 3, 4;
