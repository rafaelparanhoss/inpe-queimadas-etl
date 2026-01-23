create schema if not exists marts;

create table if not exists marts.focos_diario_bioma (
  day date not null,
  cd_bioma text not null,
  bioma text,
  focos integer not null,
  primary key (day, cd_bioma)
);

create index if not exists idx_marts_focos_diario_bioma_day
  on marts.focos_diario_bioma (day);

create index if not exists idx_marts_focos_diario_bioma_cd_day
  on marts.focos_diario_bioma (cd_bioma, day);

delete from marts.focos_diario_bioma
where day = :'DATE'::date;

insert into marts.focos_diario_bioma (day, cd_bioma, bioma, focos)
select
  file_date as day,
  cd_bioma,
  max(bioma) as bioma,
  count(*)::int as focos
from curated.inpe_focos_enriched
where file_date = :'DATE'::date
  and geom is not null
  and cd_bioma is not null
group by file_date, cd_bioma;