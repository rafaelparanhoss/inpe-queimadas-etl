-- 60_focos_diario_ti.sql
create table if not exists marts.focos_diario_ti (
  day date not null,
  terrai_cod text not null,
  terrai_nom text,
  etnia_nome text,
  focos integer not null,
  primary key (day, terrai_cod)
);

create index if not exists idx_marts_focos_diario_ti_day
  on marts.focos_diario_ti (day);

create index if not exists idx_marts_focos_diario_ti_cod_day
  on marts.focos_diario_ti (terrai_cod, day);

delete from marts.focos_diario_ti
where day = :'DATE'::date;

insert into marts.focos_diario_ti (day, terrai_cod, terrai_nom, etnia_nome, focos)
select
  file_date as day,
  terrai_cod,
  max(terrai_nom) as terrai_nom,
  max(etnia_nome) as etnia_nome,
  count(*)::int as focos
from curated.inpe_focos_enriched
where file_date = :'DATE'::date
  and geom is not null
  and terrai_cod is not null
group by file_date, terrai_cod;