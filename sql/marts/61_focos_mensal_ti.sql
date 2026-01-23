-- 61_focos_mensal_ti.sql
create table if not exists marts.focos_mensal_ti (
  month date not null,
  terrai_cod text not null,
  terrai_nom text,
  etnia_nome text,
  focos integer not null,
  primary key (month, terrai_cod)
);

create index if not exists idx_marts_focos_mensal_ti_month
  on marts.focos_mensal_ti (month);

create index if not exists idx_marts_focos_mensal_ti_cod_month
  on marts.focos_mensal_ti (terrai_cod, month);

with p as (
  select date_trunc('month', :'DATE'::date)::date as month0
)
delete from marts.focos_mensal_ti mt
using p
where mt.month = p.month0;

insert into marts.focos_mensal_ti (month, terrai_cod, terrai_nom, etnia_nome, focos)
select
  date_trunc('month', file_date)::date as month,
  terrai_cod,
  max(terrai_nom) as terrai_nom,
  max(etnia_nome) as etnia_nome,
  count(*)::int as focos
from curated.inpe_focos_enriched
where date_trunc('month', file_date)::date = date_trunc('month', :'DATE'::date)::date
  and geom is not null
  and terrai_cod is not null
group by 1, 2;