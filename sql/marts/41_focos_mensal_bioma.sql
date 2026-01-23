create table if not exists marts.focos_mensal_bioma (
  month date not null,
  cd_bioma text not null,
  bioma text,
  focos integer not null,
  primary key (month, cd_bioma)
);

create index if not exists idx_marts_focos_mensal_bioma_month
  on marts.focos_mensal_bioma (month);

create index if not exists idx_marts_focos_mensal_bioma_cd_month
  on marts.focos_mensal_bioma (cd_bioma, month);

-- recalcula o mês do dia processado
with p as (
  select date_trunc('month', :'DATE'::date)::date as month0
)
delete from marts.focos_mensal_bioma mb
using p
where mb.month = p.month0;

insert into marts.focos_mensal_bioma (month, cd_bioma, bioma, focos)
select
  date_trunc('month', file_date)::date as month,
  cd_bioma,
  max(bioma) as bioma,
  count(*)::int as focos
from curated.inpe_focos_enriched
where date_trunc('month', file_date)::date = date_trunc('month', :'DATE'::date)::date
  and geom is not null
  and cd_bioma is not null
group by 1, 2;
