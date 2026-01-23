-- 51_focos_mensal_uc.sql
create table if not exists marts.focos_mensal_uc (
  month date not null,
  uc_id text not null,
  cd_cnuc text,
  nome_uc text,
  focos integer not null,
  primary key (month, uc_id)
);

create index if not exists idx_marts_focos_mensal_uc_month
  on marts.focos_mensal_uc (month);

create index if not exists idx_marts_focos_mensal_uc_uc_month
  on marts.focos_mensal_uc (uc_id, month);

with p as (
  select date_trunc('month', :'DATE'::date)::date as month0
)
delete from marts.focos_mensal_uc mu
using p
where mu.month = p.month0;

insert into marts.focos_mensal_uc (month, uc_id, cd_cnuc, nome_uc, focos)
select
  date_trunc('month', file_date)::date as month,
  uc_id,
  max(cd_cnuc) as cd_cnuc,
  max(nome_uc) as nome_uc,
  count(*)::int as focos
from curated.inpe_focos_enriched
where date_trunc('month', file_date)::date = date_trunc('month', :'DATE'::date)::date
  and geom is not null
  and uc_id is not null
group by 1, 2;
