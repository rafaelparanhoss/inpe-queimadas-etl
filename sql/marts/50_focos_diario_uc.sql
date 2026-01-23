-- 50_focos_diario_uc.sql
create table if not exists marts.focos_diario_uc (
  day date not null,
  uc_id text not null,
  cd_cnuc text,
  nome_uc text,
  focos integer not null,
  primary key (day, uc_id)
);

create index if not exists idx_marts_focos_diario_uc_day
  on marts.focos_diario_uc (day);

create index if not exists idx_marts_focos_diario_uc_uc_day
  on marts.focos_diario_uc (uc_id, day);

delete from marts.focos_diario_uc
where day = :'DATE'::date;

insert into marts.focos_diario_uc (day, uc_id, cd_cnuc, nome_uc, focos)
select
  file_date as day,
  uc_id,
  max(cd_cnuc) as cd_cnuc,
  max(nome_uc) as nome_uc,
  count(*)::int as focos
from curated.inpe_focos_enriched
where file_date = :'DATE'::date
  and geom is not null
  and uc_id is not null
group by file_date, uc_id;
