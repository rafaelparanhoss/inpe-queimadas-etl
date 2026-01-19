create schema if not exists marts;

do $$
declare
  obj_kind char;
begin
  select c.relkind
  into obj_kind
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'marts'
    and c.relname = 'focos_diario_uf_trend';

  if obj_kind = 'v' then
    execute 'drop view marts.focos_diario_uf_trend';
  elsif obj_kind is not null then
    execute 'drop table marts.focos_diario_uf_trend';
  end if;
end $$;

create or replace view marts.focos_diario_uf_trend as
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
