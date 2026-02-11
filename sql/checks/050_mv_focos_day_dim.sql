-- check mv_focos_day_dim coverage (last 30 days)
DO $$
declare
  last_day date;
  start_day date;
  n_days int;
  n_rows int;
  n_focos bigint;
begin
  select max(day) into last_day from marts.mv_focos_day_dim;
  if last_day is null then
    raise exception 'check mv_focos_day_dim failed: no rows';
  end if;

  start_day := last_day - interval '30 days';

  select count(distinct day) into n_days
  from marts.mv_focos_day_dim
  where day >= start_day;

  if n_days = 0 then
    raise exception 'check mv_focos_day_dim failed: no days in last 30 days';
  end if;

  select count(*), coalesce(sum(m.n_focos),0) into n_rows, n_focos
  from marts.mv_focos_day_dim m
  where day >= start_day;

  if n_rows = 0 or n_focos = 0 then
    raise exception 'check mv_focos_day_dim failed: empty rows or n_focos=0 in last 30 days';
  end if;
end $$;
