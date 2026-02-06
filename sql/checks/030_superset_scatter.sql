-- check scatter (last day)
DO $$
declare
  last_day date;
  n_total int;
  n_bad int;
begin
  select max(file_date) into last_day from curated.inpe_focos_enriched;
  if last_day is null then
    raise exception 'check scatter failed: no day in curated.inpe_focos_enriched';
  end if;

  select count(*) into n_total
  from marts.v_chart_focos_scatter
  where file_date = last_day;

  if n_total is null or n_total = 0 then
    raise exception 'check scatter failed: no rows for day=%', last_day;
  end if;

  select count(*) into n_bad
  from marts.v_chart_focos_scatter
  where file_date = last_day
    and (lat is null or lon is null or geom is null);

  if n_bad <> 0 then
    raise exception 'check scatter failed: null coords=% for day=%', n_bad, last_day;
  end if;
end $$;
