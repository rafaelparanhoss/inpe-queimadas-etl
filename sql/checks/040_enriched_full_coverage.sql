-- check enriched full coverage (last day + rolling window)
DO $$
declare
  last_day date;
  n_total int;
  pct_bioma numeric;
  pct_mun numeric;
  n_any_ti_uc int;
begin
  select max(day) into last_day from marts.v_focos_enriched_full;
  if last_day is null then
    raise exception 'check enriched_full failed: no day in v_focos_enriched_full';
  end if;

  select count(*) into n_total
  from marts.v_focos_enriched_full
  where day = last_day;

  if n_total is null or n_total = 0 then
    raise exception 'check enriched_full failed: no rows for day=%', last_day;
  end if;

  select round(
    1.0 * count(*) filter (where cd_bioma is not null) / nullif(count(*), 0),
    4
  ) into pct_bioma
  from marts.v_focos_enriched_full
  where day = last_day;

  if pct_bioma < 0.90 then
    raise exception 'check enriched_full failed: pct_bioma=% for day=%', pct_bioma, last_day;
  end if;

  select round(
    1.0 * count(*) filter (where cd_mun is not null and uf is not null) / nullif(count(*), 0),
    4
  ) into pct_mun
  from marts.v_focos_enriched_full
  where day = last_day;

  if pct_mun < 0.99 then
    raise exception 'check enriched_full failed: pct_mun_uf=% for day=%', pct_mun, last_day;
  end if;

  -- tolerate days with no TI/UC, but fail if none in the last 30 days
  select count(*) into n_any_ti_uc
  from marts.v_focos_enriched_full
  where day >= (last_day - interval '30 days')
    and (uc_id is not null or ti_id is not null);

  if n_any_ti_uc = 0 then
    raise exception 'check enriched_full failed: no ti/uc rows in last 30 days (ending %)', last_day;
  end if;
end $$;
