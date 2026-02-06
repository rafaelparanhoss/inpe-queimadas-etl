-- check mun polygon dataset
DO $$
declare
  last_day date;
  n_null int;
  n_mun int;
begin
  select max(day) into last_day
  from marts.v_chart_mun_choropleth_day;

  if last_day is null then
    raise exception 'check mun poly failed: no day in dataset';
  end if;

  select count(*) into n_null
  from marts.v_chart_mun_choropleth_day
  where day = last_day and poly_coords is null;

  if n_null > 0 then
    raise exception 'check mun poly failed: poly_coords nulls=% for day=%', n_null, last_day;
  end if;

  select count(distinct cd_mun) into n_mun
  from marts.v_chart_mun_choropleth_day
  where day = last_day;

  if n_mun = 0 then
    raise exception 'check mun poly failed: zero municipios for day=%', last_day;
  end if;
end $$;
