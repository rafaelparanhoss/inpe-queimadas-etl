-- check mun choropleth (last day)
DO $$
declare
  last_day date;
  n_mun int;
  n_null int;
  n_viz int;
begin
  select max(day) into last_day from marts.focos_diario_municipio;
  if last_day is null then
    raise exception 'check mun failed: no day in marts.focos_diario_municipio';
  end if;

  select count(distinct cd_mun) into n_mun
  from marts.v_chart_mun_choropleth_day
  where day = last_day;

  if n_mun is null or n_mun < 1 then
    raise exception 'check mun failed: cd_mun=% for day=%', n_mun, last_day;
  end if;

  select count(*) into n_null
  from marts.v_chart_mun_choropleth_day
  where day = last_day and poly_coords is null;

  if n_null > 0 then
    raise exception 'check mun failed: poly_coords nulls=% for day=%', n_null, last_day;
  end if;

  select count(*) into n_viz
  from marts.v_chart_mun_choropleth_day
  where day = last_day and n_focos_viz is not null;

  if n_viz < 1 then
    raise exception 'check mun failed: n_focos_viz missing for day=%', last_day;
  end if;
end $$;
