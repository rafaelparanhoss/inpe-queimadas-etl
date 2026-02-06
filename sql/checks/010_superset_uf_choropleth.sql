-- check A: 27 UFs in mv
DO $$
declare
  cnt int;
begin
  select count(*) into cnt from marts.mv_uf_polycoords_polygon_superset;
  if cnt <> 27 then
    raise exception 'check A failed: expected 27 ufs, got %', cnt;
  end if;
end $$;

-- check B: 27 UFs per day
DO $$
declare
  bad_day date;
  bad_count int;
begin
  select day, count(*)
  into bad_day, bad_count
  from marts.v_chart_uf_choropleth_day
  group by day
  having count(*) <> 27
  limit 1;

  if bad_day is not null then
    raise exception 'check B failed: day % has % rows', bad_day, bad_count;
  end if;
end $$;

-- check C: poly_coords not null
DO $$
declare
  n_null int;
begin
  select count(*) into n_null
  from marts.v_chart_uf_choropleth_day
  where poly_coords is null;

  if n_null <> 0 then
    raise exception 'check C failed: poly_coords null count=%', n_null;
  end if;
end $$;

-- check D: sum matches for latest day
DO $$
declare
  sum_tbl numeric;
  sum_view numeric;
  last_day date;
begin
  select max(day) into last_day from marts.focos_diario_uf;
  if last_day is null then
    raise exception 'check D failed: no day in marts.focos_diario_uf';
  end if;

  select sum(n_focos) into sum_tbl
  from marts.focos_diario_uf
  where day = last_day;

  select sum(n_focos) into sum_view
  from marts.v_chart_uf_choropleth_day
  where day = last_day;

  if coalesce(sum_tbl,0) <> coalesce(sum_view,0) then
    raise exception 'check D failed: sum_tbl=% sum_view=% day=%', sum_tbl, sum_view, last_day;
  end if;
end $$;
