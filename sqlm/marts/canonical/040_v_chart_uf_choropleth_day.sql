create schema if not exists marts;

create or replace view marts.v_chart_uf_choropleth_day as
with days as (
  select distinct day
  from marts.focos_diario_uf
)
select
  d.day,
  u.uf,
  coalesce(f.n_focos, 0::bigint) as n_focos,
  case
    when coalesce(f.n_focos, 0::bigint) = 0 then 0.000001
    else coalesce(f.n_focos, 0::bigint)::numeric
  end as n_focos_viz,
  u.poly_coords
from days d
cross join marts.mv_uf_polycoords_polygon_superset u
left join marts.focos_diario_uf f
  on f.day = d.day
 and f.uf = u.uf;
