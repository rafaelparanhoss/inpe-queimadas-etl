create schema if not exists marts;

drop view if exists marts.v_chart_mun_choropleth_day;
create view marts.v_chart_mun_choropleth_day as
select
  day,
  cd_mun,
  mun_nm_mun,
  poly_coords,
  n_focos,
  n_focos_viz
from marts.v_geo_focos_diario_mun_poly_by_day_superset_full_viz;
