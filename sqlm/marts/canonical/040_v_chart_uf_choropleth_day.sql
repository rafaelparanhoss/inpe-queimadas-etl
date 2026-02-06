create schema if not exists marts;

create or replace view marts.v_chart_uf_choropleth_day as
select *
from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz;
