-- 900_drop_legacy_exec.sql
-- drop plan (allowlist-based). run manually after verifying dependencies.
-- no cascade. views first, then materialized views/tables.

-- core allowlist (do not drop)
with core(schema_name, object_name) as (
  values
    ('curated','inpe_focos_enriched'),
    ('ref_core','ibge_municipios'),
    ('ref_core','ibge_municipios_web'),
    ('ref_core','ibge_ufs_web'),
    ('ref_core','ibge_uf_area'),
    ('ref_core','bioma'),
    ('ref_core','uc'),
    ('ref_core','ti'),
    ('marts','focos_diario_municipio'),
    ('marts','focos_diario_uf'),
    ('marts','mv_uf_geom_mainland'),
    ('marts','mv_uf_mainland_poly_noholes'),
    ('marts','mv_uf_polycoords_polygon_superset'),
    ('marts','v_geo_focos_diario_uf_poly_by_day_superset_full'),
    ('marts','v_geo_focos_diario_uf_poly_by_day_superset_full_viz'),
    ('marts','geo_focos_diario_municipio'),
    ('marts','v_geo_focos_diario_mun_poly_by_day_superset_full_viz'),
    ('marts','v_chart_uf_choropleth_day'),
    ('marts','v_chart_mun_choropleth_day'),
    ('marts','v_chart_focos_scatter'),
    ('marts','mv_focos_day_dim'),
    ('marts','v_focos_enriched_full')
),
objs as (
  select n.nspname as schema_name, c.relname as object_name, c.relkind
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname in ('marts','ref','ref_core','curated')
    and c.relkind in ('v','m','r')
)
select schema_name, object_name, relkind
from objs o
left join core c
  on c.schema_name = o.schema_name and c.object_name = o.object_name
where c.object_name is null
order by schema_name, object_name;

-- dependencies for legacy candidates (review before drop)
with legacy(name) as (
  values
    ('mv_dim_uf_geom_simpl'),
    ('mv_focos_uf_day'),
    ('v_geo_focos_diario_uf_simpl'),
    ('v_geo_uf_fc_by_day'),
    ('v_geo_uf_fc_by_day_ok'),
    ('v_geo_uf_fc_by_day_ok_txt'),
    ('v_geo_uf_fc_by_day_old3'),
    ('v_dim_uf_geom_noholes'),
    ('v_dim_uf_geom_raw'),
    ('geo_focos_diario_uf'),
    ('geo_focos_diario_uf_simpl'),
    ('v_geo_focos_diario_uf_poly_by_day_superset'),
    ('v_geo_focos_diario_uf_poly_by_day_superset_viz'),
    ('mv_uf_geom_mainland_fast'),
    ('mv_uf_geom_mainland_noholes'),
    ('mv_uf_poly_coords_mainland'),
    ('mv_uf_polycoords_superset'),
    ('v_geo_mun_fc_by_day')
)
select
  dependent_ns.nspname as dependent_schema,
  dependent_view.relname as dependent_object,
  source_ns.nspname as source_schema,
  source.relname as source_object
from pg_depend
join pg_rewrite on pg_depend.objid = pg_rewrite.oid
join pg_class dependent_view on pg_rewrite.ev_class = dependent_view.oid
join pg_class source on pg_depend.refobjid = source.oid
join pg_namespace dependent_ns on dependent_view.relnamespace = dependent_ns.oid
join pg_namespace source_ns on source.relnamespace = source_ns.oid
where source_ns.nspname = 'marts'
  and source.relname in (select name from legacy)
order by 1,2,3,4;

-- drop views (uncomment after review)
-- drop view if exists marts.v_geo_focos_diario_uf_simpl;
-- drop view if exists marts.v_geo_uf_fc_by_day;
-- drop view if exists marts.v_geo_uf_fc_by_day_ok;
-- drop view if exists marts.v_geo_uf_fc_by_day_ok_txt;
-- drop view if exists marts.v_geo_uf_fc_by_day_old3;
-- drop view if exists marts.v_dim_uf_geom_noholes;
-- drop view if exists marts.v_dim_uf_geom_raw;
-- drop view if exists marts.geo_focos_diario_uf;
-- drop view if exists marts.geo_focos_diario_uf_simpl;
-- drop view if exists marts.v_geo_focos_diario_uf_poly_by_day_superset;
-- drop view if exists marts.v_geo_focos_diario_uf_poly_by_day_superset_viz;
-- drop view if exists marts.v_geo_mun_fc_by_day;

-- drop materialized views / tables (uncomment after review)
-- drop materialized view if exists marts.mv_dim_uf_geom_simpl;
-- drop materialized view if exists marts.mv_focos_uf_day;
-- drop materialized view if exists marts.mv_uf_geom_mainland_fast;
-- drop materialized view if exists marts.mv_uf_geom_mainland_noholes;
-- drop materialized view if exists marts.mv_uf_poly_coords_mainland;
-- drop materialized view if exists marts.mv_uf_polycoords_superset;
