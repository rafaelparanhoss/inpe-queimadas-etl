# inventory (superset sql)

## pipeline end-to-end
- sql/ref/**
- sql/enrich/**
- sql/marts/**

## minimal (portfolio core)
- sql/minimal/ref_core/00_build_ref_core.sql
- sql/minimal/ref_core/01_ref_schema.sql
- sql/minimal/ref_core/05_ref_uf_area.sql
- sql/minimal/ref_core/10_ref_geo_prepare.sql
- sql/minimal/enrich/20_enrich_municipio.sql
- sql/minimal/marts/core/10_focos_diario_municipio.sql
- sql/minimal/marts/core/20_focos_diario_uf.sql
- sql/minimal/marts/prereq/010_mv_uf_geom_mainland.sql
- sql/minimal/marts/prereq/020_mv_uf_mainland_poly_noholes.sql
- sql/minimal/marts/prereq/030_mv_uf_polycoords_polygon_superset.sql
- sql/minimal/marts/aux/031_v_geo_focos_diario_uf_poly_by_day_superset_full.sql
- sql/minimal/marts/aux/032_v_geo_focos_diario_uf_poly_by_day_superset_full_viz.sql
- sql/minimal/marts/aux/034_geo_focos_diario_municipio.sql
- sql/minimal/marts/canonical/040_v_chart_uf_choropleth_day.sql
- sql/minimal/marts/canonical/050_v_chart_mun_choropleth_day.sql
- sql/minimal/marts/canonical/055_v_focos_enriched_full.sql
- sql/minimal/marts/canonical/060_v_chart_focos_scatter.sql
- sql/minimal/marts/canonical/065_mv_focos_day_dim.sql
- sql/minimal/marts/canonical/070_v_geo_focos_diario_mun_poly_by_day_superset.sql

## checks
- sql/checks/010_superset_uf_choropleth.sql
- sql/checks/020_superset_mun_choropleth.sql
- sql/checks/030_superset_scatter.sql
- sql/checks/040_enriched_full_coverage.sql
- sql/checks/050_mv_focos_day_dim.sql
- sql/checks/060_mun_polycoords.sql

## archive (historical, not applied)
- docs/archive/**
