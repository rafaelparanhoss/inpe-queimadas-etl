# inventory (superset sql)

## pipeline end-to-end
- sql/ref/**
- sql/enrich/**
- sql/marts/**

## minimal (portfolio core)
- sqlm/ref_core/00_build_ref_core.sql
- sqlm/ref_core/01_ref_schema.sql
- sqlm/ref_core/05_ref_uf_area.sql
- sqlm/ref_core/10_ref_geo_prepare.sql
- sql/enrich/20_enrich_municipio.sql
- sql/marts/10_focos_diario_municipio.sql
- sql/marts/20_focos_diario_uf.sql
- sqlm/marts/prereq/010_mv_uf_geom_mainland.sql
- sqlm/marts/prereq/020_mv_uf_mainland_poly_noholes.sql
- sqlm/marts/prereq/030_mv_uf_polycoords_polygon_superset.sql
- sqlm/marts/canonical/040_v_chart_uf_choropleth_day.sql
- sqlm/marts/canonical/050_v_chart_mun_choropleth_day.sql
- sqlm/marts/canonical/055_v_focos_enriched_full.sql
- sqlm/marts/canonical/060_v_chart_focos_scatter.sql
- sqlm/marts/canonical/065_mv_focos_day_dim.sql
- docs/migrations/sqlm_manifest.yml

## checks
- sql/checks/010_superset_uf_choropleth.sql
- sql/checks/020_superset_mun_choropleth.sql
- sql/checks/030_superset_scatter.sql
- sql/checks/040_enriched_full_coverage.sql
- sql/checks/050_mv_focos_day_dim.sql
- sql/checks/060_mun_polycoords.sql

## archive (historical, not applied)
- docs/archive/**
