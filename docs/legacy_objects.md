# Legacy objects

Este arquivo lista objetos legados e o core canonico. Remover somente depois de:
- checks em sql/checks ok
- Superset apontando para v_chart_* (UF/MUN/scatter)
- dry-run do drop (sql/legacy/900_drop_legacy_exec.sql) sem dependencias externas

## Core canonico (nao remover)
- curated.inpe_focos_enriched
- ref_core.ibge_municipios_web
- ref_core.ibge_ufs_web
- ref_core.ibge_uf_area
- ref_core.bioma
- ref_core.uc
- ref_core.ti
- marts.focos_diario_municipio
- marts.focos_diario_uf
- marts.mv_uf_geom_mainland
- marts.mv_uf_mainland_poly_noholes
- marts.mv_uf_polycoords_polygon_superset
- marts.v_geo_focos_diario_uf_poly_by_day_superset_full
- marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz
- marts.geo_focos_diario_municipio
- marts.v_geo_focos_diario_mun_poly_by_day_superset_full_viz
- marts.v_chart_uf_choropleth_day
- marts.v_chart_mun_choropleth_day
- marts.v_chart_focos_scatter
- marts.mv_focos_day_dim
- marts.v_focos_enriched_full

## Legados (remover quando nenhum chart usar)
- marts.mv_dim_uf_geom_simpl
- marts.mv_focos_uf_day
- marts.v_geo_focos_diario_uf_simpl
- marts.v_geo_uf_fc_by_day*
- marts.v_dim_uf_geom_raw
- marts.v_dim_uf_geom_noholes
- marts.geo_focos_diario_uf
- marts.geo_focos_diario_uf_simpl
- marts.mv_uf_geom_mainland_fast
- marts.mv_uf_geom_mainland_noholes
- marts.mv_uf_poly_coords_mainland
- marts.mv_uf_polycoords_superset
- marts.v_geo_focos_diario_uf_poly_by_day_superset
- marts.v_geo_focos_diario_uf_poly_by_day_superset_viz
- marts.v_geo_mun_fc_by_day (antigo GeoJSON por dia)

## Como remover
1) Atualizar datasets no Superset para v_chart_* e mv_focos_day_dim
2) Rodar o plano de drop (dry-run): sql/legacy/900_drop_legacy_exec.sql
3) Se nao houver dependencias externas, executar os DROPs comentados
