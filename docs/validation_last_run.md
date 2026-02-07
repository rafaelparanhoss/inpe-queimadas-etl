# validation last run

timestamp_utc: 2026-02-07T00:07:44.371876Z

marts:
- applied: 12
- skipped_date: 3
- skipped_stub: 0
- failed: 0

checks:
- applied: 6
- skipped_date: 0
- skipped_stub: 0
- failed: 0

check_results:
- 010_superset_uf_choropleth.sql: ok
- 020_superset_mun_choropleth.sql: ok
- 030_superset_scatter.sql: ok
- 040_enriched_full_coverage.sql: ok
- 050_mv_focos_day_dim.sql: ok
- 060_mun_polycoords.sql: ok

last_day_counts:
- uf_day: 2026-02-05
- uf_rows: 27
- mun_day: 2026-02-05
- mun_features: 585
- scatter_day: 2026-02-05
- scatter_rows: 2253

status:
- ok: true
