# validation last run

timestamp_utc: 2026-02-06T20:03:03.023428Z

marts:
- applied: 0
- skipped_date: 3
- skipped_stub: 0
- failed: 0

checks:
- applied: 0
- skipped_date: 0
- skipped_stub: 0
- failed: 0

check_results:
- 010_superset_uf_choropleth.sql: ok | dry-run
- 020_superset_mun_choropleth.sql: ok | dry-run
- 030_superset_scatter.sql: ok | dry-run
- 040_enriched_full_coverage.sql: ok | dry-run
- 050_mv_focos_day_dim.sql: ok | dry-run
- 060_mun_polycoords.sql: ok | dry-run

last_day_counts:
- uf_day: None
- uf_rows: None
- mun_day: None
- mun_features: None
- scatter_day: None
- scatter_rows: None

status:
- ok: true

TODO: restore/regen sqlm/marts/aux/031_uf_poly_day_full.sql (was blocking git add on Windows)
