# INPE Queimadas - ETL (Brasil) -> PostGIS

ETL geoespacial para ingestao diaria de focos de queimadas (INPE CSV) com carga em PostGIS.
Objetivo: pipeline end-to-end + datasets canonicos para Superset.

## Stack
Python 3.11 | pandas | requests | psycopg | PostGIS | Docker | uv

## Camadas no banco
- raw.inpe_focos
- curated.inpe_focos
- curated.inpe_focos_enriched
- marts (tabelas/visoes para dashboard)

## Estrutura do projeto
- sql/ref, sql/enrich, sql/marts: pipeline end-to-end
- sql/minimal: portfolio core (views canonicas + mv_focos_day_dim)
- sql/checks: checks de qualidade
- dash/superset: scaffold local
- docs/superset_setup.md: wiring dos charts
- docs/archive: historico (nao usado pelo core)

## Quickstart (end-to-end)
```bash
uv sync
uv pip install -e .
uv run python -m etl.app run --date 2026-01-18 --checks
```

## Quickstart (portfolio minimal)
```bash
python -m etl.apply_portfolio --engine direct
```
Relatorios:
- docs/validation_last_run.md
- logs/last_run.json

## Aplicar SQL (manual)
```bash
python -m etl.apply_sql --dir sql/ref
python -m etl.apply_sql --dir sql/enrich --date 2026-01-18
python -m etl.apply_sql --dir sql/marts --date 2026-01-18
```

## Validar
```bash
python -m etl.validate_marts --apply-minimal --engine direct
```

## Datasets para Superset (10 charts finais)
- marts.mv_focos_day_dim (series + tops)
- marts.v_chart_focos_scatter
- marts.v_chart_uf_choropleth_day
- marts.v_chart_mun_choropleth_day

Guia completo: docs/superset_setup.md

## Superset (local)
```bash
cd dash/superset
docker compose up -d
```
Login: http://localhost:8088 (admin / admin)

## Config
Use .env.example como referencia.
Env vars principais:
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- INPE_MONTHLY_BASE_URL, INPE_RETENTION_DAYS
