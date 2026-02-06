# INPE Queimadas ETL

Pipeline end-to-end para focos de queimadas (INPE), com carga em Postgres/PostGIS e datasets canonicos para Superset.

## Core
- `python -m etl.app run --date YYYY-MM-DD`
- `python -m etl.validate_repo`
- `python -m etl.validate_marts --apply-minimal --dry-run --engine direct`

## Setup minimo
```bash
uv sync
uv pip install -e .
```

Use `.env.example` como referencia.
Variaveis principais:
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## Execucao end-to-end
```bash
python -m etl.app run --date 2026-02-05 --checks --engine direct
```

## Validacao
```bash
python -m etl.validate_repo
python -m etl.validate_marts --apply-minimal --dry-run --engine direct
```

## Estrutura SQL
- `sql/ref`, `sql/enrich`, `sql/marts`: runtime do pipeline
- `sqlm/ref_core`, `sqlm/marts/{prereq,canonical}`: camada canonica para dashboard
- `sql/checks`: checks de qualidade

## Objetos canonicos do Superset
- `marts.mv_focos_day_dim`
- `marts.v_chart_focos_scatter`
- `marts.v_chart_uf_choropleth_day`
- `marts.v_chart_mun_choropleth_day`

Guia de wiring: `docs/superset_setup.md`
Inventario SQL: `docs/inventory_superset_sql.md`
Relatorio de validacao: `docs/validation_last_run.md`

## Windows hardening (git)
```powershell
git config --global core.longpaths true
git config --global core.protectNTFS true
git config --global core.precomposeunicode true
git config --global core.autocrlf input
```

Se `git add` falhar com lock/permissao:
```powershell
powershell -ExecutionPolicy Bypass -File devtools/win_git_diag.ps1
powershell -ExecutionPolicy Bypass -File devtools/win_git_index_recover.ps1
```
