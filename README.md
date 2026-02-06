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
- sqlm: portfolio core (views canonicas + mv_focos_day_dim)
  - docs/migrations/sqlm_manifest.yml: mapeamento old_path -> new_path (rastreamento de renomeacao)
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
python -m etl.validate_repo
```

## Windows hardening (git)
Checklist recomendado no Windows:
```powershell
git config --global core.longpaths true
git config --global core.protectNTFS true
git config --global core.precomposeunicode true
git config --global core.autocrlf input
```
Observacao:
- `.gitattributes` forca `LF` para `*.py`, `*.sql` e `*.md`.
- no Git Bash, se houver conversao indevida de path, use `MSYS2_ARG_CONV_EXCL="*"` e `MSYS_NO_PATHCONV=1`.
- se `git add -A` falhar em arquivos SQL, rode:
```powershell
powershell -ExecutionPolicy Bypass -File devtools/win_fix_index.ps1
```
- repair definitivo de index no Git Bash:
```bash
bash devtools/win_git_diag.sh
bash devtools/win_git_repair.sh
```
- diagnostico equivalente via PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File devtools/win_git_diag.ps1
```
- se falhar com `index.lock` no PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File devtools/win_git_index_recover.ps1
# so remova lock quando nao houver processo git ativo:
powershell -ExecutionPolicy Bypass -File devtools/win_git_index_recover.ps1 -ForceUnlock
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


