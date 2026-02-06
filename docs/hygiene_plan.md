# hygiene plan (cleanup/hygiene)

## baseline
- root: `C:/Users/rafae/dev/github/inpe-queimadas-etl`
- branch: `cleanup/hygiene`
- single worktree expected

## linha reta oficial (end-to-end)
- comando: `python -m etl.app run --date YYYY-MM-DD`
- fluxo:
  1. `ensure_database`
  2. `run_ref` -> aplica `sql/ref/*.sql` + `ensure_ref_ibge`
  3. `etl.cli` -> extract/transform/load (`src/etl/extract`, `src/etl/transform`, `src/etl/load`)
  4. `run_enrich` -> aplica `sql/enrich/*.sql`
  5. `run_marts` -> aplica `sql/marts/10,11,20,21,30`
  6. modo dashboard (default): `validate_marts --apply-minimal` -> aplica `sqlm/ref_core`, `sql/enrich/20`, `sql/marts/{10,20}`, `sqlm/marts/{prereq,canonical}` + `sql/checks`

## entrypoints essenciais
- `src/etl/app.py` (comando principal)
- `src/etl/cli.py` (extract/transform/load)
- `src/etl/apply_sql.py`
- `src/etl/sql_runner.py`
- `src/etl/ref_runner.py`
- `src/etl/enrich_runner.py`
- `src/etl/marts_runner.py`
- `src/etl/validate_marts.py`
- `src/etl/validate_repo.py`
- `src/etl/db_bootstrap.py`
- `src/etl/ensure_ref_ibge.py`

## sql fonte de verdade (runtime)
- runtime end-to-end: `sql/ref`, `sql/enrich`, `sql/marts`
- runtime dashboard checks/canonicos: `sqlm/ref_core`, `sqlm/marts/{prereq,canonical}`, `sql/checks`
- nao-core no windows: `sqlm/marts/aux` (fora do preflight/core por bug historico de index no windows)

## keep (essencial)
- `src/etl/**` dos modulos listados acima
- `sql/ref/**`, `sql/enrich/**`, `sql/marts/**`, `sql/checks/**`
- `sqlm/ref_core/**`, `sqlm/marts/{prereq,canonical}/**`
- `docs/superset_setup.md`, `docs/inventory_superset_sql.md`, `docs/validation_last_run.md`
- `README.md`, `pyproject.toml`, `uv.lock`, `docker-compose.yml`, `.env.example`, `.gitignore`, `.gitattributes`

## remove_candidate (baixo risco)
- `notebooks/**` (ja ausente)
- `_quarantine/**` (ja ausente)
- `devtools/legacy/**` (ja ausente)
- docs redundantes nao usadas pelo runtime (movidas para `docs/archive/`)
- modulos de analytics/report/viz removidos do `etl.app` para manter o core linear

## etapa 3 (decisao sql)
- opcao aplicada: manter `sql/` como runtime do pipeline e `sqlm/` como canonico dashboard
- migracao completa para `sqlm/` fica como fase futura (refactor maior)

## aceite por etapa
1. `set PYTHONPATH=src` (ou equivalente)
2. `python -m etl.validate_repo`
3. `python -m etl.validate_marts --apply-minimal --dry-run --engine direct`
4. `git add -A --dry-run`
5. `python -m etl.app run --date 2026-02-05`
