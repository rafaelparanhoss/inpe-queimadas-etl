# INPE Queimadas ETL v1.x

ETL de focos INPE para Postgres/PostGIS + app operacional (`api/` + `web/`).

## Comandos essenciais
```powershell
# ETL (1 dia)
python -m etl.app run --date YYYY-MM-DD --checks --engine direct --mode dashboard

# Validacoes
python -m etl.validate_repo
python -m etl.validate_marts --apply-minimal --dry-run --engine direct

# API
cd api
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level info

# WEB
cd web
npm run dev

# Smoke
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1 -BaseUrl "http://127.0.0.1:8000"
```

## Regra de range
- Todos os endpoints usam `[from,to)` (to exclusivo).
- Para 1 dia: `from=D` e `to=D+1`.

Runbook operacional: `docs/runbook.md`.
