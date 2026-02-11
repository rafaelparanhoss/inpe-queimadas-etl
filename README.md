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

## Agendamento diario local (Task Scheduler)
- Programa: `powershell.exe`
- Argumentos:
```powershell
-ExecutionPolicy Bypass -File "C:\Users\rafae\dev\github\inpe-queimadas-etl\scripts\run_daily.ps1"
```

## Deploy da API no Render (Blueprint)

1. Suba este repo no GitHub.
2. No Render: `New` -> `Blueprint`.
3. Selecione o repo e confira o service com:
   - `rootDir: api`
   - `buildCommand: pip install -r requirements.txt`
   - `startCommand: uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. No painel do service, configure as env vars minimas:
   - `PYTHON_VERSION` (`3.11.11`, recomendado)
   - `DB_HOST`
   - `DB_PORT`
   - `DB_NAME`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_SSLMODE` (`prefer` ou `require`)
   - `CORS_ORIGINS`
5. Deploy e teste:
   - `/health`
   - `/api/summary?from=2025-08-01&to=2025-09-01`
6. O banco precisa ser Postgres acessivel pela internet (nao `localhost`/Docker local da sua maquina).

`api/.env` e `web/.env.local` sao locais; nunca comitar segredos.
