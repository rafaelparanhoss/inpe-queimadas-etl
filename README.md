# INPE Queimadas ETL v1.x

ETL de focos INPE para Postgres/PostGIS + app (`api/` e `web/`) com range sempre `[from,to)` (to exclusivo).

## Pre-requisitos
- Python 3.11+
- Node 18+
- Docker + Docker Compose

## Bootstrap local (Windows)
```powershell
# 1) Banco local
docker compose up -d

# 2) Dependencias Python da ETL
uv sync
uv pip install -e .
```

## Ambiente (.env)
Crie `.env` na raiz com os dados do banco (exemplo minimo):
```dotenv
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=geoetl
DB_USER=geoetl
DB_PASSWORD=geoetl
DB_SSLMODE=prefer
```

## Rodar ETL (1 dia)
```powershell
python -m etl.app run --date YYYY-MM-DD --checks --engine direct --mode dashboard
```

## Rodar ETL por range completo ([from,to))
`To` e exclusivo. Exemplo de `2025-01-01` ate hoje:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_range.ps1 -From "2025-01-01" -To (Get-Date -Format "yyyy-MM-dd")
```

Comportamento de performance no range:
- cada dia roda apenas ETL diario + enrich + marts diarios (`etl.app run --mode full`);
- etapa pesada (`validate_marts --apply-minimal --dry-run`, sqlm/canonical/checks/report) roda 1x no final.
- para executar smoke no final do range, passe `-SmokeBaseUrl`:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_range.ps1 -From "2025-01-01" -To "2025-01-04" -SmokeBaseUrl "http://127.0.0.1:8000"
```

Regra de 1 dia:
- use `from=D` e `to=D+1`

## Validacoes
```powershell
python -m etl.validate_repo
python -m etl.validate_marts --apply-minimal --dry-run --engine direct
```

## API local
```powershell
cd api
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level info
```

## WEB local
```powershell
cd web
& "C:\Program Files\nodejs\npm.cmd" install
& "C:\Program Files\nodejs\npm.cmd" run dev
```

## Smoke
```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1 -BaseUrl "http://127.0.0.1:8000" -From "2025-08-01" -To "2025-09-01"
```

## Agendamento diario (Task Scheduler)
- Programa: `powershell.exe`
- Argumentos:
```powershell
-ExecutionPolicy Bypass -File "C:\Users\rafae\dev\github\inpe-queimadas-etl\scripts\run_daily.ps1"
```
