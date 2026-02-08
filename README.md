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

## App (MVP Leaflet + FastAPI)

Este repositório contém a ETL (existente) e um app MVP separado em `api/` (FastAPI) e `web/` (Vite + Leaflet + Chart.js).
O MVP usa um estado global de filtros `{from,to,uf}` e todos os componentes obedecem ao mesmo range (to exclusivo `[from,to)`).

### Requisitos
- Python 3.10+ (recomendado 3.11)
- Node 18+ (ou 20+)

### API (Windows)
```powershell
cd api
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
# edite api\.env com credenciais do Postgres local/remoto
uvicorn app.main:app --reload --port 8000
```

Endpoints:

`http://localhost:8000/health`

`http://localhost:8000/api/choropleth/uf?from=YYYY-MM-DD&to=YYYY-MM-DD`

`http://localhost:8000/api/timeseries/total?from=YYYY-MM-DD&to=YYYY-MM-DD&uf=MT`

`http://localhost:8000/api/top?group=uf&from=YYYY-MM-DD&to=YYYY-MM-DD&limit=10`

`http://localhost:8000/api/totals?from=YYYY-MM-DD&to=YYYY-MM-DD&uf=MT`

### WEB (Windows)
```powershell
cd web
npm install
# opcional: apontar para API diferente
# set VITE_API_BASE=http://localhost:8000
npm run dev
```

Abrir: `http://localhost:5173`

### Variáveis de ambiente
API (`api/.env`):

`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SSLMODE`

`CORS_ORIGINS` (ex: `http://localhost:5173`)

`CACHE_TTL_SECONDS` (ex: `300`)

`LOG_LEVEL` (ex: `INFO`)

WEB (env do Vite):

`VITE_API_BASE` (ex: `http://localhost:8000`)

### Checks rápidos (curl)
Use datas reais. to é exclusivo.

```bash
curl "http://localhost:8000/health"
curl "http://localhost:8000/api/choropleth/uf?from=2026-01-01&to=2026-02-01"
curl "http://localhost:8000/api/top?group=uf&from=2026-01-01&to=2026-02-01&limit=10"
curl "http://localhost:8000/api/timeseries/total?from=2026-01-01&to=2026-02-01"
curl "http://localhost:8000/api/totals?from=2026-01-01&to=2026-02-01"
curl "http://localhost:8000/api/timeseries/total?from=2026-01-01&to=2026-02-01&uf=MT"
curl "http://localhost:8000/api/totals?from=2026-01-01&to=2026-02-01&uf=MT"
```

### Fase 1 (agregados + crossfilter)

Nesta fase o estado global de filtros passou a ser:
`{from,to,uf,bioma,mun,uc,ti}` com `to` exclusivo (`[from,to)`).

Todos os endpoints agregados aceitam filtros opcionais:
- `uf`
- `bioma`
- `mun`
- `uc`
- `ti`

Endpoints novos:
- `GET /api/summary?from&to&uf&bioma&mun&uc&ti`
- `GET /api/validate?from&to&uf&bioma&mun&uc&ti` (QA opcional)

`/api/top` agora aceita:
- `group=uf|bioma|mun|uc|ti`
- resposta: `{"group","items":[{"key","label","n_focos"}],"note"}`  
  para `group=mun` sem `uf`, a API aplica guardrail de limite menor e retorna `note`.

Comandos de teste rapido (Windows):

```powershell
cd api
& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level info
```

```powershell
curl.exe -s "http://127.0.0.1:8000/api/summary?from=2026-01-01&to=2026-02-01"
curl.exe -s "http://127.0.0.1:8000/api/top?group=bioma&from=2026-01-01&to=2026-02-01&limit=10"
curl.exe -s "http://127.0.0.1:8000/api/top?group=mun&uf=MT&from=2026-01-01&to=2026-02-01&limit=10"
curl.exe -s "http://127.0.0.1:8000/api/validate?from=2026-01-01&to=2026-02-01"
```

```powershell
cd web
& "C:\Program Files\nodejs\npm.cmd" run dev
```
