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

## Documentacao minima mantida
- `README.md` (runbook principal)
- `docs/scatter.md` (semantica e UX do scatter)
- `docs/geo_sources_report.md` (auditoria das fontes geo)
- `docs/sql/geo_sources_apply.sql` (views geo base)
- `docs/sql/geo_sources_apply_fix_ti_uc.sql` (ajustes canonicos TI/UC)
- `docs/superset_setup.md` (wiring de datasets)
- `docs/inventory_superset_sql.md` (inventario SQL do portfolio)
- `docs/validation_last_run.md` (resultado da ultima validacao)

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

### Fase 1.5 (Polish + Navegacao Espacial)

Novidades desta fase:
- UI em tema escuro, labels em Title Case e melhor legibilidade.
- Choropleth de UF com legenda e bins dinamicos (escala quantile).
- Zoom espacial por ranking via `GET /api/bounds`.
- Camada municipal opcional via `GET /api/choropleth/mun`, com guardrails de performance.

#### Endpoints novos

- `GET /api/bounds?entity=uf|mun|bioma|uc|ti&key=...&uf=...`
  - Retorna: `{"entity","key","bbox":[minLng,minLat,maxLng,maxLat],"center":[lat,lng]}`
  - Quando fonte de geometria nao estiver configurada: `404 geometry source not configured`.
- `GET /api/choropleth/mun?from=YYYY-MM-DD&to=YYYY-MM-DD&uf=XX`
  - Exige `uf`.
  - `to` exclusivo (`[from,to)`).
  - Se range exceder `CHORO_MAX_DAYS_MUN` (default `180`), retorna `400`.
  - Se fonte municipal nao estiver configurada, retorna `501 geometry source not configured`.
- `GET /api/lookup/mun?key=<cd_mun>`
  - Resolve municipio para crossfilter: `{"mun","mun_nome","uf","uf_nome"}`.
  - Usado no clique do ranking municipal para garantir estado valido (`mun` + `uf`) e habilitar camada municipal.

#### Legenda dinamica (breaks)

Os endpoints de choropleth (`/api/choropleth/uf` e `/api/choropleth/mun`) retornam metadados para simbologia:

- `breaks`: limites de classe calculados no backend
- `domain`: `[min,max]` dos valores no payload
- `method`: `quantile` (fallback `equal` quando os valores nao permitem quantis estritamente crescentes)
- `unit`: `focos`
- `zero_class`: `true|false` (quando zero fica em classe separada)
- `palette`: cores utilizadas no mapa

Metodo padrao:
- quantis com `k=5` classes
- quando ha valores zero e valores positivos, zero pode ser tratado como classe separada (`zero_class=true`) e os quantis sao calculados sobre valores `> 0`
- os breaks enviados para o front sao monotonicos e usados no renderer como intervalos `[b[i], b[i+1])` (ultimo intervalo inclusivo no limite superior)

#### Variaveis GEO_* (api/.env)

Padrao recomendado (ja configurado em `api/.env.example`):

- `GEO_UF_TABLE=public.geo_uf`
- `GEO_UF_KEY_COL=uf`
- `GEO_UF_GEOM_COL=geom`
- `GEO_MUN_TABLE=public.geo_mun`
- `GEO_MUN_KEY_COL=cd_mun`
- `GEO_MUN_UF_COL=uf`
- `GEO_MUN_GEOM_COL=geom`
- `GEO_BIOMA_TABLE=public.geo_bioma`
- `GEO_BIOMA_KEY_COL=key`
- `GEO_BIOMA_GEOM_COL=geom`
- `GEO_UC_TABLE=public.geo_uc`
- `GEO_UC_KEY_COL=key`
- `GEO_UC_GEOM_COL=geom`
- `GEO_TI_TABLE=public.geo_ti`
- `GEO_TI_KEY_COL=key`
- `GEO_TI_GEOM_COL=geom`
- `CHORO_MAX_DAYS_MUN=180`
- `CHORO_SIMPLIFY_TOL=0.01`

Aplicar views de geometria:

```powershell
docker exec -i geoetl_postgis psql -U geoetl -d geoetl -f /work/docs/sql/geo_sources_apply.sql
```

Arquivos SQL:
- `docs/sql/geo_sources_apply.sql` (views usadas pela API)
- `docs/sql/geo_sources_template.sql` (template para adaptar a outras fontes)

Como descobrir tabelas/colunas de geometria no PostGIS:
```sql
select f_table_schema, f_table_name, f_geometry_column
from geometry_columns
order by 1,2;
```

#### Testes rapidos (Windows)

Subir API:
```powershell
cd api
& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --log-level info
```

Smoke tests:
```powershell
curl.exe -s "http://127.0.0.1:8001/health"
curl.exe -s "http://127.0.0.1:8001/api/choropleth/uf?from=2025-08-01&to=2025-09-01"
curl.exe -s "http://127.0.0.1:8001/api/bounds?entity=uf&key=MT"
curl.exe -s "http://127.0.0.1:8001/api/bounds?entity=mun&key=5103254"
curl.exe -s "http://127.0.0.1:8001/api/bounds?entity=bioma&key=1"
curl.exe -s "http://127.0.0.1:8001/api/choropleth/mun?from=2025-08-01&to=2025-09-01&uf=MT"
curl.exe -s "http://127.0.0.1:8001/api/lookup/mun?key=5103254"
curl.exe -s "http://127.0.0.1:8001/api/validate?from=2025-08-01&to=2025-09-01"
```

Validar metadados de legenda (PowerShell):
```powershell
$uf = curl.exe -s "http://127.0.0.1:8001/api/choropleth/uf?from=2025-08-01&to=2025-09-01" | ConvertFrom-Json
$uf | Select-Object breaks, domain, method, unit, zero_class

$mun = curl.exe -s "http://127.0.0.1:8001/api/choropleth/mun?from=2025-08-01&to=2025-09-01&uf=MT" | ConvertFrom-Json
$mun | Select-Object breaks, domain, method, unit, zero_class

$qa = curl.exe -s "http://127.0.0.1:8001/api/validate?from=2025-08-01&to=2025-09-01" | ConvertFrom-Json
$qa | Select-Object consistent, invalid_filter_state, break_monotonicity_ok
```

Subir web:
```powershell
cd web
set VITE_API_BASE=http://127.0.0.1:8001
& "C:\Program Files\nodejs\npm.cmd" run dev
```

#### Checklist rapido

- Legenda do mapa aparece e muda com o range de datas.
- Bins do choropleth UF mudam quando o periodo muda.
- Clique em ranking aplica filtro e executa fit bounds.
- Clique em ranking municipal seta `mun` e `uf`, habilita camada municipal e faz fit no municipio.
- Se bounds do item falhar, o zoom cai para UF selecionada e depois para Brasil.
- Toggle municipal so habilita com UF selecionada.
- Sem configuracao GEO_MUN_*, a UI continua com camada UF (fallback sem travar).

### Fase 1.7 (Simbologia Fire + Dropdown UF + Zoom UC/TI)

Nesta fase:
- o choropleth UF/municipal usa paleta "fire" (amarelo -> laranja -> vermelho -> roxo escuro);
- os breaks continuam dinamicos no backend (quantile com fallback equal), e a legenda Leaflet usa o metadata do layer ativo;
- foi adicionado dropdown de UF no painel;
- qualquer selecao de UF (dropdown, mapa, ranking) liga automaticamente a camada municipal;
- ao limpar UF (opcao "Todas"), a camada municipal e desligada e o mapa volta ao Brasil;
- cliques em ranking de UC/TI usam `/api/bounds?entity=uc|ti&key=...` para fit bounds do poligono.

#### Curls adicionais

```powershell
curl.exe -s "http://127.0.0.1:8001/api/bounds?entity=uc&key=0000.00.0001"
curl.exe -s "http://127.0.0.1:8001/api/bounds?entity=ti&key=10001"
```

Conferir metadata da paleta/breaks:

```powershell
$uf = curl.exe -s "http://127.0.0.1:8001/api/choropleth/uf?from=2025-08-01&to=2025-09-01" | ConvertFrom-Json
$uf | Select-Object method, unit, domain, zero_class, breaks, palette

$mun = curl.exe -s "http://127.0.0.1:8001/api/choropleth/mun?from=2025-08-01&to=2025-09-01&uf=MT" | ConvertFrom-Json
$mun | Select-Object method, unit, domain, zero_class, breaks, palette
```

Checklist visual:
- selecione UF pelo dropdown e confirme que a camada municipal liga automaticamente;
- clique em item de TI/UC e confirme zoom no poligono (fallback para UF/Brasil se bounds indisponivel);
- alterne entre layer UF e municipal e confirme que a legenda muda sem faixas repetidas.

### Fase 1.9 (Overlay UC/TI)

Nesta fase foi adicionado:
- `GET /api/geo?entity=uc|ti&key=<id>` para retornar GeoJSON simplificado do poligono selecionado;
- overlay visual no mapa para UC/TI ativa (contorno + preenchimento leve), mantendo o choropleth UF/municipal como camada base;
- zoom preferencial no overlay, com fallback para `/api/bounds` se `/api/geo` falhar.

#### Endpoint `/api/geo`

Retorno:
- `entity`
- `key`
- `geojson` (`FeatureCollection` com 1 `Feature`)
  - `properties`: `entity`, `key`, `label`, `n_focos`
  - `geometry`: simplificada via `st_simplifypreservetopology(..., CHORO_SIMPLIFY_TOL)`

Erros:
- `404` quando `key` nao existe (ou fonte nao configurada)
- `422` quando a geometria encontrada e nula

Smoke (Windows):

```powershell
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=uc&key=0000.00.0001"
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=ti&key=10001"
```

Validacao visual:
- clique em Top UCs: aparece overlay da UC e zoom no poligono;
- clique em Top TIs: aparece overlay da TI e zoom no poligono;
- limpar filtros: overlay some.

### Fase 2.0 (Timeseries legivel + Range 365d + Overlay UC/TI robusto)

Melhorias aplicadas:
- eixo X da serie temporal com reducao automatica de ticks:
  - `n<=14`: mostra todos
  - `15..45`: ~9 ticks
  - `>45`: maximo 10 ticks
- labels do eixo em formato curto (`MM-DD` para dia/semana) e tooltip com data completa;
- range global permitido ate `365` dias (`APP_MAX_RANGE_DAYS`, default `365`);
- `GET /api/timeseries/total` passa a retornar `granularity`:
  - `day` ate `92` dias
  - `week` entre `93` e `273`
  - `month` acima de `273`
- `/api/geo` com geometria valida e simplificacao em metros (via 3857), com tolerancia adaptativa por area;
- endpoint QA opcional: `GET /api/geo/qa?entity=uc|ti&key=...`.

#### Curls Fase 2.0

Timeseries 365 dias (verificar `granularity`):
```powershell
curl.exe -s "http://127.0.0.1:8001/api/timeseries/total?from=2025-01-01&to=2026-01-01"
```

Geo overlay UC/TI:
```powershell
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=uc&key=0000.00.0001"
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=uc&key=<outra_uc_existente>"
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=ti&key=10001"
curl.exe -s "http://127.0.0.1:8001/api/geo?entity=ti&key=<outra_ti_existente>"
```

QA de geometria (npoints/area/validade/bbox):
```powershell
curl.exe -s "http://127.0.0.1:8001/api/geo/qa?entity=uc&key=0000.00.0001"
curl.exe -s "http://127.0.0.1:8001/api/geo/qa?entity=ti&key=10001"
```

Checklist visual:
- serie de 31 dias fica legivel no eixo X (sem sobreposicao total);
- range de 2 a 12 meses nao bloqueia (ate 365 dias);
- UC/TI desenha poligono coerente (sem encolhimento severo) e zoom cobre o shape.

## v1.0 Checkpoint and Runbook (Windows)

This repository was checkpointed at tag `v1.0` and branch `release/v1.0`.

### Run from zero

Terminal 1 (API):

```powershell
cd api
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
# adjust DB_* and GEO_* in api\.env if needed
& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level info
```

Terminal 2 (WEB):

```powershell
cd web
& "C:\Program Files\nodejs\npm.cmd" install
$env:VITE_API_BASE = "http://127.0.0.1:8000"
& "C:\Program Files\nodejs\npm.cmd" run dev
```

If PowerShell blocks `npm.ps1`, run either:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
npm run dev
```

or keep using `npm.cmd` as shown above.

### Smoke script

With API running, execute:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1 -BaseUrl "http://127.0.0.1:8000"
```

The script fails immediately when any expected endpoint returns status different from `200`.
It checks:
- `/health`
- `/api/validate`
- `/api/choropleth/uf`
- `/api/choropleth/mun`
- `/api/bounds` for `uf|uc|ti`
- `/api/geo` for `uc|ti`

### Fase 2.2 (Mapa Scatter com Guardrails)

Novo endpoint:
- `GET /api/points?date=YYYY-MM-DD&bbox=minLon,minLat,maxLon,maxLat&limit=20000&uf=&bioma=&mun=&uc=&ti=`

Contrato de resposta:
- `date`, `bbox`, `returned`, `limit`, `truncated`, `points[{lon,lat,n}]`

Guardrails:
- `date` e `bbox` obrigatorios.
- `limit` default `20000` com hard cap `50000`.
- Se o backend encontrar mais de `limit`, retorna apenas `limit` e `truncated=true`.
- Cache curto por URL+bucket de zoom (`POINTS_CACHE_TTL_SECONDS`, default `30s`).
- Em `/api/validate`, o backend agora informa `points_date_used` e `points_returned` no smoke interno de pontos.

Exemplos curl (Windows):

```powershell
curl.exe -s "http://127.0.0.1:8000/api/points?date=2025-08-01&bbox=-61.0,-16.5,-55.0,-10.0&limit=5000"
curl.exe -s "http://127.0.0.1:8000/api/points?date=2025-08-01&bbox=-74,-34,-34,6&limit=20000"
curl.exe -s "http://127.0.0.1:8000/api/validate?from=2025-08-01&to=2025-09-01"
```

Validacao:
- `scripts/smoke.ps1` agora testa `/api/points` e falha se `returned > limit`.
- `scripts/smoke.ps1` tambem usa `peak_day` de `/api/summary?uf=RS` para validar pontos no dia de pico.
- No frontend, toggle **Pontos (MVP)**:
  - recarrega em `moveend/zoomend` com debounce,
  - usa clustering no cliente,
  - mostra badge de truncamento quando `truncated=true`,
  - permite escolher o dia dos pontos via seletor `peak_day | from | custom`.
  - quando `to-from == 1 dia`, o modo fica travado em `from`.
