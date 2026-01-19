# INPE Queimadas - ETL diário (Brasil) -> PostGIS

ETL geoespacial para ingestão diária de focos de queimadas (INPE, CSV público) e carga em PostGIS, com camadas `raw/curated` para auditoria e análise.

## Stack
Python 3.11 | pandas | requests | psycopg | PostGIS | Docker | uv

## Decisões técnicas
- **`raw` vs `curated`**
  - `raw`: preserva o dado "como veio" (inclui `props` completo) para auditoria/reprocessamento.
  - `curated`: tabela enxuta, padronizada e indexada para consulta espacial.
- **`props` em `jsonb`**
  - evita migração de schema quando a fonte muda colunas;
  - mantém rastreabilidade e permite inspeção pontual.
- **Dedup por `event_hash`**
  - idempotência (rodar a mesma data não duplica);
  - `ON CONFLICT DO NOTHING`.
- **SRID 4326**
  - a fonte já vem em lat/lon (WGS84).

## Banco
Tabelas:
- `raw.inpe_focos`
- `curated.inpe_focos`
- `curated.inpe_focos_enriched`

Índices:
- GiST em `geom`
- B-tree em `file_date`

## Estrutura do projeto
- `sql/ref`: schemas e tabelas de referência
- `sql/enrich`: enriquecimento espacial
- `sql/marts`: tabelas analíticas
- `scripts`: automações de execução

## Como rodar

### 1) Subir PostGIS
```bash
docker compose up -d
```

### 2) Preparar ambiente Python
```bash
uv sync
uv pip install -e .
```

### 2.1) Comandos oficiais (python)
Os comandos python sao os oficiais. Os scripts em `scripts/*.sh` sao wrappers compat.
```bash
uv run python -m etl.app run --date 2026-01-18 --checks
uv run python -m etl.app report --date 2026-01-18
uv run python -m etl.app reprocess --date 2026-01-18
uv run python -m etl.app checks --date 2026-01-18
uv run python -m etl.app today
```

### 3) Rodar tudo (ref -> ingestao INPE -> enrich -> marts)
```bash
scripts/run_all.sh --date 2026-01-18
scripts/run_all.sh --date 2026-01-18 --checks
```

### 4) Rodar etapas isoladas
```bash
scripts/run_ref.sh
scripts/run_enrich.sh --date 2026-01-18
scripts/run_marts.sh --date 2026-01-18
```

### 5) Rebuild total (marts)
```bash
scripts/rebuild_marts.sh
```

### 6) Exemplo de validacoes
```bash
docker exec -it geoetl_postgis psql -U geoetl -d geoetl -c "select count(*) from curated.inpe_focos_enriched;"

docker exec -it geoetl_postgis psql -U geoetl -d geoetl -c "select count(*) - count(distinct event_hash) from curated.inpe_focos_enriched;"
```

### 7) Reprocessar um dia
```bash
scripts/reprocess_day.sh --date 2026-01-18
scripts/reprocess_day.sh --date 2026-01-18 --dry-run
```

### 8) Smoke checks
```bash
scripts/checks.sh
scripts/checks.sh --date 2026-01-18
```

### 9) OK esperado (reprocess)
raw_n=curated_n e marts_day_sum=curated_n



### 10) Rodar hoje
```bash
scripts/run_today.sh
scripts/run_today.sh --date 2026-01-18
```

### 11) Gerar report de um dia
```bash
scripts/report_day.sh --date 2026-01-18
```

### 12) GitHub Actions (artifacts)
- ver em: Actions -> workflow "daily" -> artifacts

## Logs
Arquivo gerado em `data/logs/etl.log`.

## Configuração
Arquivo `.env` (não versionado). Use `.env.example` como referência.
Para scripts, use `.env.local` (ver `.env.local.example`).

## Roadmap
- [ ] Rodar intervalo de datas (`--start-date/--end-date` ou `--backfill`)
- [ ] Incremental automático (último arquivo disponível)
- [ ] Evitar reprocessar datas já carregadas (`file_date`)
- [ ] Agregação por município (IBGE) e tabela diária `focos_por_municipio`
- [ ] Métricas de execução (tempo, inseridos, duplicados, descartados)
