# INPE Queimadas — ETL diário (Brasil) → PostGIS

ETL geoespacial para ingestão diária de focos de queimadas (INPE, CSV público) e carga em PostGIS, com camadas `raw/curated` para auditoria e análise.

## Stack
Python 3.11 • pandas • requests • psycopg • PostGIS • Docker • uv

## Decisões técnicas
- **`raw` vs `curated`**
  - `raw`: preserva o dado “como veio” (inclui `props` completo) para auditoria/reprocessamento.
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

Índices:
- GiST em `geom`
- B-tree em `file_date`

## Como rodar

### 1) Subir PostGIS
```bash
docker compose up -d
```

### 2) Preparar ambiente Python
```bash
python -m uv sync
```

### 3) Rodar a ETL (um dia)
```bash
PYTHONPATH=src python -m uv run python -m etl.cli --date 2026-01-16
```

### 4) Verificar no banco
```bash
docker exec -it geoetl_postgis psql -U geoetl -d geoetl -c "select count(*) from curated.inpe_focos;"
```

### 5) Query espacial (exemplo)
```bash
docker exec -it geoetl_postgis psql -U geoetl -d geoetl -c "
select count(*)
from curated.inpe_focos
where geom && ST_MakeEnvelope(-55, -34, -48, -27, 4326);
"
```

## Configuração
Arquivo `.env` (não versionado). Use `.env.example` como referência.

## Testes
```bash
PYTHONPATH=src python -m uv run pytest -q
```

## Roadmap
- [ ] Rodar intervalo de datas (`--start-date/--end-date` ou `--backfill`)
- [ ] Incremental automático (último arquivo disponível)
- [ ] Evitar reprocessar datas já carregadas (`file_date`)
- [ ] Agregação por município (IBGE) e tabela diária `focos_por_municipio`
- [ ] Métricas de execução (tempo, inseridos, duplicados, descartados)
