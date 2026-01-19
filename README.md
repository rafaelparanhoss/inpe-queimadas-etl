# INPE Queimadas - ETL diario (Brasil) -> PostGIS

ETL geoespacial para ingestao diaria de focos de queimadas (INPE, CSV publico) e carga em PostGIS, com camadas `raw/curated` para auditoria e analise.

## Stack
Python 3.11 | pandas | requests | psycopg | PostGIS | Docker | uv

## Decisoes tecnicas
- **`raw` vs `curated`**
  - `raw`: preserva o dado "como veio" (inclui `props` completo) para auditoria/reprocessamento.
  - `curated`: tabela enxuta, padronizada e indexada para consulta espacial.
- **`props` em `jsonb`**
  - evita migracao de schema quando a fonte muda colunas;
  - mantem rastreabilidade e permite inspecao pontual.
- **Dedup por `event_hash`**
  - idempotencia (rodar a mesma data nao duplica);
  - `ON CONFLICT DO NOTHING`.
- **SRID 4326**
  - a fonte ja vem em lat/lon (WGS84).

## Banco
Tabelas:
- `raw.inpe_focos`
- `curated.inpe_focos`

Indices:
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
python -m uv pip install -e .
```

### 3) Rodar a ETL (um dia)
```bash
python -m uv run python main.py --date 2026-01-16
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

## Logs
Arquivo gerado em `data/logs/etl.log`.

## Configuracao
Arquivo `.env` (nao versionado). Use `.env.example` como referencia.

## Roadmap
- [ ] Rodar intervalo de datas (`--start-date/--end-date` ou `--backfill`)
- [ ] Incremental automatico (ultimo arquivo disponivel)
- [ ] Evitar reprocessar datas ja carregadas (`file_date`)
- [ ] Agregacao por municipio (IBGE) e tabela diaria `focos_por_municipio`
- [ ] Metricas de execucao (tempo, inseridos, duplicados, descartados)
