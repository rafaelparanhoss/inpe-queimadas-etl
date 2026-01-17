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
