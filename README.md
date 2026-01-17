## Decisões técnicas

- **Camadas `raw` e `curated` (PostGIS):**
  - `raw`: guarda o registro “como veio” (inclui `props` completo), permite auditoria, reprocessamento e rastreio de mudanças na fonte.
  - `curated`: tabela enxuta e padronizada para análise espacial/indicadores (índices, campos principais, geometria pronta).

- **`props` em `jsonb`:**
  - preserva colunas variáveis do CSV do INPE sem quebrar o schema do banco;
  - facilita inspeção e filtros pontuais (ex.: extrair um atributo específico sem alterar tabela).

- **Deduplicação por `event_hash`:**
  - a fonte pode ser rebaixada/reprocessada e o pipeline precisa ser idempotente;
  - `event_hash` (baseado em data + coordenadas + timestamp/satélite quando disponível) permite `ON CONFLICT DO NOTHING`.

- **SRID 4326 (WGS84):**
  - o CSV do INPE é distribuído em lat/lon geográficas;
  - 4326 é o padrão para integração com outras bases e para consultas espaciais rápidas (ex.: BBOX, interseção com limites administrativos).
