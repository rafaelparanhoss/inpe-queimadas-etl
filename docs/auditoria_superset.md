# Auditoria marts (views + materialized views) â€” INPE Focos (Superset)

nota: este documento e historico. a configuracao final valida esta em README.md e docs/superset_setup.md.

Contexto
- ETL roda pelo VSCode (Python) e alimenta:
  - curated.inpe_focos_enriched (pontos enriquecidos)
  - marts.focos_diario_uf (tabela agregada por UF/dia) + outras tabelas de marts
- As MVs/views abaixo foram criadas â€œpor foraâ€ no pgAdmin (SQL avulso), para resolver:
  1) UF sumindo (ilhas/multiparts)
  2) buracos (interior rings) no Polygon
  3) performance (payload/complexidade geom)
  4) preencher UFs com 0 focos no dia (27 linhas por dia)
  5) compatibilidade Superset (Deck.gl Polygon / GeoJSON)

---

## 1) Fonte da verdade (o que Ã© â€œoficialâ€ para charts)

### 1.1 UF choroplÃ©tico (Deck.gl Polygon) â€” FINAL âœ…
Dataset recomendado (Superset):
- marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz

Por quÃª:
- Garante 27 UFs por dia (CROSS JOIN days Ã— ufs)
- 0 focos vira n_focos=0 e (para color bucket) n_focos_viz=0.000001
- poly_coords vem de uma MV â€œlimpaâ€ (mainland + sem buracos)

DependÃªncias:
- marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz
  -> marts.v_geo_focos_diario_uf_poly_by_day_superset_full
     -> marts.mv_uf_polycoords_polygon_superset
        -> marts.mv_uf_mainland_poly_noholes
           -> marts.mv_uf_geom_mainland
              -> ref.ibge_ufs_web

### 1.2 MunicÃ­pios choroplÃ©tico (Deck.gl GeoJSON) â€” FINAL âœ…
Dataset recomendado (Superset):
- marts.v_geo_mun_fc_by_day

Por quÃª:
- Entrega FeatureCollection por dia, pronto para Deck.gl GeoJSON
- properties incluem uf/cd_mun/nm_mun/n_focos/area_km2

DependÃªncias:
- marts.v_geo_mun_fc_by_day
  -> marts.geo_focos_diario_municipio (view)
     -> marts.focos_diario_municipio (tabela, implÃ­cita)
     -> ref_core.ibge_municipios_web (geom/area/nome)

### 1.3 Pontos (scatter)
Dataset recomendado:
- curated.inpe_focos_enriched

---

## 2) InventÃ¡rio e propÃ³sito â€” Materialized Views

### mv_uf_geom_mainland (M) âœ… manter
SQL: dump/makevalid, extrai polÃ­gonos, rank por Ã¡rea e pega o MAIOR (mainland).
Objetivo: remover ilhas e multiparts â€œproblemÃ¡ticosâ€ mantendo sÃ³ o continente principal.
Input: ref.ibge_ufs_web (geom)
Output: uf, geom Polygon(4326)

### mv_uf_geom_mainland_fast (M) âš ï¸ opcional / experimental
SQL: ST_SimplifyPreserveTopology(geom, 0.02) em cima do mainland.
Objetivo: performance rÃ¡pida com perda de detalhe e risco de deformar fronteiras.
Uso: nÃ£o aparece na cadeia final do UF choroplÃ©tico v2. SÃ³ manter se for necessÃ¡rio para um â€œmodo rÃ¡pidoâ€.

### mv_uf_geom_mainland_noholes (M) âš ï¸ opcional / nÃ£o usado no final
SQL: remove buracos do mainland (constrÃ³i polygon a partir do exterior ring), volta como MultiPolygon.
Uso: nÃ£o Ã© o que alimenta o dataset final (vocÃªs optaram por â€œpoly noholesâ€ + â€œmainland por Ã¡reaâ€).

### mv_uf_mainland_poly_noholes (M) âœ… manter (base do Superset Polygon)
SQL:
- pega mv_uf_geom_mainland
- extrai partes, rank por Ã¡rea, mantÃ©m maior
- remove buracos com ST_MakePolygon(ST_ExteriorRing(p))
Output: uf, geom Polygon(4326)
Motivo: Polygon simples (nÃ£o MultiPolygon) + sem interior rings â†’ Deck.gl Polygon fica estÃ¡vel e sem â€œburacosâ€.

### mv_uf_polycoords_polygon_superset (M) âœ… manter (crÃ­tica)
SQL: st_asgeojson(geom)::jsonb -> 'coordinates' AS poly_coords
Input: mv_uf_mainland_poly_noholes
Output: uf, poly_coords (JSON coordinates)
Motivo: formato ideal para Deck.gl Polygon (Polygon encoding JSON).

### mv_uf_poly_coords_mainland (M) âš ï¸ redundante / deprecÃ¡vel
SQL: poly_coords direto do mv_uf_geom_mainland.
NÃ£o remove buracos â†’ nÃ£o Ã© o â€œfinalâ€ do choroplÃ©tico v2.

### mv_uf_polycoords_superset (M) âš ï¸ redundante / deprecÃ¡vel
SQL: poly_coords a partir de mv_uf_geom_mainland_noholes (MultiPolygon).
Deck.gl Polygon tende a ser mais sensÃ­vel com MultiPolygon + nesting â†’ preferimos Polygon simples.

### mv_focos_uf_day (M) âš ï¸ legado / nÃ£o usar como fonte principal de UF
SQL: agrega curated.inpe_focos_enriched por file_date e faz mapping via campo â€œestadoâ€ (texto).
Problemas potenciais:
- depende de texto/acento (ex.: AMAPÃ) e pode quebrar por variaÃ§Ãµes
- perde rastreabilidade do UF obtido por spatial join de municÃ­pio (mun_uf)
RecomendaÃ§Ã£o:
- NÃ£o usar para choroplÃ©tico UF (use marts.focos_diario_uf).
- Manter apenas se algum chart antigo ainda depende (ex.: v_geo_focos_diario_uf_simpl / v_geo_uf_fc_by_day).

### mv_dim_uf_geom_simpl (M) âŒ legado / problemÃ¡tico / deprecÃ¡vel
SQL: clip + snap + simplify preserve topology em geometria â€œrawâ€.
HistÃ³rico: foi a origem dos â€œburacosâ€, estados sumindo e inconsistÃªncia de render.
RecomendaÃ§Ã£o:
- NÃ£o usar em charts novos.
- Marcar como DEPRECATED e remover depois com migraÃ§Ã£o controlada.

---

## 3) InventÃ¡rio e propÃ³sito â€” Views

### v_geo_focos_diario_uf_poly_by_day_superset_full_viz (V) âœ… FINAL (UF choroplÃ©tico v2)
- adiciona n_focos_viz = 0.000001 quando n_focos=0
- mantÃ©m n_focos real para tooltip/contagem
Uso Superset:
- Deck.gl Polygon
- Polygon column: poly_coords
- Metric cor: SUM(n_focos_viz)
- Tooltip: usar JS para mostrar uf + n_focos (real)

### v_geo_focos_diario_uf_poly_by_day_superset_full (V) âœ… manter (base do _viz)
- cria 27 UFs por dia (days Ã— UFs) e preenche n_focos com 0

### v_geo_focos_diario_uf_poly_by_day_superset (V) âš ï¸ intermediÃ¡ria
- sÃ³ traz UFs que existem em focos_diario_uf no dia (nÃ£o garante 27).
- Ãºtil apenas para debug rÃ¡pido.

### v_geo_focos_diario_uf_poly_by_day_superset_viz (V) âš ï¸ intermediÃ¡ria
- equivalente ao â€œvizâ€ mas sem completar UFs ausentes.
- pode causar â€œUF vazia em dia sem focoâ€.

### v_geo_mun_fc_by_day (V) âœ… FINAL (mun choroplÃ©tico)
- FeatureCollection por dia via jsonb_agg
- pronto para Deck.gl GeoJSON.

### geo_focos_diario_municipio (V) âœ… manter (base do mun)
- junta focos_diario_municipio com ref_core.ibge_municipios_web.

### geo_focos_eventos (V) âœ… manter (base de pontos/inspeÃ§Ã£o)
- gera feature e properties para cada evento.
- Ãºtil para debug e scatter/inspeÃ§Ã£o.

### focos_diario_uf_trend (V) âœ… manter
- calcula MA7 e MA30 por UF.
- Ã³timo para chart de tendÃªncia (linha).

### geo_focos_diario_uf + geo_focos_diario_uf_simpl (V) âš ï¸ legado
- usa ref_core.ibge_ufs_web geom direto e â€œsimplifyâ€ por 3857.
- pode reintroduzir buracos/ilhas/complexidade e nÃ£o usa poly_coords.
- manter sÃ³ se houver chart antigo; caso contrÃ¡rio, deprecÃ¡vel.

### v_dim_uf_geom_raw / v_dim_uf_geom_noholes (V) âš ï¸ legado
- v_dim_uf_geom_raw agrega UFs a partir de ref.ibge_ufs_web
- v_dim_uf_geom_noholes remove buracos via exterior ring
- hoje a cadeia â€œmainland + poly_noholesâ€ jÃ¡ cobre o caso com mais estabilidade.

### v_geo_uf_fc_by_day / v_geo_uf_fc_by_day_ok / v_geo_uf_fc_by_day_ok_txt / v_geo_uf_fc_by_day_old3 (V) âŒ legado/experimentos GeoJSON UF
- foram tentativas de Deck.gl GeoJSON (houve erro JSON/React e parsing).
- vocÃªs migraram para Deck.gl Polygon com poly_coords (muito mais estÃ¡vel).
RecomendaÃ§Ã£o: deprecÃ¡veis, mas sÃ³ remover apÃ³s checar se algum chart ainda usa.

### v_geo_focos_diario_uf_simpl (V) âŒ legado
- depende de mv_focos_uf_day e dim_uf_geom_simpl
- Ã© a combinaÃ§Ã£o dos dois elementos que mais deram problema.

---

## 4) O que manter vs o que congelar/depreciar

### Manter como â€œcoreâ€ (nÃ£o mexer)
- mv_uf_geom_mainland
- mv_uf_mainland_poly_noholes
- mv_uf_polycoords_polygon_superset
- v_geo_focos_diario_uf_poly_by_day_superset_full
- v_geo_focos_diario_uf_poly_by_day_superset_full_viz
- v_geo_mun_fc_by_day
- geo_focos_eventos
- focos_diario_uf_trend

### Opcional (manter se usar, senÃ£o marcar deprecated)
- mv_uf_geom_mainland_fast
- mv_uf_geom_mainland_noholes
- mv_uf_polycoords_superset
- mv_uf_poly_coords_mainland
- geo_focos_diario_uf / geo_focos_diario_uf_simpl
- v_dim_uf_geom_raw / v_dim_uf_geom_noholes

### Deprecar (nÃ£o usar mais; remover depois com migraÃ§Ã£o)
- mv_dim_uf_geom_simpl
- mv_focos_uf_day (para UF choroplÃ©tico; manter sÃ³ se algum chart antigo precisar)
- v_geo_focos_diario_uf_simpl
- v_geo_uf_fc_by_day* (todas variaÃ§Ãµes)
- v_geo_focos_diario_uf_poly_by_day_superset_viz (se jÃ¡ existe o _full_viz)

---

## 5) HigienizaÃ§Ã£o recomendada (sem quebrar nada)

### 5.1 â€œCongelamentoâ€ de naming (criar aliases finais e parar de trocar dataset)
Criar views â€œcanÃ´nicasâ€ para o Superset (nomes explÃ­citos):
- marts.v_chart_uf_choropleth_day  -> aponta para v_geo_focos_diario_uf_poly_by_day_superset_full_viz
- marts.v_chart_mun_choropleth_day -> aponta para v_geo_mun_fc_by_day
- marts.v_chart_focos_scatter      -> (se quiser) view sobre curated.inpe_focos_enriched filtrando geom != null

Assim o Superset usa sÃ³ `v_chart_*` e vocÃªs podem refatorar por baixo sem mexer no dashboard.

### 5.2 Guard-rails (queries de validaÃ§Ã£o automÃ¡tica)
- garantir 27 UFs por dia:
  select day, count(*) from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz group by day having count(*)<>27;
- garantir poly_coords nÃ£o nulo:
  select count(*) from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz where poly_coords is null;

### 5.3 MigraÃ§Ã£o para o repo (parar SQL â€œsoltoâ€)
- criar a estrutura `sqlm/` no repositorio com:
  - 010_mv_uf_geom_mainland.sql
  - 020_mv_uf_mainland_poly_noholes.sql
  - 030_mv_uf_polycoords_polygon_superset.sql
  - 040_v_chart_uf_choropleth_day.sql
  - 050_v_chart_mun_choropleth_day.sql
  - 900_deprecate_legacy_objects.md (sÃ³ documentaÃ§Ã£o, sem drop)

### 5.4 Limpeza (DROP) sÃ³ depois que o Superset estiver apontando para v_chart_*
- primeiro atualizar datasets no Superset para usar v_chart_*.
- depois: dropar legacy com CASCADE cuidadosamente (um por vez), sempre checando dependÃªncias.

---

## 6) Como validar que â€œUF choroplÃ©tico v2â€ Ã© 100% estÃ¡vel
Checklist:
1) 27 UFs na MV:
   select count(*) from marts.mv_uf_polycoords_polygon_superset;
2) 27 UFs por dia na view final:
   select day, count(*) from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz group by day having count(*)<>27;
3) nenhuma UF sem geometria:
   select count(*) from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz where poly_coords is null;
4) soma bate com focos_diario_uf (para um dia):
   select
     (select sum(n_focos) from marts.focos_diario_uf where day='YYYY-MM-DD') as sum_tbl,
     (select sum(n_focos) from marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz where day='YYYY-MM-DD') as sum_view;

---

## 7) ObservaÃ§Ã£o importante (duas â€œlinhasâ€ de UF)
Hoje existem duas formas de UF:
A) UF por municÃ­pio (recomendado):
- curated.inpe_focos_enriched.mun_uf (derivado do spatial join em municÃ­pio)
- marts.focos_diario_uf usa f.mun_uf

B) UF por texto â€œestadoâ€ (legado):
- mv_focos_uf_day usa curated.inpe_focos_enriched.estado (texto) + CASE mapping

RecomendaÃ§Ã£o: para UF/dia, padronizar tudo em (A) e tratar (B) como legado/debug.

---

Fim.
