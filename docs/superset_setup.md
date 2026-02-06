# Superset setup (portfolio)

Este guia foca nos 3 datasets canonicos do portfolio:
- `marts.v_chart_uf_choropleth_day`
- `marts.v_chart_mun_choropleth_day`
- `marts.v_chart_focos_scatter`
E no agregado leve para os charts analiticos:
- `marts.mv_focos_day_dim`

## Datasets -> charts -> campos

| Dataset | Chart sugerido | Campos principais | Tooltip sugerido |
|---|---|---|---|
| `marts.v_chart_uf_choropleth_day` | Choropleth UF (polygon) | `day` (time), `uf`, `poly_coords`, `n_focos_viz` (metric) | `uf`, `n_focos`, `day` |
| `marts.v_chart_mun_choropleth_day` | Choropleth MUN (Deck.gl Polygon) | `day`, `cd_mun`, `mun_nm_mun`, `poly_coords`, `n_focos`, `n_focos_viz` | `mun_nm_mun`, `n_focos`, `day` |
| `marts.v_chart_focos_scatter` | Scatter/mapa de pontos | `day`, `lat`, `lon`, `geom`, `file_date` | `uf`, `mun_nm_mun`, `bioma`, `nome_uc`, `terrai_nom`, `day` |
| `marts.mv_focos_day_dim` | Timeseries + tops (agregado) | `day`, `n_focos`, `bioma`, `uf`, `cd_mun`, `mun_nm_mun`, `uc_nome`, `ti_nome` | usar metric `SUM(n_focos)` + dimensoes |

## Observacoes de performance
- No scatter, limitar o periodo (ex.: ultimo mes) se o mapa ficar pesado. Time column = `day`.
- Em choropleth UF/MUN, filtrar por `day` evita carga desnecessaria.

## Datasets (criar/atualizar)
1) `marts.v_chart_uf_choropleth_day`
2) `marts.v_chart_mun_choropleth_day` (metric: `SUM(n_focos_viz)`)
3) `marts.v_chart_focos_scatter`
4) `marts.mv_focos_day_dim`

## Notas de qualidade
- `v_chart_focos_scatter` usa `marts.v_focos_enriched_full`, entao ja traz bioma/UC/TI.
- Os checks em `sql/checks` validam cobertura (UF=27, MUN>0, scatter>0) e enrichment basico.
