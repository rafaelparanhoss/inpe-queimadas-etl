# Scatter (Fase 2.3)

## Semantica

- **Choropleth**: sempre agregado no periodo `[from,to)` (`to` exclusivo).
- **Pontos (scatter)**: snapshot de **um unico dia** dentro do periodo.

## Seletor de dia dos pontos

No painel, o modo do dia dos pontos possui 3 opcoes:

- `Dia de Pico (no Periodo)` (`peak_day`)
- `Primeiro Dia do Periodo` (`from`)
- `Escolher Dia` (`custom`)

Regras:

- Se o range for exatamente 1 dia (`to = from + 1`), a UI forca `from`.
- Se o range for maior que 1 dia, a UI usa o modo selecionado.

## Regra de range `[from,to)` (to exclusivo)

- Todos os endpoints e graficos usam range **exclusivo em `to`**.
- Para representar **1 dia**, use `from = D` e `to = D+1`.
- Se o usuario aplicar `from == to` (ou `to < from`), a UI ajusta automaticamente para `to = from + 1` antes de chamar a API.

## Endpoint `/api/points`

`GET /api/points?date=YYYY-MM-DD&bbox=minLon,minLat,maxLon,maxLat&limit=...&uf=&bioma=&mun=&uc=&ti=`

Campos principais do payload:

- `date`, `bbox`, `returned`, `limit`, `truncated`
- `points[]` com:
  - `lon`, `lat`, `n`
  - `uf`
  - `mun_key`, `mun_label`
  - `bioma_key`, `bioma_label`
  - `uc_key`, `uc_label`
  - `ti_key`, `ti_label`

Observacoes:

- Os campos extras sao lidos da mesma fonte de pontos (`marts.v_chart_focos_scatter`), sem spatial join adicional.
- Quando `truncated=true`, o backend retorna amostra limitada por `limit`.

## Tooltip no mapa

Com pontos ativos, o tooltip/popup do ponto mostra:

- Data
- Focos
- UF
- Municipio
- Bioma
- UC(s)
- TI(s)

Listas longas (UC/TI) sao truncadas visualmente com `+N`.

## Pontos sobrepostos (spiderfy/lista)

Quando varios pontos caem na mesma celula de cluster:

- Em zoom normal, o clique no agregado continua aproximando o mapa.
- No zoom maximo (ou quase maximo), o clique abre uma lista de pontos sobrepostos.
- Cada item da lista abre o detalhe individual do ponto no popup.
