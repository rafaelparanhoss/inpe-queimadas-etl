# Geo Sources Report

Date: 2026-02-09  
Scope: discover geometry sources used by ETL/canonical SQL, create stable API-facing views in `public.geo_*`, and validate API endpoints without changing ETL tables/code.

## 1) ETL/code scan (`ref`/`refs` and thematic layers)

### UF
- `sql/ref/00_ref_geo_prepare.sql:47` creates `ref.ibge_ufs_web`.
- `sql/ref/00_ref_geo_prepare.sql:53` populates `ref.ibge_ufs_web` from `ref.ibge_municipios_web`.
- `sql/marts/91_geo_focos_diario_uf.sql:12` joins UF geometry from `ref.ibge_ufs_web`.
- `sqlm/ref_core/10_ref_geo_prepare.sql:47` creates `ref_core.ibge_ufs_web` (canonical branch).
- `sqlm/marts/prereq/010_mv_uf_geom_mainland.sql:6` reads from `ref_core.ibge_ufs_web`.

Chosen object for API UF: `ref.ibge_ufs_web` (stable 27 rows, SRID 4326, directly used by ETL SQL branch).

### Municipality
- `src/etl/ensure_ref_ibge.py:236` loads `ref.ibge_municipios (cd_mun,nm_mun,uf,geom)`.
- `sql/enrich/20_enrich_municipio.sql:49` uses `ref.ibge_municipios` for enrichment.
- `sql/ref/00_ref_geo_prepare.sql:17` creates `ref.ibge_municipios_web`.
- `sql/ref/00_ref_geo_prepare.sql:25` populates `ref.ibge_municipios_web` with simplified geometry.
- `sql/marts/90_geo_focos_diario_municipio.sql:14` joins `ref.ibge_municipios_web`.
- `sqlm/marts/canonical/050_v_chart_mun_choropleth_day.sql:12` canonical branch uses `ref_core.ibge_municipios_web`.

Chosen object for API municipality: `ref.ibge_municipios_web` (`cd_mun`, `uf`, `geom`, web-ready simplification).

### Bioma
- `sql/enrich/21_enrich_biomas_ucs_tis.sql:39` uses `ref.biomas_4326_sub`.
- `sqlm/ref_core/00_build_ref_core.sql:57`..`sqlm/ref_core/00_build_ref_core.sql:60` picks bioma source (priority list).
- `sqlm/ref_core/00_build_ref_core.sql:254` builds normalized `ref_core.bioma`.
- `sqlm/marts/canonical/055_v_focos_enriched_full.sql:38` uses `ref_core.bioma`.

Chosen object for API bioma: `ref_core.bioma` (normalized key/name contract used by canonical marts).

### UC
- `sql/enrich/21_enrich_biomas_ucs_tis.sql:69` uses `ref.ucs_4326_sub`.
- `sqlm/ref_core/00_build_ref_core.sql:33`..`sqlm/ref_core/00_build_ref_core.sql:42` picks UC source.
- `sqlm/ref_core/00_build_ref_core.sql:126` builds normalized `ref_core.uc`.
- `sqlm/marts/canonical/055_v_focos_enriched_full.sql:48` uses `ref_core.uc`.

Chosen object for API UC: `ref_core.uc` (normalized key/name contract used by canonical marts).

### TI
- `sql/enrich/21_enrich_biomas_ucs_tis.sql:99` uses `ref.tis_4326_sub`.
- `sqlm/ref_core/00_build_ref_core.sql:45`..`sqlm/ref_core/00_build_ref_core.sql:53` picks TI source.
- `sqlm/ref_core/00_build_ref_core.sql:190` builds normalized `ref_core.ti`.
- `sqlm/marts/canonical/055_v_focos_enriched_full.sql:58` uses `ref_core.ti`.

Chosen object for API TI: `ref_core.ti` (normalized key/name contract used by canonical marts).

## 2) Postgres introspection

Execution method: `docker exec ... psql -U geoetl -d geoetl`.

### A) Geometry-like columns in relevant schemas

Query:
```sql
select table_schema, table_name, column_name, udt_name, data_type
from information_schema.columns
where table_schema in ('ref','refs','ref_core','public')
  and (column_name ilike '%geom%' or column_name ilike '%shape%' or column_name ilike '%wkb%')
order by 1,2,3;
```

Observed key rows:
- `ref.ibge_ufs_web.geom`
- `ref.ibge_municipios_web.geom`
- `ref.biomas_4326_sub.geom`, `ref.ucs_4326_sub.geom`, `ref.tis_4326_sub.geom`
- `ref_core.bioma.geom`, `ref_core.uc.geom`, `ref_core.ti.geom`

Result table:

| table_schema | table_name | column_name | udt_name | data_type |
|---|---|---|---|---|
| public | geometry_columns | f_geometry_column | name | name |
| ref | biomas_4326 | geom | geometry | USER-DEFINED |
| ref | biomas_4326_sub | geom | geometry | USER-DEFINED |
| ref | cnuc_uc | geom | geometry | USER-DEFINED |
| ref | ibge_municipios | geom | geometry | USER-DEFINED |
| ref | ibge_municipios_web | geom | geometry | USER-DEFINED |
| ref | ibge_ufs_web | geom | geometry | USER-DEFINED |
| ref | tis_4326 | geom | geometry | USER-DEFINED |
| ref | tis_4326_sub | geom | geometry | USER-DEFINED |
| ref | tis_poligonaisPolygon | geom | geometry | USER-DEFINED |
| ref | ucs_4326 | geom | geometry | USER-DEFINED |
| ref | ucs_4326_sub | geom | geometry | USER-DEFINED |
| ref_core | bioma | geom | geometry | USER-DEFINED |
| ref_core | ibge_municipios | geom | geometry | USER-DEFINED |
| ref_core | ibge_municipios_web | geom | geometry | USER-DEFINED |
| ref_core | ibge_ufs_web | geom | geometry | USER-DEFINED |
| ref_core | ti | geom | geometry | USER-DEFINED |
| ref_core | uc | geom | geometry | USER-DEFINED |

### B) Views in ref/refs-related schemas

Query:
```sql
select schemaname, viewname
from pg_views
where schemaname in ('ref','refs','ref_core','public')
order by 1,2;
```

Result: only PostGIS metadata views in `public`; candidate geo sources are base tables/materialized tables in `ref`/`ref_core`.

Result table:

| schemaname | viewname |
|---|---|
| public | geography_columns |
| public | geometry_columns |

### C) Candidate columns for selected sources

`ref.ibge_ufs_web`: `uf`, `area_km2`, `geom`  
`ref.ibge_municipios_web`: `cd_mun`, `nm_mun`, `uf`, `area_km2`, `geom`  
`ref_core.bioma`: `cd_bioma`, `bioma`, `geom`  
`ref_core.uc`: `cd_cnuc`, `nome_uc`, `geom`  
`ref_core.ti`: `ti_cod`, `ti_nome`, `geom`

### D) Geometry type and SRID checks

- `ref.ibge_ufs_web`: `ST_MultiPolygon`, SRID `4326`, rows `27`.
- `ref.ibge_municipios_web`: `ST_MultiPolygon`/`ST_Polygon`, SRID `4326`, rows `5572`.
- `ref_core.bioma`: `ST_MultiPolygon`, SRID `4326`, rows `10725`.
- `ref_core.uc`: `ST_MultiPolygon`, SRID `4326`, rows `34604`.
- `ref_core.ti`: `ST_MultiPolygon`, SRID `4326`, rows `15291`.

Distinct keys:
- bioma: 6
- uc: 3121
- ti: 650

Fact coverage sanity (`marts.mv_focos_day_dim` vs geo keys):
- UF distinct: fact `27`, geo `27`.
- Municipality distinct: fact `5516`, geo `5572`.

## 3) Final source decision

| Entity | Selected source | Key column | Label column | Geom column | Why |
|---|---|---|---|---|---|
| UF | `ref.ibge_ufs_web` | `uf` | `uf` | `geom` | ETL geo branch directly uses it; 27 stable rows; SRID 4326. |
| mun | `ref.ibge_municipios_web` | `cd_mun` | `nm_mun` | `geom` | ETL/marts geo branch uses it; simplified web geometry; SRID 4326. |
| bioma | `ref_core.bioma` | `cd_bioma` | `bioma` | `geom` | Canonical normalized layer, used by canonical marts joins. |
| uc | `ref_core.uc` | `cd_cnuc` | `nome_uc` | `geom` | Canonical normalized UC layer with stable code/name. |
| ti | `ref_core.ti` | `ti_cod` | `ti_nome` | `geom` | Canonical normalized TI layer with stable code/name. |

## 4) Applied SQL (views for API)

Applied file:
- `docs/sql/geo_sources_apply.sql`

Created:
- `public.geo_uf (uf, geom)`
- `public.geo_mun (cd_mun, uf, geom)`
- `public.geo_bioma (key, label, geom)`
- `public.geo_uc (key, label, geom)`
- `public.geo_ti (key, label, geom)`

Validation:
- all five views created successfully (`CREATE VIEW` x5).
- all views return SRID min/max = `4326`.

## 5) API smoke validation

API started with `GEO_*` pointed to `public.geo_*` (port `8001`).

Status checks:
- `200 /health`
- `200 /api/bounds?entity=uf&key=MT`
- `200 /api/bounds?entity=mun&key=5103254`
- `200 /api/bounds?entity=bioma&key=1`
- `200 /api/choropleth/mun?from=2025-08-01&to=2025-09-01&uf=MT`

Sample response snippets:
- UF bounds (`MT`): bbox `[-61.6333, -18.0416, -50.2248, -7.3490]`
- mun bounds (`5103254`): bbox `[-61.6333, -10.0495, -58.9384, -8.7966]`

## 6) Files changed in this task

- `docs/sql/geo_sources_apply.sql` (new)
- `docs/geo_sources_report.md` (new)
- `api/.env.example` (updated GEO_* defaults to `public.geo_*`)
- `README.md` (updated setup/tests for geo views and bounds endpoints)

## 7) Encoding + Overlay Diagnostics (2026-02-09)

### DB encoding checks

```sql
show server_encoding;
show client_encoding;
select key, label from public.geo_ti where key='58401' limit 5;
select encode(convert_to(label, 'UTF8'),'escape') from public.geo_ti where key='58401' limit 1;
```

Observed:
- `server_encoding=UTF8`
- `client_encoding=UTF8`
- label persisted as `Sï¿½o Marcos - RR`
- UTF8 escaped bytes for label: `S\357\277\275o Marcos - RR` (`\357\277\275` is U+FFFD replacement character)

Inference: mojibake is already persisted in source data, not generated by HTTP response serialization.

### TI source comparison for key 58401

Compared:
- `ref_core.ti.ti_nome`
- `ref.tis_4326_sub.terrai_nom`
- `ref.tis_4326.terrai_nom`

All carried the same replacement-character form (`Sï¿½o`), so no clean alternate label source was found inside current loaded layers.

### Geometry multiplicity diagnosis for key 33401

```sql
with src as (
  select key::text as key, geom
  from public.geo_ti
  where key::text='33401'
)
select count(*) from src;
```

Observed:
- `count(*) = 48` geometries for the same TI key.

This confirmed why picking a single row/fragment can yield incorrect or undersized overlays.

### Action taken

- `/api/geo` pipeline changed to union all parts by key before output:
  - `ST_UnaryUnion(ST_Collect(...))`
  - `ST_MakeValid(...)`
  - `ST_CollectionExtract(...,3)`
- Optional simplification is now parameterized (`simplify=0|1`, `tol_m`) and applied in metric CRS (`3857`).
- Added optional wrapper view `public.geo_ti_utf8` with best-effort label patch and switched `.env.example` to `GEO_TI_TABLE=public.geo_ti_utf8`.

## 8) Encoding + Simplify Validation (2026-02-09, post-fix)

### DB checks (docker psql)

```sql
show server_encoding;
show client_encoding;
select key,label from public.geo_ti where key='58401' limit 5;
select encode(convert_to(label,'UTF8'),'escape') from public.geo_ti where key='58401' limit 1;
select key,label from public.geo_ti_utf8 where key='58401' limit 1;
```

Observed:
- `server_encoding = UTF8`
- `client_encoding = UTF8`
- `public.geo_ti` still stores mojibake (`S\uFFFDo Marcos - RR`)
- escaped bytes confirm replacement-char payload: `S\357\277\275o Marcos - RR`
- `public.geo_ti_utf8` returns corrected display label: `São Marcos - RR`

### API smoke (port 8002)

- `200 /api/geo?entity=ti&key=33401&simplify=0`
- `200 /api/geo?entity=ti&key=33401&simplify=1&tol_m=10`
- `200 /api/geo/qa?entity=ti&key=33401&simplify=0`
- `200 /api/geo/qa?entity=ti&key=33401&simplify=1&tol_m=10`
- `200 /api/geo?entity=ti&key=58401`
- `200 /api/validate?from=2025-08-01&to=2025-09-01&ti=33401`

Key assertions:
- simplify is now effective (`geo_equal=False` for simplify 0 vs 1)
- QA confirms parameter propagation:
  - simplify=0 -> `simplify_applied=False`, `npoints_out=4078`, hash `9f5fdfb386795a9f`
  - simplify=1 -> `simplify_applied=True`, `npoints_out=2576`, hash `9e25064694e59a45`
- label fixed in API response for key 58401: `São Marcos - RR`
- consistency preserved: `validate.consistent=True`, `bounds_ratio=1.0`, `bounds_consistent=True`

## 9) TI/UC Canonical Geometry + Encoding Audit (2026-02-09)

### 9.1 Source audit (Postgres)

Relevant objects found:
- `ref_core.ti`, `ref.tis_4326_sub`, `ref.tis_4326`, `ref."tis_poligonaisPolygon"`
- `ref_core.uc`, `ref.ucs_4326_sub`, `ref.ucs_4326`

Key multiplicity (problem evidence):
```sql
select count(*) from public.geo_ti where key='33401';          -- 48 (before fix)
select count(*) from public.geo_uc where key='0000.00.0096';   -- 1228 (before fix)
```

Comparative source checks for the same keys:
```sql
select count(*) from ref_core.ti where ti_cod='33401';         -- 48
select count(*) from ref.tis_4326 where terrai_cod=33401;      -- 1
select count(*) from ref_core.uc where cd_cnuc='0000.00.0096'; -- 1228
select count(*) from ref.ucs_4326 where cd_cnuc='0000.00.0096';-- 1
```

Geometry comparison (dissolved by source, same key):
```sql
-- TI 33401
-- ref_core.ti: area ~ 6.020e9 m², bbox [-59.1084,-14.6660,-58.0816,-14.0002], n_rows=48
-- ref.tis_4326: area ~ 6.020e9 m², bbox [-59.1084,-14.6660,-58.0816,-14.0002], n_rows=1

-- UC 0000.00.0096
-- ref_core.uc: area ~ 4.036e9 m², bbox [-56.8378,-5.4998,-56.0363,-4.6837], n_rows=1228
-- ref.ucs_4326: area ~ 4.036e9 m², bbox [-56.8378,-5.4998,-56.0363,-4.6837], n_rows=1
```

Encoding diagnostics:
```sql
select count(*) from ref_core.ti where ti_nome like '%' || chr(65533) || '%'; -- 6251
select count(*) from ref.tis_4326 where terrai_nom like '%' || chr(65533) || '%'; -- 282
select count(*) from ref_core.uc where nome_uc like '%' || chr(65533) || '%'; -- 0
select count(*) from ref.ucs_4326 where nome_uc like '%' || chr(65533) || '%'; -- 0
```

Decision:
- TI canonical source: `ref.tis_4326` (single row/key + fewer broken labels than `ref_core.ti`).
- UC canonical source: `ref.ucs_4326` (single row/key + clean labels).

### 9.2 Applied fix SQL

New file:
- `docs/sql/geo_sources_apply_fix_ti_uc.sql`

What it does:
- recreates `public.geo_ti` and `public.geo_uc`
- enforces one row per key with dissolved geometry:
  - `ST_UnaryUnion(ST_Collect(ST_MakeValid(geom)))`
  - `ST_CollectionExtract(..., 3)`
  - SRID normalized to 4326
- TI label best-effort cleanup for replacement-char cases (`chr(65533)`), e.g. `S?o -> São`

### 9.3 Post-fix validation

View integrity:
```sql
select count(*) from (select key from public.geo_ti group by key having count(*)>1) t; -- 0
select count(*) from (select key from public.geo_uc group by key having count(*)>1) t; -- 0
```

Problem keys:
```sql
select count(*) from public.geo_ti where key='33401'; -- 1
select st_astext(st_envelope(geom)) from public.geo_ti where key='33401';
-- POLYGON((-59.10840348 -14.66598739,-59.10840348 -14.0002127,-58.08160635 -14.0002127,-58.08160635 -14.66598739,...))

select count(*) from public.geo_uc where key='0000.00.0096'; -- 1
```

Label checks:
```sql
select count(*) from public.geo_ti where label like '%' || chr(65533) || '%'; -- 0
select label from public.geo_ti where key='58401'; -- São Marcos - RR
```

API smoke (after fix):
- `200 /api/geo?entity=ti&key=33401&simplify=0`
- `200 /api/geo/qa?entity=ti&key=33401&simplify=0`
- `200 /api/top?group=ti&from=2025-08-01&to=2025-09-01&limit=20`
- `200 /api/top?group=uc&from=2025-08-01&to=2025-09-01&limit=20`

Observed:
- `geo_ti_33401_bbox = [-59.1084, -14.6660, -58.0816, -14.0002]` (coerente com a extensão real)
- `/api/top` TI/UC sem `?` na amostra validada.
