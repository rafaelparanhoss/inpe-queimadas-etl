\set ON_ERROR_STOP on

-- Canonical TI/UC API views (single-row-per-key, dissolved geometry).
-- This does not modify ETL tables or ETL code.

create or replace view public.geo_ti as
with src as (
  select
    terrai_cod::text as key,
    nullif(trim(terrai_nom), '')::text as label_raw,
    case
      when geom is null then null
      when st_srid(geom) in (4326, 4674) then st_transform(geom, 4326)
      when st_srid(geom) = 0 then st_setsrid(geom, 4326)
      else st_transform(geom, 4326)
    end as geom
  from ref.tis_4326
  where terrai_cod is not null
    and geom is not null
),
agg as (
  select
    key,
    max(label_raw) as label_raw,
    st_collectionextract(
      st_makevalid(
        st_unaryunion(st_collect(st_makevalid(geom)))
      ),
      3
    ) as geom
  from src
  group by key
)
select
  key,
  coalesce(
    nullif(
      replace(
        replace(
          replace(label_raw, 'S' || chr(65533) || 'o', 'S' || chr(227) || 'o'),
          's' || chr(65533) || 'o', 's' || chr(227) || 'o'
        ),
        chr(65533), 'a'
      ),
      ''
    ),
    key
  )::text as label,
  geom
from agg
where geom is not null
  and not st_isempty(geom);


create or replace view public.geo_uc as
with src as (
  select
    cd_cnuc::text as key,
    nullif(trim(nome_uc), '')::text as label_raw,
    case
      when geom is null then null
      when st_srid(geom) in (4326, 4674) then st_transform(geom, 4326)
      when st_srid(geom) = 0 then st_setsrid(geom, 4326)
      else st_transform(geom, 4326)
    end as geom
  from ref.ucs_4326
  where cd_cnuc is not null
    and geom is not null
),
agg as (
  select
    key,
    max(label_raw) as label_raw,
    st_collectionextract(
      st_makevalid(
        st_unaryunion(st_collect(st_makevalid(geom)))
      ),
      3
    ) as geom
  from src
  group by key
)
select
  key,
  coalesce(nullif(label_raw, ''), key)::text as label,
  geom
from agg
where geom is not null
  and not st_isempty(geom);
