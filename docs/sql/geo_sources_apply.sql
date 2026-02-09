\set ON_ERROR_STOP on

-- Stable geometry sources for API bounds and municipal choropleth.
-- No ETL object is modified; this creates read-only views in public schema.

create or replace view public.geo_uf as
select
  upper(trim(uf))::text as uf,
  case
    when st_srid(geom) = 4326 then geom
    else st_transform(geom, 4326)
  end as geom
from ref.ibge_ufs_web
where uf is not null
  and length(trim(uf)) = 2
  and geom is not null;


create or replace view public.geo_mun as
select
  cd_mun::text as cd_mun,
  upper(trim(uf))::text as uf,
  case
    when st_srid(geom) = 4326 then geom
    else st_transform(geom, 4326)
  end as geom
from ref.ibge_municipios_web
where cd_mun is not null
  and uf is not null
  and length(trim(uf)) = 2
  and geom is not null;


create or replace view public.geo_bioma as
select
  coalesce(nullif(trim(cd_bioma::text), ''), nullif(trim(bioma), ''))::text as key,
  coalesce(nullif(trim(bioma), ''), nullif(trim(cd_bioma::text), ''))::text as label,
  case
    when st_srid(geom) = 4326 then geom
    else st_transform(geom, 4326)
  end as geom
from ref_core.bioma
where geom is not null
  and coalesce(nullif(trim(cd_bioma::text), ''), nullif(trim(bioma), '')) is not null;


create or replace view public.geo_uc as
select
  coalesce(nullif(trim(cd_cnuc::text), ''), nullif(trim(nome_uc), ''))::text as key,
  coalesce(nullif(trim(nome_uc), ''), nullif(trim(cd_cnuc::text), ''))::text as label,
  case
    when st_srid(geom) = 4326 then geom
    else st_transform(geom, 4326)
  end as geom
from ref_core.uc
where geom is not null
  and coalesce(nullif(trim(cd_cnuc::text), ''), nullif(trim(nome_uc), '')) is not null;


create or replace view public.geo_ti as
select
  coalesce(nullif(trim(ti_cod::text), ''), nullif(trim(ti_nome), ''))::text as key,
  coalesce(nullif(trim(ti_nome), ''), nullif(trim(ti_cod::text), ''))::text as label,
  case
    when st_srid(geom) = 4326 then geom
    else st_transform(geom, 4326)
  end as geom
from ref_core.ti
where geom is not null
  and coalesce(nullif(trim(ti_cod::text), ''), nullif(trim(ti_nome), '')) is not null;


-- Optional wrapper for label mojibake fallback in TI names.
create or replace view public.geo_ti_utf8 as
select
  key,
  case
    when label is null then null
    else replace(replace(replace(label, 'S�o', 'São'), 's�o', 'são'), '�', 'a')
  end as label,
  geom
from public.geo_ti;
