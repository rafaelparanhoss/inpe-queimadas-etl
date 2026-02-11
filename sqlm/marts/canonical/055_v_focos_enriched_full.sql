create schema if not exists marts;

create or replace view marts.v_focos_enriched_full as
select
  coalesce(f.view_ts::date, f.file_date) as day,
  f.event_hash,
  f.file_date,
  f.view_ts,
  f.satelite,
  'inpe'::text as fonte,
  f.municipio,
  f.estado,
  f.lat,
  f.lon,
  f.geom,
  f.mun_uf as uf,
  f.mun_uf as cd_uf,
  f.mun_nm_mun as mun,
  f.mun_cd_mun as cd_mun,
  f.mun_cd_mun as mun_cd_mun,
  f.mun_nm_mun as mun_nm_mun,
  f.mun_uf as mun_uf,
  f.mun_area_km2 as mun_area_km2,
  coalesce(b.cd_bioma, f.cd_bioma) as cd_bioma,
  coalesce(b.bioma, f.bioma) as bioma,
  uc.cd_cnuc as uc_id,
  uc.nome_uc as uc_nome,
  uc.cd_cnuc as cd_cnuc,
  uc.nome_uc as nome_uc,
  ti.ti_cod as ti_id,
  ti.ti_nome as ti_nome,
  ti.ti_cod as terrai_cod,
  ti.ti_nome as terrai_nom,
  f.etnia_nome
from curated.inpe_focos_enriched f
left join lateral (
  select cd_bioma, bioma
  from ref_core.bioma b
  where f.geom is not null
    and b.geom is not null
    and b.geom && f.geom
    and st_intersects(b.geom, f.geom)
  order by b.cd_bioma
  limit 1
) b on true
left join lateral (
  select cd_cnuc, nome_uc
  from ref_core.uc u
  where f.geom is not null
    and u.geom is not null
    and u.geom && f.geom
    and st_intersects(u.geom, f.geom)
  order by u.cd_cnuc
  limit 1
) uc on true
left join lateral (
  select ti_cod, ti_nome
  from ref_core.ti t
  where f.geom is not null
    and t.geom is not null
    and t.geom && f.geom
    and st_intersects(t.geom, f.geom)
  order by t.ti_cod
  limit 1
) ti on true
where f.geom is not null;
