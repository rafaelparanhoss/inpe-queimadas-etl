-- enrich curated records with biomas, ucs, and tis

alter table curated.inpe_focos_enriched
  add column if not exists cd_bioma text,
  add column if not exists bioma text,
  add column if not exists uc_id text,
  add column if not exists cd_cnuc text,
  add column if not exists nome_uc text,
  add column if not exists terrai_cod text,
  add column if not exists terrai_nom text,
  add column if not exists etnia_nome text;

with src as (
  select
    event_hash,
    geom,
    cd_bioma,
    bioma,
    uc_id,
    cd_cnuc,
    nome_uc,
    terrai_cod,
    terrai_nom,
    etnia_nome
  from curated.inpe_focos_enriched
  where file_date = :'DATE'::date
    and geom is not null
    and (
      cd_bioma is null
      or bioma is null
      or uc_id is null
      or cd_cnuc is null
      or nome_uc is null
      or terrai_cod is null
      or terrai_nom is null
      or etnia_nome is null
    )
)
update curated.inpe_focos_enriched f
set
  cd_bioma = coalesce(f.cd_bioma, b.cd_bioma),
  bioma = coalesce(f.bioma, b.bioma),
  uc_id = coalesce(f.uc_id, u.uc_id),
  cd_cnuc = coalesce(f.cd_cnuc, u.cd_cnuc),
  nome_uc = coalesce(f.nome_uc, u.nome_uc),
  terrai_cod = coalesce(f.terrai_cod, t.terrai_cod),
  terrai_nom = coalesce(f.terrai_nom, t.terrai_nom),
  etnia_nome = coalesce(f.etnia_nome, t.etnia_nome)
from src
left join lateral (
  select b.cd_bioma::text as cd_bioma, b.bioma::text as bioma
  from ref.biomas_4326 b
  where b.geom is not null
    and st_intersects(src.geom, b.geom)
  order by b.id
  limit 1
) b on true
left join lateral (
  select u.uc_id::text as uc_id, u.cd_cnuc::text as cd_cnuc, u.nome_uc::text as nome_uc
  from ref.ucs_4326 u
  where u.geom is not null
    and st_intersects(src.geom, u.geom)
  order by u.id
  limit 1
) u on true
left join lateral (
  select
    t.terrai_cod::text as terrai_cod,
    t.terrai_nom::text as terrai_nom,
    t.etnia_nome::text as etnia_nome
  from ref.tis_4326 t
  where t.geom is not null
    and st_intersects(src.geom, t.geom)
  order by t.id
  limit 1
) t on true
where f.event_hash = src.event_hash;
