-- 21_enrich_biomas_ucs_tis.sql (set-based + checked flags)

begin;

set local statement_timeout = '0';
set local synchronous_commit = off;
set local work_mem = '256MB';
set local max_parallel_workers_per_gather = 4;

alter table curated.inpe_focos_enriched
  add column if not exists cd_bioma text,
  add column if not exists bioma text,
  add column if not exists uc_id text,
  add column if not exists cd_cnuc text,
  add column if not exists nome_uc text,
  add column if not exists terrai_cod text,
  add column if not exists terrai_nom text,
  add column if not exists etnia_nome text,
  add column if not exists bioma_checked boolean,
  add column if not exists uc_checked boolean,
  add column if not exists ti_checked boolean;

-- defaults (evita coalesce e facilita index parcial)
alter table curated.inpe_focos_enriched
  alter column bioma_checked set default false,
  alter column uc_checked set default false,
  alter column ti_checked set default false;

with src as (
  select
    event_hash,
    geom
  from curated.inpe_focos_enriched
  where file_date = :'DATE'::date
    and geom is not null
    and (
      coalesce(bioma_checked,false) = false
      or coalesce(uc_checked,false) = false
      or coalesce(ti_checked,false) = false
    )
),
bioma_hit as (
  select distinct on (s.event_hash)
    s.event_hash,
    b.cd_bioma::text as cd_bioma,
    b.bioma::text as bioma
  from src s
  join ref.biomas_4326 b
    on b.geom is not null
   and b.geom && s.geom
   and st_intersects(b.geom, s.geom)
  order by s.event_hash, b.id
),
uc_hit as (
  select distinct on (s.event_hash)
    s.event_hash,
    u.uc_id::text as uc_id,
    u.cd_cnuc::text as cd_cnuc,
    u.nome_uc::text as nome_uc
  from src s
  join ref.ucs_4326 u
    on u.geom is not null
   and u.geom && s.geom
   and st_intersects(u.geom, s.geom)
  order by s.event_hash, u.id
),
ti_hit as (
  select distinct on (s.event_hash)
    s.event_hash,
    t.terrai_cod::text as terrai_cod,
    t.terrai_nom::text as terrai_nom,
    t.etnia_nome::text as etnia_nome
  from src s
  join ref.tis_4326 t
    on t.geom is not null
   and t.geom && s.geom
   and st_intersects(t.geom, s.geom)
  order by s.event_hash, t.id
)
update curated.inpe_focos_enriched f
set
  cd_bioma   = coalesce(f.cd_bioma, bh.cd_bioma),
  bioma      = coalesce(f.bioma, bh.bioma),
  uc_id      = coalesce(f.uc_id, uh.uc_id),
  cd_cnuc    = coalesce(f.cd_cnuc, uh.cd_cnuc),
  nome_uc    = coalesce(f.nome_uc, uh.nome_uc),
  terrai_cod = coalesce(f.terrai_cod, th.terrai_cod),
  terrai_nom = coalesce(f.terrai_nom, th.terrai_nom),
  etnia_nome = coalesce(f.etnia_nome, th.etnia_nome),
  -- checked significa “já verifiquei”, não “tenho interseção”
  bioma_checked = true,
  uc_checked    = true,
  ti_checked    = true
from src
left join bioma_hit bh on bh.event_hash = src.event_hash
left join uc_hit    uh on uh.event_hash = src.event_hash
left join ti_hit    th on th.event_hash = src.event_hash
where f.event_hash = src.event_hash;

commit;
