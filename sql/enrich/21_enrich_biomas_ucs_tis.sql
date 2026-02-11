begin;

set local jit = off;
set local work_mem = '256MB';
set local maintenance_work_mem = '512MB';
set local synchronous_commit = off;

alter table curated.inpe_focos_enriched
  add column if not exists cd_bioma text,
  add column if not exists bioma text,
  add column if not exists uc_id text,
  add column if not exists cd_cnuc text,
  add column if not exists nome_uc text,
  add column if not exists terrai_cod text,
  add column if not exists terrai_nom text,
  add column if not exists etnia_nome text,
  add column if not exists bioma_checked boolean default false,
  add column if not exists uc_checked boolean default false,
  add column if not exists ti_checked boolean default false;

-- working set do dia (só o que ainda precisa checar)
create temp table tmp_src on commit drop as
select event_hash, geom
from curated.inpe_focos_enriched
where file_date = :'DATE'::date
  and geom is not null
  and (bioma_checked=false or uc_checked=false or ti_checked=false);

create index tmp_src_geom_gix on tmp_src using gist (geom);
analyze tmp_src;

-- biomas (fallback de fonte para ambientes limpos/CI)
do $$
begin
  if to_regclass('ref.biomas_4326_sub') is not null then
    create temp table tmp_bioma on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      b.cd_bioma::text as cd_bioma,
      b.bioma::text as bioma
    from tmp_src s
    join ref.biomas_4326_sub b
      on b.geom is not null
     and s.geom && b.geom
     and st_intersects(s.geom, b.geom)
    order by s.event_hash;
  elsif to_regclass('ref.biomas_4326') is not null then
    create temp table tmp_bioma on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      b.cd_bioma::text as cd_bioma,
      b.bioma::text as bioma
    from tmp_src s
    join ref.biomas_4326 b
      on b.geom is not null
     and s.geom && b.geom
     and st_intersects(s.geom, b.geom)
    order by s.event_hash;
  elsif to_regclass('ref_core.bioma') is not null then
    create temp table tmp_bioma on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      b.cd_bioma::text as cd_bioma,
      b.bioma::text as bioma
    from tmp_src s
    join ref_core.bioma b
      on b.geom is not null
     and s.geom && b.geom
     and st_intersects(s.geom, b.geom)
    order by s.event_hash;
  else
    raise notice 'skip bioma enrich: no source table found';
  end if;
end
$$;

do $$
begin
  if to_regclass('pg_temp.tmp_bioma') is not null then
    update curated.inpe_focos_enriched f
    set
      cd_bioma = coalesce(f.cd_bioma, tb.cd_bioma),
      bioma    = coalesce(f.bioma,    tb.bioma),
      bioma_checked = true
    from tmp_bioma tb
    where f.event_hash = tb.event_hash
      and f.file_date = :'DATE'::date;
  end if;
end
$$;

-- marca como checado mesmo quando não achou bioma (evita reprocessar para sempre)
update curated.inpe_focos_enriched f
set bioma_checked = true
where f.file_date = :'DATE'::date
  and f.geom is not null
  and bioma_checked = false;

-- ucs (fallback de fonte)
do $$
begin
  if to_regclass('ref.ucs_4326_sub') is not null then
    create temp table tmp_uc on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      u.uc_id::text as uc_id,
      u.cd_cnuc::text as cd_cnuc,
      u.nome_uc::text as nome_uc
    from tmp_src s
    join ref.ucs_4326_sub u
      on u.geom is not null
     and s.geom && u.geom
     and st_intersects(s.geom, u.geom)
    order by s.event_hash;
  elsif to_regclass('ref.ucs_4326') is not null then
    create temp table tmp_uc on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      u.uc_id::text as uc_id,
      u.cd_cnuc::text as cd_cnuc,
      u.nome_uc::text as nome_uc
    from tmp_src s
    join ref.ucs_4326 u
      on u.geom is not null
     and s.geom && u.geom
     and st_intersects(s.geom, u.geom)
    order by s.event_hash;
  elsif to_regclass('ref_core.uc') is not null then
    create temp table tmp_uc on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      u.cd_cnuc::text as uc_id,
      u.cd_cnuc::text as cd_cnuc,
      u.nome_uc::text as nome_uc
    from tmp_src s
    join ref_core.uc u
      on u.geom is not null
     and s.geom && u.geom
     and st_intersects(s.geom, u.geom)
    order by s.event_hash;
  elsif to_regclass('ref.cnuc_uc') is not null then
    create temp table tmp_uc on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      u.id_uc::text as uc_id,
      null::text as cd_cnuc,
      u.nome::text as nome_uc
    from tmp_src s
    join ref.cnuc_uc u
      on u.geom is not null
     and s.geom && u.geom
     and st_intersects(s.geom, u.geom)
    order by s.event_hash;
  else
    raise notice 'skip uc enrich: no source table found';
  end if;
end
$$;

do $$
begin
  if to_regclass('pg_temp.tmp_uc') is not null then
    update curated.inpe_focos_enriched f
    set
      uc_id   = coalesce(f.uc_id,   tu.uc_id),
      cd_cnuc = coalesce(f.cd_cnuc, tu.cd_cnuc),
      nome_uc = coalesce(f.nome_uc, tu.nome_uc),
      uc_checked = true
    from tmp_uc tu
    where f.event_hash = tu.event_hash
      and f.file_date = :'DATE'::date;
  end if;
end
$$;

update curated.inpe_focos_enriched f
set uc_checked = true
where f.file_date = :'DATE'::date
  and f.geom is not null
  and uc_checked = false;

-- tis (fallback de fonte)
do $$
begin
  if to_regclass('ref.tis_4326_sub') is not null then
    create temp table tmp_ti on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      t.terrai_cod::text as terrai_cod,
      t.terrai_nom::text as terrai_nom,
      t.etnia_nome::text as etnia_nome
    from tmp_src s
    join ref.tis_4326_sub t
      on t.geom is not null
     and s.geom && t.geom
     and st_intersects(s.geom, t.geom)
    order by s.event_hash;
  elsif to_regclass('ref.tis_4326') is not null then
    create temp table tmp_ti on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      t.terrai_cod::text as terrai_cod,
      t.terrai_nom::text as terrai_nom,
      t.etnia_nome::text as etnia_nome
    from tmp_src s
    join ref.tis_4326 t
      on t.geom is not null
     and s.geom && t.geom
     and st_intersects(s.geom, t.geom)
    order by s.event_hash;
  elsif to_regclass('ref_core.ti') is not null then
    create temp table tmp_ti on commit drop as
    select distinct on (s.event_hash)
      s.event_hash,
      t.ti_cod::text as terrai_cod,
      t.ti_nome::text as terrai_nom,
      null::text as etnia_nome
    from tmp_src s
    join ref_core.ti t
      on t.geom is not null
     and s.geom && t.geom
     and st_intersects(s.geom, t.geom)
    order by s.event_hash;
  else
    raise notice 'skip ti enrich: no source table found';
  end if;
end
$$;

do $$
begin
  if to_regclass('pg_temp.tmp_ti') is not null then
    update curated.inpe_focos_enriched f
    set
      terrai_cod = coalesce(f.terrai_cod, tt.terrai_cod),
      terrai_nom = coalesce(f.terrai_nom, tt.terrai_nom),
      etnia_nome = coalesce(f.etnia_nome, tt.etnia_nome),
      ti_checked = true
    from tmp_ti tt
    where f.event_hash = tt.event_hash
      and f.file_date = :'DATE'::date;
  end if;
end
$$;

update curated.inpe_focos_enriched f
set ti_checked = true
where f.file_date = :'DATE'::date
  and f.geom is not null
  and ti_checked = false;

commit;
