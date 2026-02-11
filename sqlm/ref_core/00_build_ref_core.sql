\set ON_ERROR_STOP on

create schema if not exists ref_core;

do $$
declare
  src_uc text;
  src_ti text;
  src_bioma text;
  uc_schema text;
  uc_table text;
  uc_geom text;
  uc_code_expr text;
  uc_name_expr text;
  uc_code_candidates text[] := array['cd_cnuc', 'uc_cd_cnuc', 'id_uc', 'uc_id', 'cd_uc', 'cod_uc'];
  uc_name_candidates text[] := array['nome_uc', 'uc_nome', 'nm_uc', 'nome', 'name'];
  ti_schema text;
  ti_table text;
  ti_geom text;
  ti_code_expr text;
  ti_name_expr text;
  ti_code_candidates text[] := array['ti_cod', 'terrai_cod', 'cod_ti', 'ti_codigo'];
  ti_name_candidates text[] := array['ti_nome', 'terrai_nom', 'nm_ti', 'nome', 'name'];
  bioma_schema text;
  bioma_table text;
  bioma_geom text;
  bioma_code_expr text;
  bioma_name_expr text;
  bioma_code_candidates text[] := array['cd_bioma', 'bioma_cd', 'cod_bioma'];
  bioma_name_candidates text[] := array['bioma', 'bioma_nm', 'nm_bioma', 'nome', 'name'];
  col text;
begin
  -- escolher fonte UC (ordem fixa)
  if to_regclass('ref_core.ucs_4326_sub') is not null then src_uc := 'ref_core.ucs_4326_sub';
  elsif to_regclass('ref_core.ucs_4326') is not null then src_uc := 'ref_core.ucs_4326';
  elsif to_regclass('ref_core.cnuc_uc') is not null then src_uc := 'ref_core.cnuc_uc';
  elsif to_regclass('ref_core.cnuc_2025_08') is not null then src_uc := 'ref_core.cnuc_2025_08';
  elsif to_regclass('ref.ucs_4326_sub') is not null then src_uc := 'ref.ucs_4326_sub';
  elsif to_regclass('ref.ucs_4326') is not null then src_uc := 'ref.ucs_4326';
  elsif to_regclass('ref.cnuc_uc') is not null then src_uc := 'ref.cnuc_uc';
  elsif to_regclass('ref.cnuc_2025_08') is not null then src_uc := 'ref.cnuc_2025_08';
  else raise exception 'nenhuma fonte UC encontrada em schema ref';
  end if;

  -- escolher fonte TI (ordem fixa)
  if to_regclass('ref_core.tis_4326_sub') is not null then src_ti := 'ref_core.tis_4326_sub';
  elsif to_regclass('ref_core.tis_4326') is not null then src_ti := 'ref_core.tis_4326';
  elsif to_regclass('ref_core.tis_poligonaisPolygon') is not null then src_ti := 'ref_core.tis_poligonaisPolygon';
  elsif to_regclass('ref.tis_4326_sub') is not null then src_ti := 'ref.tis_4326_sub';
  elsif to_regclass('ref.tis_4326') is not null then src_ti := 'ref.tis_4326';
  elsif to_regclass('ref.tis_poligonaisPolygon') is not null then src_ti := 'ref.tis_poligonaisPolygon';
  elsif to_regclass('ref.tis_4326') is not null then src_ti := 'ref.tis_4326';
  else raise exception 'nenhuma fonte TI encontrada em schema ref';
  end if;

  -- escolher fonte BIOMA (ordem fixa)
  if to_regclass('ref_core.biomas_4326_sub') is not null then src_bioma := 'ref_core.biomas_4326_sub';
  elsif to_regclass('ref_core.biomas_4326') is not null then src_bioma := 'ref_core.biomas_4326';
  elsif to_regclass('ref.biomas_4326_sub') is not null then src_bioma := 'ref.biomas_4326_sub';
  elsif to_regclass('ref.biomas_4326') is not null then src_bioma := 'ref.biomas_4326';
  else raise exception 'nenhuma fonte BIOMA encontrada em schema ref';
  end if;

  raise notice 'ref_core.uc <= %', src_uc;
  raise notice 'ref_core.ti <= %', src_ti;
  raise notice 'ref_core.bioma <= %', src_bioma;

  uc_schema := split_part(src_uc, '.', 1);
  uc_table := split_part(src_uc, '.', 2);
  ti_schema := split_part(src_ti, '.', 1);
  ti_table := split_part(src_ti, '.', 2);
  bioma_schema := split_part(src_bioma, '.', 1);
  bioma_table := split_part(src_bioma, '.', 2);

  select column_name into uc_geom
  from information_schema.columns
  where table_schema = uc_schema and table_name = uc_table and udt_name = 'geometry'
  order by ordinal_position
  limit 1;

  if uc_geom is null then
    raise exception 'no geometry column in %.%', uc_schema, uc_table;
  end if;

  uc_code_expr := '';
  foreach col in array uc_code_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = uc_schema and table_name = uc_table and column_name = col
    ) then
      if uc_code_expr = '' then
        uc_code_expr := format('nullif(trim(%I::text),'''')', col);
      else
        uc_code_expr := uc_code_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if uc_code_expr = '' then
    raise exception 'no uc code column in %.%', uc_schema, uc_table;
  end if;
  uc_code_expr := 'coalesce(' || uc_code_expr || ')';

  uc_name_expr := '';
  foreach col in array uc_name_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = uc_schema and table_name = uc_table and column_name = col
    ) then
      if uc_name_expr = '' then
        uc_name_expr := format('nullif(trim(%I::text),'''')', col);
      else
        uc_name_expr := uc_name_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if uc_name_expr = '' then
    uc_name_expr := uc_code_expr;
  else
    uc_name_expr := 'coalesce(' || uc_name_expr || ')';
  end if;

  execute 'drop table if exists ref_core.uc cascade';
  execute format($q$
    create table ref_core.uc as
    select
      %s as cd_cnuc,
      %s as nome_uc,
      st_multi(st_collectionextract(st_makevalid(st_transform(%I, 4326)), 3))::geometry(MultiPolygon,4326) as geom
    from %I.%I
    where %I is not null
      and %s is not null
  $q$, uc_code_expr, uc_name_expr, uc_geom, uc_schema, uc_table, uc_geom, uc_code_expr);

  execute 'create index if not exists ix_ref_core_uc_geom on ref_core.uc using gist(geom)';
  execute 'create index if not exists ix_ref_core_uc_cd on ref_core.uc(cd_cnuc)';

  select column_name into ti_geom
  from information_schema.columns
  where table_schema = ti_schema and table_name = ti_table and udt_name = 'geometry'
  order by ordinal_position
  limit 1;

  if ti_geom is null then
    raise exception 'no geometry column in %.%', ti_schema, ti_table;
  end if;

  ti_code_expr := '';
  foreach col in array ti_code_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = ti_schema and table_name = ti_table and column_name = col
    ) then
      if ti_code_expr = '' then
        ti_code_expr := format('nullif(trim(%I::text),'''')', col);
      else
        ti_code_expr := ti_code_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if ti_code_expr = '' then
    raise exception 'no ti code column in %.%', ti_schema, ti_table;
  end if;
  ti_code_expr := 'coalesce(' || ti_code_expr || ')';

  ti_name_expr := '';
  foreach col in array ti_name_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = ti_schema and table_name = ti_table and column_name = col
    ) then
      if ti_name_expr = '' then
        ti_name_expr := format('nullif(trim(%I::text),'''')', col);
      else
        ti_name_expr := ti_name_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if ti_name_expr = '' then
    ti_name_expr := ti_code_expr;
  else
    ti_name_expr := 'coalesce(' || ti_name_expr || ')';
  end if;

  execute 'drop table if exists ref_core.ti cascade';
  execute format($q$
    create table ref_core.ti as
    select
      %s as ti_cod,
      %s as ti_nome,
      st_multi(st_collectionextract(st_makevalid(st_transform(%I, 4326)), 3))::geometry(MultiPolygon,4326) as geom
    from %I.%I
    where %I is not null
      and %s is not null
  $q$, ti_code_expr, ti_name_expr, ti_geom, ti_schema, ti_table, ti_geom, ti_code_expr);

  execute 'create index if not exists ix_ref_core_ti_geom on ref_core.ti using gist(geom)';
  execute 'create index if not exists ix_ref_core_ti_cd on ref_core.ti(ti_cod)';

  select column_name into bioma_geom
  from information_schema.columns
  where table_schema = bioma_schema and table_name = bioma_table and udt_name = 'geometry'
  order by ordinal_position
  limit 1;

  if bioma_geom is null then
    raise exception 'no geometry column in %.%', bioma_schema, bioma_table;
  end if;

  bioma_code_expr := '';
  foreach col in array bioma_code_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = bioma_schema and table_name = bioma_table and column_name = col
    ) then
      if bioma_code_expr = '' then
        bioma_code_expr := format('nullif(trim(%I::text),'''')', col);
      else
        bioma_code_expr := bioma_code_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if bioma_code_expr = '' then
    raise exception 'no bioma code column in %.%', bioma_schema, bioma_table;
  end if;
  bioma_code_expr := 'coalesce(' || bioma_code_expr || ')';

  bioma_name_expr := '';
  foreach col in array bioma_name_candidates loop
    if exists (
      select 1 from information_schema.columns
      where table_schema = bioma_schema and table_name = bioma_table and column_name = col
    ) then
      if bioma_name_expr = '' then
        bioma_name_expr := format('nullif(trim(%I::text),'''')', col);
      else
        bioma_name_expr := bioma_name_expr || format(', nullif(trim(%I::text),'''')', col);
      end if;
    end if;
  end loop;

  if bioma_name_expr = '' then
    bioma_name_expr := bioma_code_expr;
  else
    bioma_name_expr := 'coalesce(' || bioma_name_expr || ')';
  end if;

  execute 'drop table if exists ref_core.bioma cascade';
  execute format($q$
    create table ref_core.bioma as
    select
      %s as cd_bioma,
      %s as bioma,
      st_multi(st_collectionextract(st_makevalid(st_transform(%I, 4326)), 3))::geometry(MultiPolygon,4326) as geom
    from %I.%I
    where %I is not null
  $q$, bioma_code_expr, bioma_name_expr, bioma_geom, bioma_schema, bioma_table, bioma_geom);

  execute 'create index if not exists ix_ref_core_bioma_geom on ref_core.bioma using gist(geom)';
  execute 'create index if not exists ix_ref_core_bioma_cd on ref_core.bioma(cd_bioma)';
end $$;

-- sanity checks
select 'uc' as layer, count(*) as n, min(st_srid(geom)) as srid_min, max(st_srid(geom)) as srid_max from ref_core.uc
union all
select 'ti', count(*), min(st_srid(geom)), max(st_srid(geom)) from ref_core.ti
union all
select 'bioma', count(*), min(st_srid(geom)), max(st_srid(geom)) from ref_core.bioma;

