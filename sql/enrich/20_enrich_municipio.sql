-- enrich curated records with municipality data
create schema if not exists curated;

create table if not exists curated.inpe_focos_enriched (
  event_hash text primary key,
  file_date date not null,
  view_ts text,
  satelite text,
  municipio text,
  estado text,
  bioma text,
  lat double precision not null,
  lon double precision not null,
  geom geometry(Point, 4326),
  inserted_at timestamptz not null default now(),
  mun_cd_mun text,
  mun_nm_mun text,
  mun_uf text,
  mun_area_km2 double precision
);

create index if not exists idx_curated_inpe_focos_enriched_geom
  on curated.inpe_focos_enriched using gist (geom);

create index if not exists idx_curated_inpe_focos_enriched_file_date
  on curated.inpe_focos_enriched (file_date);

create index if not exists idx_curated_inpe_focos_enriched_mun_cd_mun
  on curated.inpe_focos_enriched (mun_cd_mun);

insert into curated.inpe_focos_enriched (
  event_hash, file_date, view_ts, satelite, municipio, estado, bioma,
  lat, lon, geom
)
select
  f.event_hash, f.file_date, f.view_ts, f.satelite, f.municipio, f.estado, f.bioma,
  f.lat, f.lon, f.geom
from curated.inpe_focos f
left join curated.inpe_focos_enriched e on e.event_hash = f.event_hash
where e.event_hash is null
  and f.file_date = :'DATE'::date;

update curated.inpe_focos_enriched f
set
  mun_cd_mun = m.cd_mun,
  mun_nm_mun = m.nm_mun,
  mun_uf = m.uf,
  mun_area_km2 = m.area_km2
from ref.ibge_municipios m
where f.mun_cd_mun is null
  and f.file_date = :'DATE'::date
  and f.geom is not null
  and m.geom is not null
  and st_intersects(f.geom, m.geom);

update curated.inpe_focos_enriched f
set
  mun_cd_mun = m.cd_mun,
  mun_nm_mun = m.nm_mun,
  mun_uf = m.uf,
  mun_area_km2 = m.area_km2
from ref.ibge_municipios m
where f.mun_cd_mun is null
  and f.file_date = :'DATE'::date
  and f.geom is not null
  and m.geom is not null
  and m.cd_mun = (
    select m2.cd_mun
    from ref.ibge_municipios m2
    where m2.geom is not null
    order by m2.geom <-> f.geom
    limit 1
  )
  and st_distance(f.geom::geography, m.geom::geography) <= 2000;
