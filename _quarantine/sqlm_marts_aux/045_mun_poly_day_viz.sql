create schema if not exists marts;

create or replace view marts.v_geo_focos_diario_mun_poly_by_day_superset_full_viz as
with base as (
  select
    g.day,
    g.cd_mun,
    g.nm_mun as mun_nm_mun,
    g.n_focos,
    g.geom
  from marts.geo_focos_diario_municipio g
  where g.geom is not null
    and g.n_focos is not null
    and g.n_focos > 0
),
poly as (
  select
    b.day,
    b.cd_mun,
    b.mun_nm_mun,
    b.n_focos,
    (
      select st_makepolygon(st_exteriorring(p.geom))::geometry(Polygon, 4326)
      from (
        select (st_dump(st_collectionextract(st_makevalid(b.geom), 3))).geom as geom
        order by st_area((st_dump(st_collectionextract(st_makevalid(b.geom), 3))).geom) desc
        limit 1
      ) p
    ) as geom_poly
  from base b
)
select
  day,
  cd_mun,
  mun_nm_mun,
  st_asgeojson(geom_poly)::jsonb -> 'coordinates' as poly_coords,
  n_focos::bigint as n_focos,
  case
    when n_focos = 0 then 0.000001
    else n_focos::numeric
  end as n_focos_viz
from poly
where geom_poly is not null;
