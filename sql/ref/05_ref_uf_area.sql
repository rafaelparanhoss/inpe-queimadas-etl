-- aggregate uf area from ibge municipalities
create table if not exists ref.ibge_uf_area (
  uf text not null,
  area_km2 double precision not null
);

create unique index if not exists idx_ref_ibge_uf_area_uf
  on ref.ibge_uf_area (uf);

insert into ref.ibge_uf_area (uf, area_km2)
select
  m.uf,
  coalesce(sum(m.area_km2), 0) as area_km2
from ref.ibge_municipios m
where m.uf is not null
group by m.uf
on conflict (uf) do update set
  area_km2 = excluded.area_km2;
