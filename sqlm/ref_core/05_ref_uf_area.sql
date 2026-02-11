-- aggregate uf area from ibge municipalities
create table if not exists ref_core.ibge_uf_area (
  uf text not null,
  area_km2 double precision not null
);

create unique index if not exists idx_ref_core_ibge_uf_area_uf
  on ref_core.ibge_uf_area (uf);

insert into ref_core.ibge_uf_area (uf, area_km2)
select
  m.uf,
  coalesce(sum(m.area_km2), 0) as area_km2
from ref_core.ibge_municipios m
where m.uf is not null
group by m.uf
on conflict (uf) do update set
  area_km2 = excluded.area_km2;
