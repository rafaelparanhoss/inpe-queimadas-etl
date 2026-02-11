create schema if not exists marts;

drop materialized view if exists marts.mv_focos_day_dim;

create materialized view marts.mv_focos_day_dim as
select
  day,
  uf,
  cd_uf,
  cd_mun,
  mun_nm_mun,
  bioma,
  cd_bioma,
  uc_nome,
  cd_cnuc,
  ti_nome,
  terrai_cod,
  count(*)::bigint as n_focos
from marts.v_focos_enriched_full
group by day, uf, cd_uf, cd_mun, mun_nm_mun, bioma, cd_bioma, uc_nome, cd_cnuc, ti_nome, terrai_cod;

create index if not exists idx_mv_focos_day_dim_day on marts.mv_focos_day_dim (day);
create index if not exists idx_mv_focos_day_dim_bioma_day on marts.mv_focos_day_dim (bioma, day);
create index if not exists idx_mv_focos_day_dim_uf_day on marts.mv_focos_day_dim (uf, day);
create index if not exists idx_mv_focos_day_dim_cd_mun_day on marts.mv_focos_day_dim (cd_mun, day);
create index if not exists idx_mv_focos_day_dim_uc_nome on marts.mv_focos_day_dim (uc_nome);
create index if not exists idx_mv_focos_day_dim_ti_nome on marts.mv_focos_day_dim (ti_nome);
