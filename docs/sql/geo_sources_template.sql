-- Template de padronizacao de fontes de geometria para a API.
-- Nao executar automaticamente no pipeline.
-- Ajuste os schemas/tabelas de origem conforme seu banco.

-- UF
-- create or replace view public.br_uf as
-- select
--   uf::text as uf,
--   geom
-- from algum_schema.alguma_tabela_uf;

-- Municipios
-- create or replace view public.br_mun as
-- select
--   cd_mun::text as cd_mun,
--   uf::text as uf,
--   geom
-- from algum_schema.alguma_tabela_municipios;

-- Biomas (opcional para /api/bounds?entity=bioma)
-- create or replace view public.br_bioma as
-- select
--   cd_bioma::text as cd_bioma,
--   geom
-- from algum_schema.alguma_tabela_biomas;

-- UCs (opcional para /api/bounds?entity=uc)
-- create or replace view public.br_uc as
-- select
--   cd_cnuc::text as cd_cnuc,
--   geom
-- from algum_schema.alguma_tabela_uc;

-- TIs (opcional para /api/bounds?entity=ti)
-- create or replace view public.br_ti as
-- select
--   terrai_cod::text as terrai_cod,
--   geom
-- from algum_schema.alguma_tabela_ti;

-- Dica para descoberta de tabelas com geometrias:
-- select f_table_schema, f_table_name, f_geometry_column
-- from geometry_columns
-- order by 1,2;
