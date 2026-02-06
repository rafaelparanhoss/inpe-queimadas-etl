create schema if not exists marts;
create or replace view marts.v_geo_focos_diario_uf_poly_by_day_superset_full_viz as
 SELECT day,
    uf,
    n_focos,
        CASE
            WHEN n_focos = 0 THEN 0.000001
            ELSE n_focos::numeric
        END AS n_focos_viz,
    poly_coords
   FROM marts.v_geo_focos_diario_uf_poly_by_day_superset_full;
