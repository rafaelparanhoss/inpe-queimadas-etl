create schema if not exists marts;
create or replace view marts.v_geo_focos_diario_uf_poly_by_day_superset_full as
 WITH days AS (
         SELECT DISTINCT focos_diario_uf.day
           FROM marts.focos_diario_uf
        )
 SELECT d.day,
    u.uf,
    COALESCE(f.n_focos, 0::bigint) AS n_focos,
    u.poly_coords
   FROM days d
     CROSS JOIN marts.mv_uf_polycoords_polygon_superset u
     LEFT JOIN marts.focos_diario_uf f ON f.day = d.day AND f.uf = u.uf;
