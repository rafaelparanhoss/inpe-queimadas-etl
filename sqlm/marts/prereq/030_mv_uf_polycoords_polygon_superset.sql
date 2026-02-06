create schema if not exists marts;
create materialized view if not exists marts.mv_uf_polycoords_polygon_superset as
 SELECT uf,
    st_asgeojson(geom)::jsonb -> 'coordinates'::text AS poly_coords
   FROM marts.mv_uf_mainland_poly_noholes;
refresh materialized view marts.mv_uf_polycoords_polygon_superset;
CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_uf_polycoords_polygon_superset_uf ON marts.mv_uf_polycoords_polygon_superset USING btree (uf);
