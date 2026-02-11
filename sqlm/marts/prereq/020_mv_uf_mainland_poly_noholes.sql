create schema if not exists marts;
create materialized view if not exists marts.mv_uf_mainland_poly_noholes as
 WITH parts AS (
         SELECT mv_uf_geom_mainland.uf,
            (st_dump(st_collectionextract(st_makevalid(mv_uf_geom_mainland.geom), 3))).geom AS p
           FROM marts.mv_uf_geom_mainland
        ), ranked AS (
         SELECT parts.uf,
            parts.p,
            row_number() OVER (PARTITION BY parts.uf ORDER BY (st_area(parts.p::geography)) DESC) AS rn
           FROM parts
          WHERE NOT st_isempty(parts.p)
        ), mainland AS (
         SELECT ranked.uf,
            ranked.p
           FROM ranked
          WHERE ranked.rn = 1
        )
 SELECT uf,
    st_makepolygon(st_exteriorring(p))::geometry(Polygon,4326) AS geom
   FROM mainland;
refresh materialized view marts.mv_uf_mainland_poly_noholes;
CREATE INDEX IF NOT EXISTS gix_mv_uf_mainland_poly_noholes_geom ON marts.mv_uf_mainland_poly_noholes USING gist (geom);
CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_uf_mainland_poly_noholes_uf ON marts.mv_uf_mainland_poly_noholes USING btree (uf);
