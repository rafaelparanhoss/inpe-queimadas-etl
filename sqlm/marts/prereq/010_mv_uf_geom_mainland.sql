create schema if not exists marts;
create materialized view if not exists marts.mv_uf_geom_mainland as
 WITH parts AS (
         SELECT ibge_ufs_web.uf,
            (st_dump(st_makevalid(ibge_ufs_web.geom))).geom AS g
           FROM ref_core.ibge_ufs_web
        ), polys AS (
         SELECT parts.uf,
            st_collectionextract(parts.g, 3) AS gpoly
           FROM parts
          WHERE NOT st_isempty(parts.g)
        ), ranked AS (
         SELECT polys.uf,
            polys.gpoly,
            row_number() OVER (PARTITION BY polys.uf ORDER BY (st_area(polys.gpoly::geography)) DESC) AS rn
           FROM polys
          WHERE polys.gpoly IS NOT NULL AND NOT st_isempty(polys.gpoly)
        )
 SELECT uf,
    gpoly::geometry(Polygon,4326) AS geom
   FROM ranked
  WHERE rn = 1;
refresh materialized view marts.mv_uf_geom_mainland;
CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_uf_geom_mainland_uf ON marts.mv_uf_geom_mainland USING btree (uf);
