# Legacy dependency report

Query:
```sql
select
  dependent_ns.nspname as dependent_schema,
  dependent_view.relname as dependent_object,
  source_ns.nspname as source_schema,
  source.relname as source_object
from pg_depend
join pg_rewrite on pg_depend.objid = pg_rewrite.oid
join pg_class dependent_view on pg_rewrite.ev_class = dependent_view.oid
join pg_class source on pg_depend.refobjid = source.oid
join pg_namespace dependent_ns on dependent_view.relnamespace = dependent_ns.oid
join pg_namespace source_ns on source.relnamespace = source_ns.oid
where source_ns.nspname='marts'
  and source.relname in (
    'mv_dim_uf_geom_simpl',
    'mv_focos_uf_day',
    'v_geo_focos_diario_uf_simpl',
    'v_geo_uf_fc_by_day',
    'v_geo_uf_fc_by_day_ok',
    'v_geo_uf_fc_by_day_ok_txt',
    'v_geo_uf_fc_by_day_old3',
    'v_dim_uf_geom_noholes',
    'v_dim_uf_geom_raw',
    'geo_focos_diario_uf',
    'geo_focos_diario_uf_simpl',
    'v_geo_focos_diario_uf_poly_by_day_superset',
    'v_geo_focos_diario_uf_poly_by_day_superset_viz'
  )
order by 1,2,3,4;
```

Results:
```
 dependent_schema |                dependent_object                | source_schema |                 source_object                  
------------------+------------------------------------------------+---------------+------------------------------------------------
 marts            | geo_focos_diario_uf                            | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf
 marts            | geo_focos_diario_uf_simpl                      | marts         | geo_focos_diario_uf_simpl
 marts            | mv_dim_uf_geom_simpl                           | marts         | mv_dim_uf_geom_simpl
 marts            | mv_dim_uf_geom_simpl                           | marts         | v_dim_uf_geom_raw
 marts            | mv_dim_uf_geom_simpl                           | marts         | v_dim_uf_geom_raw
 marts            | mv_focos_uf_day                                | marts         | mv_focos_uf_day
 marts            | v_dim_uf_geom_noholes                          | marts         | v_dim_uf_geom_noholes
 marts            | v_dim_uf_geom_raw                              | marts         | v_dim_uf_geom_raw
 marts            | v_geo_focos_diario_uf_poly_by_day_superset     | marts         | v_geo_focos_diario_uf_poly_by_day_superset
 marts            | v_geo_focos_diario_uf_poly_by_day_superset_viz | marts         | v_geo_focos_diario_uf_poly_by_day_superset_viz
 marts            | v_geo_focos_diario_uf_simpl                    | marts         | mv_focos_uf_day
 marts            | v_geo_focos_diario_uf_simpl                    | marts         | mv_focos_uf_day
 marts            | v_geo_focos_diario_uf_simpl                    | marts         | mv_focos_uf_day
 marts            | v_geo_focos_diario_uf_simpl                    | marts         | v_geo_focos_diario_uf_simpl
 marts            | v_geo_uf_fc_by_day                             | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day                             | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day                             | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day                             | marts         | v_geo_uf_fc_by_day
 marts            | v_geo_uf_fc_by_day_ok                          | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_ok                          | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_ok                          | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_ok                          | marts         | v_geo_uf_fc_by_day_ok
 marts            | v_geo_uf_fc_by_day_ok_txt                      | marts         | v_geo_uf_fc_by_day_ok
 marts            | v_geo_uf_fc_by_day_ok_txt                      | marts         | v_geo_uf_fc_by_day_ok
 marts            | v_geo_uf_fc_by_day_ok_txt                      | marts         | v_geo_uf_fc_by_day_ok
 marts            | v_geo_uf_fc_by_day_ok_txt                      | marts         | v_geo_uf_fc_by_day_ok
 marts            | v_geo_uf_fc_by_day_ok_txt                      | marts         | v_geo_uf_fc_by_day_ok_txt
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | mv_focos_uf_day
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | v_dim_uf_geom_noholes
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | v_dim_uf_geom_noholes
 marts            | v_geo_uf_fc_by_day_old3                        | marts         | v_geo_uf_fc_by_day_old3
(38 rows)

```
