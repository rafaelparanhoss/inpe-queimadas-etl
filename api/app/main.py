from __future__ import annotations

import logging
import os
import re
from datetime import date, timedelta
from typing import Callable, Literal, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from psycopg import errors as pg_errors

from .cache import make_ttl_cache, now_ms
from .db import load_db_config, make_pool
from .geo import to_feature
from .schemas import (
    BoundsResponse,
    ChoroplethWithLegendResponse,
    SummaryResponse,
    TimeseriesResponse,
    TopResponse,
    TotalsResponse,
    ValidateResponse,
)

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("api")

app = FastAPI(title="INPE | Queimadas API", version="0.2.0")

cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["*"],
    )

pool = make_pool(load_db_config())
cache = make_ttl_cache()

TopGroup = Literal["uf", "bioma", "mun", "uc", "ti"]
BoundsEntity = Literal["uf", "mun", "bioma", "uc", "ti"]
TOP_GROUP_EXPR: dict[str, tuple[str, str]] = {
    "uf": ("uf::text", "uf::text"),
    "bioma": ("coalesce(cd_bioma::text, bioma)", "coalesce(bioma, cd_bioma::text)"),
    "mun": ("coalesce(cd_mun::text, mun_nm_mun)", "coalesce(mun_nm_mun, cd_mun::text)"),
    "uc": ("coalesce(cd_cnuc::text, uc_nome)", "coalesce(uc_nome, cd_cnuc::text)"),
    "ti": ("coalesce(terrai_cod::text, ti_nome)", "coalesce(ti_nome, terrai_cod::text)"),
}

CHORO_PALETTE = [
    "#0f172a",
    "#1d4ed8",
    "#2563eb",
    "#0ea5e9",
    "#10b981",
    "#f59e0b",
    "#ef4444",
]
MUN_GUARDRAIL_LIMIT = 10
CHORO_MAX_DAYS_MUN = int(os.getenv("CHORO_MAX_DAYS_MUN", "180"))
CHORO_SIMPLIFY_TOL = float(os.getenv("CHORO_SIMPLIFY_TOL", "0.01"))


def _validate_range(from_date: date, to: date) -> None:
    if from_date >= to:
        raise HTTPException(status_code=400, detail="invalid range: require from < to (to is exclusive)")
    if (to - from_date).days > 3660:
        raise HTTPException(status_code=400, detail="range too large")


def _parse_default_range() -> tuple[date, date]:
    today = date.today()
    to = today + timedelta(days=1)
    from_date = to - timedelta(days=30)
    return from_date, to


def _cache_key(req: Request) -> str:
    return str(req.url)


def _cached(name: str, key: str, run: Callable[[], dict], context: dict[str, object]) -> dict:
    hit = key in cache
    if hit:
        out = cache[key]
    else:
        out = run()
        cache[key] = out
    logger.info("%s cache=%s %s", name, "hit" if hit else "miss", context)
    return out


def _norm_text(value: Optional[str], *, upper: bool = False) -> Optional[str]:
    if value is None:
        return None
    out = value.strip()
    if not out:
        return None
    return out.upper() if upper else out


def _normalize_filters(
    uf: Optional[str],
    bioma: Optional[str],
    mun: Optional[str],
    uc: Optional[str],
    ti: Optional[str],
) -> dict[str, Optional[str]]:
    return {
        "uf": _norm_text(uf, upper=True),
        "bioma": _norm_text(bioma, upper=True),
        "mun": _norm_text(mun, upper=True),
        "uc": _norm_text(uc, upper=True),
        "ti": _norm_text(ti, upper=True),
    }


def _filters_payload(filters: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
    return {
        "uf": filters.get("uf"),
        "bioma": filters.get("bioma"),
        "mun": filters.get("mun"),
        "uc": filters.get("uc"),
        "ti": filters.get("ti"),
    }


def _build_fact_where(
    from_date: date,
    to: date,
    filters: dict[str, Optional[str]],
) -> tuple[str, dict[str, object]]:
    clauses = [
        "day >= %(from)s::date",
        "day < %(to)s::date",
    ]
    params: dict[str, object] = {"from": from_date, "to": to}

    uf = filters.get("uf")
    if uf is not None:
        clauses.append("uf = %(uf)s::text")
        params["uf"] = uf

    bioma = filters.get("bioma")
    if bioma is not None:
        clauses.append(
            "(cd_bioma::text = %(bioma)s::text or upper(coalesce(bioma, '')) = %(bioma)s::text)"
        )
        params["bioma"] = bioma

    mun = filters.get("mun")
    if mun is not None:
        clauses.append(
            "(cd_mun::text = %(mun)s::text or upper(coalesce(mun_nm_mun, '')) = %(mun)s::text)"
        )
        params["mun"] = mun

    uc = filters.get("uc")
    if uc is not None:
        clauses.append(
            "(cd_cnuc::text = %(uc)s::text or upper(coalesce(uc_nome, '')) = %(uc)s::text)"
        )
        params["uc"] = uc

    ti = filters.get("ti")
    if ti is not None:
        clauses.append(
            "(terrai_cod::text = %(ti)s::text or upper(coalesce(ti_nome, '')) = %(ti)s::text)"
        )
        params["ti"] = ti

    return " and ".join(clauses), params


def _quantile(sorted_values: list[int], q: float) -> float:
    if not sorted_values:
        return 0.0
    n = len(sorted_values)
    idx = int(round((n - 1) * q))
    idx = max(0, min(n - 1, idx))
    return float(sorted_values[idx])


def _build_breaks(values: list[int], classes: int = 6) -> tuple[list[float], float, float]:
    if not values:
        return [0.0] * classes, 0.0, 0.0
    min_v = float(min(values))
    max_v = float(max(values))
    positives = sorted(v for v in values if v > 0)
    if not positives:
        return [0.0] * classes, min_v, max_v
    breaks = []
    for i in range(1, classes + 1):
        q = i / classes
        breaks.append(_quantile(positives, q))
    return breaks, min_v, max_v


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str, *, kind: str) -> str:
    if not _IDENT_RE.match(name):
        raise HTTPException(status_code=500, detail=f"invalid {kind} identifier: {name}")
    return name


def _safe_table(name: str) -> str:
    parts = [p for p in name.split(".") if p]
    if not parts:
        raise HTTPException(status_code=500, detail="invalid table identifier")
    return ".".join(_safe_ident(part, kind="table") for part in parts)


def _geo_source(entity: BoundsEntity) -> dict[str, str] | None:
    if entity == "uf":
        table = os.getenv("GEO_UF_TABLE", "").strip()
        key_col = os.getenv("GEO_UF_KEY_COL", "").strip()
        geom_col = os.getenv("GEO_UF_GEOM_COL", "").strip()
        uf_col = ""
    elif entity == "mun":
        table = os.getenv("GEO_MUN_TABLE", "").strip()
        key_col = os.getenv("GEO_MUN_KEY_COL", "").strip()
        geom_col = os.getenv("GEO_MUN_GEOM_COL", "").strip()
        uf_col = os.getenv("GEO_MUN_UF_COL", "").strip()
    elif entity == "bioma":
        table = os.getenv("GEO_BIOMA_TABLE", "").strip()
        key_col = os.getenv("GEO_BIOMA_KEY_COL", "").strip()
        geom_col = os.getenv("GEO_BIOMA_GEOM_COL", "").strip()
        uf_col = ""
    elif entity == "uc":
        table = os.getenv("GEO_UC_TABLE", "").strip()
        key_col = os.getenv("GEO_UC_KEY_COL", "").strip()
        geom_col = os.getenv("GEO_UC_GEOM_COL", "").strip()
        uf_col = ""
    else:
        table = os.getenv("GEO_TI_TABLE", "").strip()
        key_col = os.getenv("GEO_TI_KEY_COL", "").strip()
        geom_col = os.getenv("GEO_TI_GEOM_COL", "").strip()
        uf_col = ""

    if not table or not key_col or not geom_col:
        return None

    out = {
        "table": _safe_table(table),
        "key_col": _safe_ident(key_col, kind="column"),
        "geom_col": _safe_ident(geom_col, kind="column"),
    }
    if uf_col:
        out["uf_col"] = _safe_ident(uf_col, kind="column")
    return out


def _is_geo_source_error(exc: Exception) -> bool:
    return isinstance(exc, (pg_errors.UndefinedTable, pg_errors.UndefinedColumn))


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/choropleth/uf", response_model=ChoroplethWithLegendResponse)
def choropleth_uf(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        sql = f"""
        with agg as (
          select
            uf,
            sum(n_focos)::bigint as n_focos
          from marts.mv_focos_day_dim
          where {where_sql}
          group by uf
        ),
        geom as (
          select distinct on (uf)
            uf,
            poly_coords
          from marts.v_chart_uf_choropleth_day
          where uf is not null
            and poly_coords is not null
          order by uf, day desc
        )
        select
          g.uf,
          coalesce(a.n_focos, 0)::bigint as n_focos,
          (coalesce(a.n_focos, 0)::double precision / greatest(1, (%(to)s::date - %(from)s::date))) as mean_per_day,
          g.poly_coords
        from geom g
        left join agg a on a.uf = g.uf
        order by g.uf;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
                except Exception as exc:  # pragma: no cover - depends on runtime DB schema
                    if _is_geo_source_error(exc):
                        raise HTTPException(status_code=501, detail="geometry source not configured") from exc
                    raise

        features = []
        values = []
        for uf_val, n_focos, mean_per_day, poly_coords in rows:
            n_focos_int = int(n_focos or 0)
            values.append(n_focos_int)
            features.append(
                to_feature(
                    uf=str(uf_val),
                    n_focos=n_focos_int,
                    mean_per_day=float(mean_per_day or 0.0),
                    poly_coords=poly_coords,
                )
            )

        breaks, min_v, max_v = _build_breaks(values, classes=len(CHORO_PALETTE) - 1)
        fc = {"type": "FeatureCollection", "features": features}
        return {
            "from": from_date,
            "to": to,
            "geojson": fc,
            "breaks": breaks,
            "scale": "quantile",
            "palette": CHORO_PALETTE,
            "min": min_v,
            "max": max_v,
        }

    out = _cached(
        "choropleth_uf",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/choropleth/mun", response_model=ChoroplethWithLegendResponse)
def choropleth_mun(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    uf_norm = filters.get("uf")
    if not uf_norm:
        raise HTTPException(status_code=400, detail="uf is required for municipal choropleth")
    if (to - from_date).days > CHORO_MAX_DAYS_MUN:
        raise HTTPException(
            status_code=400,
            detail=f"range too large for municipal choropleth; reduce to <= {CHORO_MAX_DAYS_MUN} days",
        )

    source = _geo_source("mun")
    if source is None:
        raise HTTPException(status_code=501, detail="geometry source not configured")
    if "uf_col" not in source:
        raise HTTPException(status_code=501, detail="geometry source not configured")

    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        params["tol"] = CHORO_SIMPLIFY_TOL
        table = source["table"]
        key_col = source["key_col"]
        uf_col = source["uf_col"]
        geom_col = source["geom_col"]
        sql = f"""
        with agg as (
          select
            cd_mun::text as key,
            max(mun_nm_mun)::text as label,
            sum(n_focos)::bigint as n_focos
          from marts.mv_focos_day_dim
          where {where_sql}
          group by cd_mun
        ),
        g as (
          select
            {key_col}::text as key,
            {uf_col}::text as uf,
            st_simplifypreservetopology({geom_col}, %(tol)s::float8) as geom
          from {table}
          where {uf_col}::text = %(uf)s::text
        )
        select
          g.key,
          g.uf,
          coalesce(a.label, g.key) as label,
          coalesce(a.n_focos, 0)::bigint as n_focos,
          st_asgeojson(g.geom)::jsonb as geom
        from g
        left join agg a using (key)
        order by n_focos desc, g.key;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        days = max(1, (to - from_date).days)
        features = []
        values = []
        for key_val, uf_val, label, n_focos, geom in rows:
            n_focos_int = int(n_focos or 0)
            values.append(n_focos_int)
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "key": str(key_val),
                        "label": str(label),
                        "uf": str(uf_val),
                        "n_focos": n_focos_int,
                        "mean_per_day": float(n_focos_int / days),
                    },
                    "geometry": geom,
                }
            )

        breaks, min_v, max_v = _build_breaks(values, classes=len(CHORO_PALETTE) - 1)
        fc = {"type": "FeatureCollection", "features": features}
        return {
            "from": from_date,
            "to": to,
            "geojson": fc,
            "breaks": breaks,
            "scale": "quantile",
            "palette": CHORO_PALETTE,
            "min": min_v,
            "max": max_v,
            "note": f"municipal layer simplified (tol={CHORO_SIMPLIFY_TOL})",
        }

    out = _cached(
        "choropleth_mun",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/bounds", response_model=BoundsResponse)
def bounds(
    request: Request,
    entity: BoundsEntity = Query(...),
    key: str = Query(...),
    uf: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    key_norm = _norm_text(key)
    if not key_norm:
        raise HTTPException(status_code=400, detail="key is required")

    source = _geo_source(entity)
    if source is None:
        raise HTTPException(status_code=404, detail="geometry source not configured")

    key_cache = _cache_key(request)

    def run():
        params: dict[str, object] = {"key": key_norm}
        table = source["table"]
        key_col = source["key_col"]
        geom_col = source["geom_col"]
        uf_clause = ""
        if entity == "mun" and uf is not None:
            uf_col = source.get("uf_col")
            if not uf_col:
                raise HTTPException(status_code=400, detail="geometry source not configured")
            uf_norm = _norm_text(uf, upper=True)
            if uf_norm:
                params["uf"] = uf_norm
                uf_clause = f" and {uf_col}::text = %(uf)s::text"

        sql = f"""
        select
          st_xmin(ext)::float8 as minx,
          st_ymin(ext)::float8 as miny,
          st_xmax(ext)::float8 as maxx,
          st_ymax(ext)::float8 as maxy,
          st_y(st_centroid(g))::float8 as cy,
          st_x(st_centroid(g))::float8 as cx
        from (
          select
            st_extent({geom_col}) as ext,
            st_collect({geom_col}) as g
          from {table}
          where {key_col}::text = %(key)s::text
          {uf_clause}
        ) s;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, params)
                    row = cur.fetchone()
                except Exception as exc:  # pragma: no cover - depends on runtime DB schema
                    if _is_geo_source_error(exc):
                        raise HTTPException(status_code=404, detail="geometry source not configured") from exc
                    raise

        if not row or row[0] is None or row[1] is None or row[2] is None or row[3] is None:
            raise HTTPException(status_code=404, detail="geometry not found for key")

        minx, miny, maxx, maxy, cy, cx = row
        if cy is None or cx is None:
            cy = (float(miny) + float(maxy)) / 2.0
            cx = (float(minx) + float(maxx)) / 2.0
        return {
            "entity": entity,
            "key": str(key_norm),
            "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
            "center": [float(cy), float(cx)],
        }

    out = _cached(
        "bounds",
        key_cache,
        run,
        {"entity": entity, "key": key_norm, "uf": uf, "ms": now_ms() - t0},
    )
    return out


@app.get("/api/timeseries/total", response_model=TimeseriesResponse)
def timeseries_total(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        sql = f"""
        select
          day,
          sum(n_focos)::bigint as n_focos
        from marts.mv_focos_day_dim
        where {where_sql}
        group by day
        order by day;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return {"items": [{"day": r[0], "n_focos": int(r[1] or 0)} for r in rows]}

    out = _cached(
        "timeseries_total",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/top", response_model=TopResponse)
def top(
    request: Request,
    group: TopGroup = Query(default="uf"),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    if group not in TOP_GROUP_EXPR:
        raise HTTPException(status_code=400, detail="invalid group")

    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        key_expr, label_expr = TOP_GROUP_EXPR[group]
        note: Optional[str] = None
        effective_limit = limit
        if group == "mun" and filters.get("uf") is None:
            effective_limit = min(limit, MUN_GUARDRAIL_LIMIT)
            note = "Top municipios sem UF selecionada: limite aplicado em 10."
        params["limit"] = effective_limit
        sql = f"""
        with ranked as (
          select
            {key_expr} as key,
            {label_expr} as label,
            n_focos
          from marts.mv_focos_day_dim
          where {where_sql}
        )
        select
          key,
          max(label) as label,
          sum(n_focos)::bigint as n_focos
        from ranked
        where key is not null
          and key <> ''
        group by key
        order by n_focos desc, key
        limit %(limit)s;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        items = []
        for k, lbl, v in rows:
            key_val = str(k)
            label_val = str(lbl) if lbl is not None and str(lbl).strip() else key_val
            items.append({"key": key_val, "label": label_val, "n_focos": int(v or 0)})
        return {"group": group, "items": items, "note": note}

    out = _cached(
        "top",
        key,
        run,
        {
            "group": group,
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "limit": limit,
            "ms": now_ms() - t0,
        },
    )
    return out


@app.get("/api/totals", response_model=TotalsResponse)
def totals(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        sql = f"""
        select
          coalesce(sum(n_focos), 0)::bigint as n_focos
        from marts.mv_focos_day_dim
        where {where_sql};
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        return {"n_focos": int(row[0] if row else 0)}

    out = _cached(
        "totals",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/summary", response_model=SummaryResponse)
def summary(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        sql = f"""
        with ts as (
          select
            day,
            sum(n_focos)::bigint as n_focos
          from marts.mv_focos_day_dim
          where {where_sql}
          group by day
        ),
        tot as (
          select
            coalesce(sum(n_focos), 0)::bigint as total_n_focos
          from ts
        ),
        peak as (
          select
            day,
            n_focos
          from ts
          order by n_focos desc, day asc
          limit 1
        )
        select
          t.total_n_focos,
          (t.total_n_focos::double precision / greatest(1, (%(to)s::date - %(from)s::date))) as mean_per_day,
          (%(to)s::date - %(from)s::date) as days,
          p.day as peak_day,
          coalesce(p.n_focos, 0)::bigint as peak_n_focos
        from tot t
        left join peak p on true;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        return {
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "total_n_focos": int(row[0] if row else 0),
            "mean_per_day": float(row[1] if row else 0.0),
            "days": int(row[2] if row else (to - from_date).days),
            "peak_day": row[3] if row else None,
            "peak_n_focos": int(row[4] if row else 0),
        }

    out = _cached(
        "summary",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/validate", response_model=ValidateResponse)
def validate(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    key = _cache_key(request)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        sql = f"""
        with base as (
          select
            day,
            uf,
            n_focos
          from marts.mv_focos_day_dim
          where {where_sql}
        ),
        ts as (
          select
            day,
            sum(n_focos)::bigint as n_focos
          from base
          group by day
        ),
        ufagg as (
          select
            uf,
            sum(n_focos)::bigint as n_focos
          from base
          group by uf
        )
        select
          (select coalesce(sum(n_focos), 0)::bigint from base) as totals_n_focos,
          (select coalesce(sum(n_focos), 0)::bigint from ts) as timeseries_sum_n_focos,
          (select coalesce(sum(n_focos), 0)::bigint from ufagg) as choropleth_sum_n_focos;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()

        totals_n_focos = int(row[0] if row else 0)
        timeseries_sum_n_focos = int(row[1] if row else 0)
        choropleth_sum_n_focos = int(row[2] if row else 0)
        return {
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "totals_n_focos": totals_n_focos,
            "timeseries_sum_n_focos": timeseries_sum_n_focos,
            "choropleth_sum_n_focos": choropleth_sum_n_focos,
            "consistent": totals_n_focos == timeseries_sum_n_focos == choropleth_sum_n_focos,
        }

    out = _cached(
        "validate",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out
