from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Literal, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from .db import load_db_config, make_pool
from .cache import make_ttl_cache, cache_get_or_set, now_ms
from .geo import to_feature
from .schemas import (
    TimeseriesResponse,
    TopResponse,
    TotalsResponse,
    ChoroplethResponse,
    SummaryResponse,
    ValidateResponse,
)

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("api")

app = FastAPI(title="INPE | Queimadas API", version="0.1.0")

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
TOP_GROUP_EXPR: dict[str, tuple[str, str]] = {
    "uf": ("uf::text", "uf::text"),
    "bioma": ("coalesce(cd_bioma::text, bioma)", "coalesce(bioma, cd_bioma::text)"),
    "mun": ("coalesce(cd_mun::text, mun_nm_mun)", "coalesce(mun_nm_mun, cd_mun::text)"),
    "uc": ("coalesce(cd_cnuc::text, uc_nome)", "coalesce(uc_nome, cd_cnuc::text)"),
    "ti": ("coalesce(terrai_cod::text, ti_nome)", "coalesce(ti_nome, terrai_cod::text)"),
}
MUN_GUARDRAIL_LIMIT = 10


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


def _filters_payload(filters: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
    return {
        "uf": filters.get("uf"),
        "bioma": filters.get("bioma"),
        "mun": filters.get("mun"),
        "uc": filters.get("uc"),
        "ti": filters.get("ti"),
    }


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/choropleth/uf", response_model=ChoroplethResponse)
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
                cur.execute(sql, params)
                rows = cur.fetchall()

        features = []
        for uf, n_focos, mean_per_day, poly_coords in rows:
            features.append(
                to_feature(
                    uf=str(uf),
                    n_focos=int(n_focos or 0),
                    mean_per_day=float(mean_per_day or 0.0),
                    poly_coords=poly_coords,
                )
            )

        fc = {"type": "FeatureCollection", "features": features}
        return {"from": from_date, "to": to, "features": fc}

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "choropleth_uf from=%s to=%s filters=%s ms=%s",
        from_date,
        to,
        _filters_payload(filters),
        now_ms() - t0,
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

        items = [{"day": r[0], "n_focos": int(r[1] or 0)} for r in rows]
        return {"items": items}

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "timeseries_total from=%s to=%s filters=%s ms=%s",
        from_date,
        to,
        _filters_payload(filters),
        now_ms() - t0,
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

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "top group=%s from=%s to=%s filters=%s limit=%s ms=%s",
        group,
        from_date,
        to,
        _filters_payload(filters),
        limit,
        now_ms() - t0,
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

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "totals from=%s to=%s filters=%s ms=%s",
        from_date,
        to,
        _filters_payload(filters),
        now_ms() - t0,
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

        total_n_focos = int(row[0] if row else 0)
        mean_per_day = float(row[1] if row else 0.0)
        days = int(row[2] if row else (to - from_date).days)
        peak_day = row[3] if row else None
        peak_n_focos = int(row[4] if row else 0)
        return {
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "total_n_focos": total_n_focos,
            "mean_per_day": mean_per_day,
            "days": days,
            "peak_day": peak_day,
            "peak_n_focos": peak_n_focos,
        }

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "summary from=%s to=%s filters=%s ms=%s",
        from_date,
        to,
        _filters_payload(filters),
        now_ms() - t0,
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
        consistent = (
            totals_n_focos == timeseries_sum_n_focos
            and totals_n_focos == choropleth_sum_n_focos
        )
        return {
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "totals_n_focos": totals_n_focos,
            "timeseries_sum_n_focos": timeseries_sum_n_focos,
            "choropleth_sum_n_focos": choropleth_sum_n_focos,
            "consistent": consistent,
        }

    out = cache_get_or_set(cache, key, run)
    logger.info(
        "validate from=%s to=%s filters=%s ms=%s",
        from_date,
        to,
        _filters_payload(filters),
        now_ms() - t0,
    )
    return out
