from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Optional, Literal

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


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/choropleth/uf", response_model=ChoroplethResponse)
def choropleth_uf(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)

    key = _cache_key(request)

    def run():
        sql = """
        select
          uf,
          sum(n_focos)::bigint as n_focos,
          (sum(n_focos)::double precision / greatest(1, (%(to)s::date - %(from)s::date))) as mean_per_day,
          ((jsonb_agg(poly_coords) filter (where poly_coords is not null)) -> 0) as poly_coords
        from marts.v_chart_uf_choropleth_day
        where day >= %(from)s::date
          and day <  %(to)s::date
        group by uf
        order by uf;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"from": from_date, "to": to})
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
    logger.info("choropleth_uf from=%s to=%s ms=%s", from_date, to, now_ms() - t0)
    return out


@app.get("/api/timeseries/total", response_model=TimeseriesResponse)
def timeseries_total(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    uf_norm = uf.strip().upper() if uf else None

    key = _cache_key(request)

    def run():
        sql = """
        select
          day,
          sum(n_focos)::bigint as n_focos
        from marts.mv_focos_day_dim
        where day >= %(from)s::date
          and day <  %(to)s::date
          and (%(uf)s::text is null or uf = %(uf)s::text)
        group by day
        order by day;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"from": from_date, "to": to, "uf": uf_norm})
                rows = cur.fetchall()

        items = [{"day": r[0], "n_focos": int(r[1] or 0)} for r in rows]
        return {"items": items}

    out = cache_get_or_set(cache, key, run)
    logger.info("timeseries_total from=%s to=%s uf=%s ms=%s", from_date, to, uf_norm, now_ms() - t0)
    return out


@app.get("/api/top", response_model=TopResponse)
def top(
    request: Request,
    group: Literal["uf"] = Query(default="uf"),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)

    if group != "uf":
        raise HTTPException(status_code=400, detail="only group=uf is supported in MVP")

    key = _cache_key(request)

    def run():
        sql = """
        select
          uf as key,
          sum(n_focos)::bigint as n_focos
        from marts.mv_focos_day_dim
        where day >= %(from)s::date
          and day <  %(to)s::date
        group by uf
        order by n_focos desc
        limit %(limit)s;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"from": from_date, "to": to, "limit": limit})
                rows = cur.fetchall()
        items = [{"key": str(k), "n_focos": int(v or 0)} for k, v in rows]
        return {"group": "uf", "items": items}

    out = cache_get_or_set(cache, key, run)
    logger.info("top group=%s from=%s to=%s limit=%s ms=%s", group, from_date, to, limit, now_ms() - t0)
    return out


@app.get("/api/totals", response_model=TotalsResponse)
def totals(
    request: Request,
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    uf_norm = uf.strip().upper() if uf else None

    key = _cache_key(request)

    def run():
        sql = """
        select
          coalesce(sum(n_focos), 0)::bigint as n_focos
        from marts.mv_focos_day_dim
        where day >= %(from)s::date
          and day <  %(to)s::date
          and (%(uf)s::text is null or uf = %(uf)s::text);
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"from": from_date, "to": to, "uf": uf_norm})
                row = cur.fetchone()
        return {"n_focos": int(row[0] if row else 0)}

    out = cache_get_or_set(cache, key, run)
    logger.info("totals from=%s to=%s uf=%s ms=%s", from_date, to, uf_norm, now_ms() - t0)
    return out
