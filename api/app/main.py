from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import unicodedata
from datetime import date, timedelta
from typing import Callable, Literal, Optional

from cachetools import TTLCache
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
    GeoOverlayResponse,
    GeoOverlayQaResponse,
    MunicipalityLookupResponse,
    OptionsResponse,
    PointsResponse,
    SearchResponse,
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
points_cache = TTLCache(maxsize=1024, ttl=int(os.getenv("POINTS_CACHE_TTL_SECONDS", "30")))

TopGroup = Literal["uf", "bioma", "mun", "uc", "ti"]
SearchEntity = Literal["uf", "mun", "bioma", "uc", "ti"]
OptionEntity = Literal["uf", "bioma", "uc", "ti"]
BoundsEntity = Literal["uf", "mun", "bioma", "uc", "ti"]
GeoEntity = Literal["uc", "ti"]
TOP_GROUP_EXPR: dict[str, tuple[str, str]] = {
    "uf": ("uf::text", "uf::text"),
    "bioma": ("coalesce(cd_bioma::text, bioma)", "coalesce(bioma, cd_bioma::text)"),
    "mun": ("coalesce(cd_mun::text, mun_nm_mun)", "coalesce(mun_nm_mun, cd_mun::text)"),
    "uc": ("coalesce(cd_cnuc::text, uc_nome)", "coalesce(uc_nome, cd_cnuc::text)"),
    "ti": ("coalesce(terrai_cod::text, ti_nome)", "coalesce(ti_nome, terrai_cod::text)"),
}
SEARCH_ENTITY_EXPR: dict[str, tuple[str, str, str | None]] = {
    "uf": ("upper(uf)::text", "upper(uf)::text", None),
    "bioma": ("coalesce(cd_bioma::text, upper(bioma))", "coalesce(bioma, cd_bioma::text)", None),
    "mun": ("coalesce(cd_mun::text, mun_nm_mun)", "coalesce(mun_nm_mun, cd_mun::text)", "upper(uf)::text"),
    "uc": ("coalesce(cd_cnuc::text, uc_nome)", "coalesce(uc_nome, cd_cnuc::text)", None),
    "ti": ("coalesce(terrai_cod::text, ti_nome)", "coalesce(ti_nome, terrai_cod::text)", None),
}

CHORO_ZERO_COLOR = "#1a1b2f"
CHORO_QUANTILE_COLORS = [
    "#ffd166",
    "#fca311",
    "#f77f00",
    "#d62828",
    "#5a189a",
]
MUN_GUARDRAIL_LIMIT = 10
MAX_RANGE_DAYS = int(os.getenv("APP_MAX_RANGE_DAYS", "365"))
TS_WEEK_THRESHOLD_DAYS = int(os.getenv("TS_WEEK_THRESHOLD_DAYS", "92"))
TS_MONTH_THRESHOLD_DAYS = int(os.getenv("TS_MONTH_THRESHOLD_DAYS", "273"))
CHORO_MAX_DAYS_MUN = int(os.getenv("CHORO_MAX_DAYS_MUN", "180"))
CHORO_SIMPLIFY_TOL = float(os.getenv("CHORO_SIMPLIFY_TOL", "0.01"))
GEO_SIMPLIFY_DEFAULT_TOL_M = float(os.getenv("GEO_SIMPLIFY_DEFAULT_TOL_M", "10.0"))
POINTS_LIMIT_HARD_CAP = int(os.getenv("POINTS_LIMIT_HARD_CAP", "50000"))
POINTS_LIMIT_DEFAULT = min(int(os.getenv("POINTS_LIMIT_DEFAULT", "20000")), POINTS_LIMIT_HARD_CAP)
POINTS_SOURCE_TABLE = os.getenv("POINTS_SOURCE_TABLE", "marts.v_chart_focos_scatter").strip()
POINTS_SMOKE_LIMIT = int(os.getenv("POINTS_SMOKE_LIMIT", "200"))


def _validate_range(from_date: date, to: date) -> None:
    if from_date >= to:
        raise HTTPException(status_code=400, detail="invalid range: require from < to (to is exclusive)")
    if (to - from_date).days > MAX_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"range too large: max {MAX_RANGE_DAYS} days")


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


def _log_db_encoding_once() -> None:
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW server_encoding;")
                server_row = cur.fetchone()
                cur.execute("SHOW client_encoding;")
                client_row = cur.fetchone()
        server_encoding = str(server_row[0]) if server_row else "unknown"
        client_encoding = str(client_row[0]) if client_row else "unknown"
        logger.info("db_encoding server=%s client=%s", server_encoding, client_encoding)
    except Exception as exc:  # pragma: no cover - startup diagnostics
        logger.warning("db_encoding_check_failed err=%s", exc)


_log_db_encoding_once()


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


def _query_search_items(
    entity: SearchEntity,
    q_value: str,
    limit: int,
    uf: Optional[str] = None,
) -> list[dict[str, object]]:
    if entity not in SEARCH_ENTITY_EXPR:
        raise HTTPException(status_code=400, detail="invalid entity")

    key_expr, label_expr, uf_expr = SEARCH_ENTITY_EXPR[entity]
    uf_select = f", {uf_expr} as uf" if uf_expr else ""
    uf_field = ", uf::text as uf" if uf_expr else ", null::text as uf"
    uf_filter = ""
    params: dict[str, object] = {
        "q": q_value,
        "q_like": f"%{q_value}%",
        "q_prefix": f"{q_value}%",
        "limit": int(limit),
    }

    if entity == "mun":
        if not uf:
            raise HTTPException(status_code=400, detail="uf is required for entity=mun")
        uf_filter = "and upper(uf)::text = %(uf)s::text"
        params["uf"] = uf

    sql = f"""
    with src as (
      select distinct
        {key_expr} as key,
        {label_expr} as label
        {uf_select}
      from marts.mv_focos_day_dim
      where {key_expr} is not null
        and {key_expr} <> ''
        and {label_expr} is not null
        and {label_expr} <> ''
        {uf_filter}
    ),
    filtered as (
      select
        key::text as key,
        label::text as label
        {uf_field}
      from src
      where (
        %(q)s::text = ''
        or key ilike %(q_like)s::text
        or label ilike %(q_like)s::text
      )
    )
    select
      key,
      label,
      uf
    from filtered
    order by
      case when %(q)s::text <> '' and label ilike %(q_prefix)s::text then 0 else 1 end,
      label asc,
      key asc
    limit %(limit)s;
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    geo_labels: dict[str, str] = {}
    if entity in ("uc", "ti"):
        keys = [str(r[0]) for r in rows if r and r[0] is not None]
        geo_labels = _load_geo_labels(entity, keys)

    items: list[dict[str, object]] = []
    for row in rows:
        key_val = str(row[0])
        label_val = str(row[1] if row[1] is not None else key_val)
        if entity in ("uc", "ti"):
            label_val = geo_labels.get(key_val, label_val)
        label_val = _clean_display_label(label_val)
        item: dict[str, object] = {"key": key_val, "label": label_val}
        uf_val = row[2] if len(row) > 2 else None
        if uf_val:
            item["uf"] = str(uf_val)
        items.append(item)

    if entity == "bioma":
        dedup: dict[str, dict[str, object]] = {}
        for item in items:
            label_norm = str(item["label"]).strip().upper()
            current = dedup.get(label_norm)
            if current is None:
                dedup[label_norm] = item
                continue
            cur_key = str(current.get("key", ""))
            new_key = str(item.get("key", ""))
            if (not cur_key.isdigit()) and new_key.isdigit():
                dedup[label_norm] = item
        items = sorted(dedup.values(), key=lambda x: (str(x["label"]), str(x["key"])))

    return items


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


def _is_strictly_increasing(values: list[float]) -> bool:
    if len(values) < 2:
        return False
    for i in range(1, len(values)):
        if not (values[i] > values[i - 1]):
            return False
    return True


def _make_equal_breaks(min_v: float, max_v: float, classes: int) -> list[float]:
    if classes < 1:
        classes = 1
    if max_v <= min_v:
        return [min_v, min_v + 1.0]
    step = (max_v - min_v) / float(classes)
    return [min_v + (step * i) for i in range(classes + 1)]


def _palette_for_breaks(k: int, zero_class: bool) -> list[str]:
    classes = max(1, int(k))
    quantile_colors = CHORO_QUANTILE_COLORS[:classes]
    if len(quantile_colors) < classes:
        quantile_colors += [CHORO_QUANTILE_COLORS[-1]] * (classes - len(quantile_colors))
    if zero_class:
        return [CHORO_ZERO_COLOR] + quantile_colors
    return quantile_colors


def compute_breaks(
    values: list[int],
    method: str = "quantile",
    k: int = 5,
    zero_class: bool = True,
) -> dict[str, object]:
    if method != "quantile":
        raise HTTPException(status_code=500, detail=f"unsupported breaks method: {method}")

    classes = max(1, int(k))
    if not values:
        breaks = [0.0, 1.0]
        return {
            "breaks": breaks,
            "domain": [0.0, 0.0],
            "method": "equal",
            "unit": "focos",
            "zero_class": bool(zero_class),
            "palette": _palette_for_breaks(len(breaks) - 1, bool(zero_class)),
        }

    safe_values = [int(v) for v in values]
    has_zero_or_less = any(v <= 0 for v in safe_values)
    positive_values = sorted(v for v in safe_values if v > 0)
    use_zero_class = bool(zero_class and has_zero_or_less and bool(positive_values))
    method_out: Literal["quantile", "equal"] = "quantile"

    # Zero class is represented separately in legend; quantiles are computed on positive values.
    if use_zero_class and positive_values:
        sample = positive_values
    else:
        sample = sorted(safe_values)

    unique_sample = sorted(set(sample))
    if len(unique_sample) <= 1:
        only = float(unique_sample[0]) if unique_sample else 0.0
        breaks = [only, only + 1.0]
        method_out = "equal"
    else:
        quantile_breaks = [_quantile(sample, i / classes) for i in range(classes + 1)]
        if _is_strictly_increasing(quantile_breaks):
            breaks = quantile_breaks
        else:
            eq_classes = min(classes, max(2, len(unique_sample) - 1))
            breaks = _make_equal_breaks(float(unique_sample[0]), float(unique_sample[-1]), eq_classes)
            method_out = "equal"

    if not _is_strictly_increasing(breaks):
        breaks = _make_equal_breaks(float(min(sample)), float(max(sample)), 1)
        method_out = "equal"

    classes_out = max(1, len(breaks) - 1)
    return {
        "breaks": breaks,
        "domain": [float(min(safe_values)), float(max(safe_values))],
        "method": method_out,
        "unit": "focos",
        "zero_class": use_zero_class,
        "palette": _palette_for_breaks(classes_out, use_zero_class),
    }


def _legend_breaks_monotonic(values: list[int]) -> bool:
    legend = compute_breaks(values, method="quantile", k=5, zero_class=True)
    raw_breaks = legend.get("breaks")
    if not isinstance(raw_breaks, list):
        return False
    breaks = [float(x) for x in raw_breaks]
    return _is_strictly_increasing(breaks)


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


def _fact_entity_columns(entity: GeoEntity) -> tuple[str, str]:
    if entity == "uc":
        return "cd_cnuc", "uc_nome"
    return "terrai_cod", "ti_nome"


def _timeseries_granularity(days: int) -> Literal["day", "week", "month"]:
    if days > TS_MONTH_THRESHOLD_DAYS:
        return "month"
    if days > TS_WEEK_THRESHOLD_DAYS:
        return "week"
    return "day"


def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    raw = (bbox or "").strip()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) != 4:
        raise HTTPException(status_code=422, detail="bbox must be minLon,minLat,maxLon,maxLat")
    try:
        min_lon, min_lat, max_lon, max_lat = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="bbox values must be numeric") from exc

    if min_lon >= max_lon or min_lat >= max_lat:
        raise HTTPException(status_code=422, detail="invalid bbox: require min < max")
    if min_lon < -180 or max_lon > 180 or min_lat < -90 or max_lat > 90:
        raise HTTPException(status_code=422, detail="invalid bbox range")
    return min_lon, min_lat, max_lon, max_lat


def _points_zoom_bucket(bbox: tuple[float, float, float, float]) -> str:
    min_lon, min_lat, max_lon, max_lat = bbox
    extent = max(abs(max_lon - min_lon), abs(max_lat - min_lat))
    if extent >= 45:
        return "world"
    if extent >= 20:
        return "country"
    if extent >= 8:
        return "macro"
    if extent >= 3:
        return "state"
    return "local"


def _build_points_where(
    day_value: date,
    bbox: tuple[float, float, float, float],
    filters: dict[str, Optional[str]],
) -> tuple[str, dict[str, object]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    clauses = [
        "day = %(day)s::date",
        "lon is not null",
        "lat is not null",
        "lon >= %(min_lon)s::float8",
        "lon <= %(max_lon)s::float8",
        "lat >= %(min_lat)s::float8",
        "lat <= %(max_lat)s::float8",
    ]
    params: dict[str, object] = {
        "day": day_value,
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }

    uf = filters.get("uf")
    if uf is not None:
        clauses.append("upper(coalesce(mun_uf, '')) = %(uf)s::text")
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
            "(mun_cd_mun::text = %(mun)s::text or upper(coalesce(mun_nm_mun, '')) = %(mun)s::text)"
        )
        params["mun"] = mun

    uc = filters.get("uc")
    if uc is not None:
        clauses.append(
            "(cd_cnuc::text = %(uc)s::text or upper(coalesce(nome_uc, '')) = %(uc)s::text)"
        )
        params["uc"] = uc

    ti = filters.get("ti")
    if ti is not None:
        clauses.append(
            "(terrai_cod::text = %(ti)s::text or upper(coalesce(terrai_nom, '')) = %(ti)s::text)"
        )
        params["ti"] = ti

    return " and ".join(clauses), params


def _run_points_query(
    day_value: date,
    bbox: tuple[float, float, float, float],
    filters: dict[str, Optional[str]],
    limit: int,
) -> dict[str, object]:
    source = _safe_table(POINTS_SOURCE_TABLE or "marts.v_chart_focos_scatter")
    where_sql, params = _build_points_where(day_value, bbox, filters)
    params["limit_plus_one"] = int(limit) + 1
    sql = f"""
    select
      lon::float8 as lon,
      lat::float8 as lat,
      1::int as n,
      day::date as point_date,
      nullif(trim(event_hash), '')::text as point_id,
      upper(nullif(trim(mun_uf), ''))::text as uf,
      nullif(trim(mun_cd_mun), '')::text as mun_key,
      nullif(trim(mun_nm_mun), '')::text as mun_label,
      nullif(trim(cd_bioma), '')::text as bioma_key,
      nullif(trim(bioma), '')::text as bioma_label,
      nullif(trim(cd_cnuc), '')::text as uc_key,
      nullif(trim(nome_uc), '')::text as uc_label,
      nullif(trim(terrai_cod), '')::text as ti_key,
      nullif(trim(terrai_nom), '')::text as ti_label
    from {source}
    where {where_sql}
    limit %(limit_plus_one)s;
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    truncated = len(rows) > int(limit)
    if truncated:
        rows = rows[: int(limit)]

    points = []
    for row in rows:
        (
            lon,
            lat,
            n,
            point_date,
            point_id,
            uf_val,
            mun_key,
            mun_label,
            bioma_key,
            bioma_label,
            uc_key,
            uc_label,
            ti_key,
            ti_label,
        ) = row
        points.append(
            {
                "lon": float(lon),
                "lat": float(lat),
                "n": int(n or 1),
                "point_date": point_date,
                "point_id": str(point_id) if point_id else None,
                "uf": str(uf_val) if uf_val else None,
                "mun_key": str(mun_key) if mun_key else None,
                "mun_label": _clean_display_label(str(mun_label)) if mun_label else None,
                "bioma_key": str(bioma_key) if bioma_key else None,
                "bioma_label": _clean_display_label(str(bioma_label)) if bioma_label else None,
                "uc_key": str(uc_key) if uc_key else None,
                "uc_label": _clean_display_label(str(uc_label)) if uc_label else None,
                "ti_key": str(ti_key) if ti_key else None,
                "ti_label": _clean_display_label(str(ti_label)) if ti_label else None,
            }
        )

    return {
        "date": day_value,
        "bbox": [float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])],
        "returned": len(points),
        "limit": int(limit),
        "truncated": bool(truncated),
        "points": points,
    }


def _cached_points(key: str, run: Callable[[], dict], context: dict[str, object]) -> dict:
    hit = key in points_cache
    if hit:
        out = points_cache[key]
    else:
        out = run()
        points_cache[key] = out
    logger.info("points cache=%s %s", "hit" if hit else "miss", context)
    return out


def _points_smoke_validate(
    from_date: date,
    to: date,
    filters: dict[str, Optional[str]],
) -> tuple[bool, bool, Optional[date], int]:
    where_sql, params = _build_fact_where(from_date, to, filters)
    test_day: Optional[date] = None
    sql_peak = f"""
    select
      day
    from marts.mv_focos_day_dim
    where {where_sql}
    group by day
    order by sum(n_focos) desc, day asc
    limit 1;
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_peak, params)
            peak_row = cur.fetchone()
            if peak_row and peak_row[0]:
                test_day = peak_row[0]

    if test_day is None:
        test_day = to - timedelta(days=1)
        if test_day < from_date:
            test_day = from_date
    # Small bbox to keep smoke test cheap.
    bbox = (-55.5, -16.5, -54.5, -15.5)
    try:
        result = _run_points_query(test_day, bbox, filters, max(1, POINTS_SMOKE_LIMIT))
        returned = int(result.get("returned", 0))
        le_limit = bool(returned <= int(result.get("limit", 0)))
        return True, le_limit, test_day, returned
    except Exception:
        return False, False, test_day, 0


def _ascii_label(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def _clean_display_label(label: str) -> str:
    out = str(label or "").strip()
    if not out:
        return out

    repl = chr(0xFFFD)
    bad_triplet = "\u00ef\u00bf\u00bd"
    has_mojibake = repl in out or bad_triplet in out

    # Attempt recovery for UTF-8 bytes decoded as latin1.
    try:
        recoded = out.encode("latin1").decode("utf-8")
        if recoded and recoded != out and repl not in recoded:
            out = recoded
            has_mojibake = repl in out or bad_triplet in out
    except UnicodeError:
        pass

    if not has_mojibake:
        return out

    sao = "S\u00e3o"
    sao_low = "s\u00e3o"
    patched = out
    patched = patched.replace(f"S{repl}o", sao).replace(f"s{repl}o", sao_low)
    patched = patched.replace(f"S{bad_triplet}o", sao).replace(f"s{bad_triplet}o", sao_low)
    patched = patched.replace(repl, "a").replace(bad_triplet, "a")
    if repl in patched or bad_triplet in patched:
        patched = _ascii_label(patched)
    return patched


def _coords_hash(geometry: object) -> str:
    payload = json.dumps(geometry, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _load_geo_shape_metrics(
    source: dict[str, str],
    key_norm: str,
    *,
    simplify: bool,
    tol_m: float,
) -> dict[str, object]:
    table = source["table"]
    key_col = source["key_col"]
    geom_col = source["geom_col"]
    sql_exists = f"""
    select 1
    from {table}
    where {key_col}::text = %(key)s::text
    limit 1;
    """
    sql = f"""
    with src as (
      select
        {key_col}::text as key,
        case
          when {geom_col} is null then null
          when st_srid({geom_col}) in (4326, 4674) then st_transform({geom_col}, 4326)
          when st_srid({geom_col}) = 0 then st_setsrid({geom_col}, 4326)
          else st_transform({geom_col}, 4326)
        end as geom_raw
      from {table}
      where {key_col}::text = %(key)s::text
    ),
    parts as (
      select key, geom_raw
      from src
      where geom_raw is not null and not st_isempty(geom_raw)
    ),
    agg as (
      select
        %(key)s::text as key,
        count(*)::int as n_parts_before_union,
        st_collectionextract(
          st_makevalid(
            st_unaryunion(st_collect(geom_raw))
          ),
          3
        ) as base_geom
      from parts
    ),
    base as (
      select
        key,
        n_parts_before_union,
        base_geom,
        st_isvalid(base_geom) as is_valid_before,
        st_npoints(base_geom)::bigint as npoints_before,
        st_area(base_geom::geography)::float8 as area_before
      from agg
      where base_geom is not null and not st_isempty(base_geom)
    ),
    outg as (
      select
        key,
        n_parts_before_union,
        is_valid_before,
        npoints_before,
        area_before,
        case
          when %(simplify)s::int = 1 then
            st_collectionextract(
              st_makevalid(
                st_transform(
                  st_simplifypreservetopology(
                    st_transform(base_geom, 3857),
                    %(tol_m)s::float8
                  ),
                  4326
                )
              ),
              3
            )
          else base_geom
        end as geom_out
      from base
    )
    select
      key,
      n_parts_before_union::int,
      is_valid_before,
      st_isvalid(geom_out) as is_valid_after,
      npoints_before::bigint,
      st_npoints(geom_out)::bigint as npoints_out,
      area_before::float8,
      st_area(geom_out::geography)::float8 as area_after,
      st_xmin(st_envelope(geom_out))::float8 as minx,
      st_ymin(st_envelope(geom_out))::float8 as miny,
      st_xmax(st_envelope(geom_out))::float8 as maxx,
      st_ymax(st_envelope(geom_out))::float8 as maxy,
      st_asgeojson(geom_out)::jsonb as geom_json
    from outg
    where geom_out is not null and not st_isempty(geom_out)
    limit 1;
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_exists, {"key": key_norm})
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="geometry not found for key")
                cur.execute(sql, {"key": key_norm, "simplify": 1 if simplify else 0, "tol_m": tol_m})
                row = cur.fetchone()
            except Exception as exc:  # pragma: no cover - depends on runtime DB schema
                if _is_geo_source_error(exc):
                    raise HTTPException(status_code=404, detail="geometry source not configured") from exc
                raise
    if not row:
        raise HTTPException(status_code=422, detail="geometry is null or invalid for key")

    bbox = [float(row[8]), float(row[9]), float(row[10]), float(row[11])]
    area_after = float(row[7])
    bbox_area = _bbox_area(bbox)
    bbox_ratio = float(bbox_area / max(area_after, 1e-12))
    return {
        "key": str(row[0]),
        "n_parts_before_union": int(row[1]),
        "is_valid_before": bool(row[2]),
        "is_valid_after": bool(row[3]),
        "npoints_before_union": int(row[4]),
        "npoints_out": int(row[5]),
        "area_m2_union_before": float(row[6]),
        "area_m2_union_after": area_after,
        "bbox": bbox,
        "bbox_ratio": bbox_ratio,
        "warning_bbox_ratio": bool(bbox_ratio > 50.0),
        "simplify_applied": bool(simplify),
        "tol_m_used": float(tol_m if simplify else 0.0),
        "geometry": row[12],
        "coords_hash": _coords_hash(row[12]),
    }


def _load_geo_labels(entity: GeoEntity, keys: list[str]) -> dict[str, str]:
    if not keys:
        return {}
    source = _geo_source(entity)
    if source is None:
        return {}

    table = source["table"]
    key_col = source["key_col"]
    sql = f"""
    select
      {key_col}::text as key,
      max(coalesce(label::text, {key_col}::text))::text as label
    from {table}
    where {key_col}::text = any(%(keys)s::text[])
    group by 1;
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"keys": keys})
                rows = cur.fetchall()
            except Exception as exc:  # pragma: no cover - depends on runtime DB schema
                if _is_geo_source_error(exc) or isinstance(exc, pg_errors.UndefinedFunction):
                    return {}
                if isinstance(exc, pg_errors.UndefinedColumn):
                    return {}
                raise

    out: dict[str, str] = {}
    for row in rows:
        key_val = str(row[0])
        label_val = str(row[1]) if row[1] is not None and str(row[1]).strip() else key_val
        out[key_val] = _clean_display_label(label_val)
    return out


def _bbox_area(bbox: list[float]) -> float:
    minx, miny, maxx, maxy = bbox
    w = max(0.0, float(maxx) - float(minx))
    h = max(0.0, float(maxy) - float(miny))
    return w * h


def _bbox_center(bbox: list[float]) -> list[float]:
    minx, miny, maxx, maxy = bbox
    return [((float(miny) + float(maxy)) / 2.0), ((float(minx) + float(maxx)) / 2.0)]


def _load_bounds_bbox(
    entity: BoundsEntity,
    key_norm: str,
    source: dict[str, str],
    uf: Optional[str] = None,
) -> list[float]:
    # Keep UC/TI bounds aligned with /api/geo geometry pipeline and cache key.
    if entity in ("uc", "ti"):
        geometry_key = f"/api/geo/shape?entity={entity}&key={key_norm}&simplify=0&tol_m=0.000000"
        metrics = _cached(
            "geo_overlay_shape",
            geometry_key,
            lambda: _load_geo_shape_metrics(source, key_norm, simplify=False, tol_m=0.0),
            {"entity": entity, "key": key_norm},
        )
        return [float(x) for x in metrics["bbox"]]

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
    with filtered as (
      select
        case
          when {geom_col} is null then null
          when st_srid({geom_col}) in (4326, 4674) then st_transform({geom_col}, 4326)
          when st_srid({geom_col}) = 0 then st_setsrid({geom_col}, 4326)
          else st_transform({geom_col}, 4326)
        end as geom_wgs84
      from {table}
      where {key_col}::text = %(key)s::text
      {uf_clause}
    ),
    agg as (
      select
        st_extent(geom_wgs84) as ext
      from filtered
      where geom_wgs84 is not null
    )
    select
      st_xmin(ext)::float8 as minx,
      st_ymin(ext)::float8 as miny,
      st_xmax(ext)::float8 as maxx,
      st_ymax(ext)::float8 as maxy
    from agg;
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

    return [float(row[0]), float(row[1]), float(row[2]), float(row[3])]


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

        legend = compute_breaks(values, method="quantile", k=5, zero_class=True)
        fc = {"type": "FeatureCollection", "features": features}
        out = {
            "from": from_date,
            "to": to,
            "geojson": fc,
        }
        out.update(legend)
        return out

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

        legend = compute_breaks(values, method="quantile", k=5, zero_class=True)
        fc = {"type": "FeatureCollection", "features": features}
        out = {
            "from": from_date,
            "to": to,
            "geojson": fc,
            "note": f"municipal layer simplified (tol={CHORO_SIMPLIFY_TOL})",
        }
        out.update(legend)
        return out

    out = _cached(
        "choropleth_mun",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out


@app.get("/api/lookup/mun", response_model=MunicipalityLookupResponse)
def lookup_mun(
    request: Request,
    key: str = Query(...),
):
    t0 = now_ms()
    key_norm = _norm_text(key)
    if not key_norm:
        raise HTTPException(status_code=400, detail="key is required")

    source = _geo_source("mun")
    if source is None or "uf_col" not in source:
        raise HTTPException(status_code=404, detail="geometry source not configured")

    cache_key = _cache_key(request)

    def run():
        table = source["table"]
        key_col = source["key_col"]
        uf_col = source["uf_col"]
        sql = f"""
        with gm as (
          select
            {key_col}::text as mun,
            {uf_col}::text as uf
          from {table}
          where {key_col}::text = %(mun)s::text
          limit 1
        ),
        d as (
          select
            cd_mun::text as mun,
            max(mun_nm_mun)::text as mun_nome
          from marts.mv_focos_day_dim
          where cd_mun::text = %(mun)s::text
          group by cd_mun
        )
        select
          gm.mun,
          coalesce(d.mun_nome, gm.mun) as mun_nome,
          upper(gm.uf)::text as uf,
          upper(gm.uf)::text as uf_nome
        from gm
        left join d on d.mun = gm.mun;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, {"mun": key_norm})
                    row = cur.fetchone()
                except Exception as exc:  # pragma: no cover - depends on runtime DB schema
                    if _is_geo_source_error(exc):
                        raise HTTPException(status_code=404, detail="geometry source not configured") from exc
                    raise

        if not row:
            raise HTTPException(status_code=404, detail="municipality not found")

        mun, mun_nome, uf, uf_nome = row
        if not uf:
            raise HTTPException(status_code=404, detail="municipality uf not found")

        return {
            "mun": str(mun),
            "mun_nome": str(mun_nome or mun),
            "uf": str(uf),
            "uf_nome": str(uf_nome or uf),
        }

    out = _cached(
        "lookup_mun",
        cache_key,
        run,
        {"key": key_norm, "ms": now_ms() - t0},
    )
    return out


@app.get("/api/search", response_model=SearchResponse)
def search(
    request: Request,
    entity: SearchEntity = Query(...),
    q: str = Query(default=""),
    uf: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
):
    t0 = now_ms()
    q_norm = (q or "").strip()
    uf_norm = _norm_text(uf, upper=True)
    if entity == "mun" and not uf_norm:
        raise HTTPException(status_code=400, detail="uf is required for entity=mun")

    key = _cache_key(request)

    def run():
        items = _query_search_items(
            entity=entity,
            q_value=q_norm,
            limit=limit,
            uf=uf_norm,
        )
        return {
            "entity": entity,
            "q": q_norm,
            "items": items,
        }

    out = _cached(
        "search",
        key,
        run,
        {"entity": entity, "q": q_norm, "uf": uf_norm, "limit": limit, "ms": now_ms() - t0},
    )
    return out


@app.get("/api/options", response_model=OptionsResponse)
def options(
    request: Request,
    entity: OptionEntity = Query(...),
    limit: int = Query(default=500, ge=1, le=500),
):
    t0 = now_ms()
    key = _cache_key(request)

    def run():
        items = _query_search_items(
            entity=entity,
            q_value="",
            limit=limit,
            uf=None,
        )
        return {
            "entity": entity,
            "items": items,
        }

    out = _cached(
        "options",
        key,
        run,
        {"entity": entity, "limit": limit, "ms": now_ms() - t0},
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
        bbox = _load_bounds_bbox(entity, key_norm, source, uf)
        cy, cx = _bbox_center(bbox)
        return {
            "entity": entity,
            "key": str(key_norm),
            "bbox": bbox,
            "center": [float(cy), float(cx)],
        }

    out = _cached(
        "bounds",
        key_cache,
        run,
        {"entity": entity, "key": key_norm, "uf": uf, "ms": now_ms() - t0},
    )
    return out


@app.get("/api/geo", response_model=GeoOverlayResponse)
def geo_overlay(
    request: Request,
    entity: GeoEntity = Query(...),
    key: str = Query(...),
    simplify: int = Query(default=1, ge=0, le=1),
    tol_m: Optional[float] = Query(default=None, ge=0.0),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to: Optional[date] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    key_norm = _norm_text(key)
    if not key_norm:
        raise HTTPException(status_code=400, detail="key is required")
    if from_date is None or to is None:
        from_date, to = _parse_default_range()
    _validate_range(from_date, to)
    simplify_flag = bool(int(simplify))
    tol_value = float(tol_m if tol_m is not None else GEO_SIMPLIFY_DEFAULT_TOL_M)
    if tol_value < 0:
        raise HTTPException(status_code=400, detail="tol_m must be >= 0")

    source = _geo_source(entity)
    if source is None:
        raise HTTPException(status_code=404, detail="geometry source not configured")

    geometry_key = (
        f"/api/geo/shape?entity={entity}&key={key_norm}"
        f"&simplify={1 if simplify_flag else 0}&tol_m={tol_value:.6f}"
    )

    def run_geometry():
        return _load_geo_shape_metrics(source, key_norm, simplify=simplify_flag, tol_m=tol_value)

    geometry_data = _cached(
        "geo_overlay_shape",
        geometry_key,
        run_geometry,
        {"entity": entity, "key": key_norm, "simplify": simplify_flag, "tol_m": tol_value},
    )

    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    context_filters = dict(filters)
    context_filters[entity] = key_norm
    where_sql, params = _build_fact_where(from_date, to, context_filters)
    key_col, label_col = _fact_entity_columns(entity)
    params["entity_key"] = key_norm

    sql = f"""
    select
      coalesce(max({label_col})::text, %(entity_key)s::text) as label,
      coalesce(sum(n_focos), 0)::bigint as n_focos
    from marts.mv_focos_day_dim
    where {where_sql}
      and {key_col}::text = %(entity_key)s::text;
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

    source_labels = _load_geo_labels(entity, [key_norm])
    source_label = source_labels.get(key_norm)
    fact_label = str((row[0] if row else None) or key_norm)
    label = _clean_display_label(source_label or fact_label)
    n_focos = int(row[1] if row else 0)
    feature = {
        "type": "Feature",
        "properties": {
            "entity": entity,
            "key": key_norm,
            "label": label,
            "n_focos": n_focos,
        },
        "geometry": geometry_data["geometry"],
    }

    out = {
        "entity": entity,
        "key": key_norm,
        "geojson": {"type": "FeatureCollection", "features": [feature]},
    }
    logger.info(
        "geo_overlay entity=%s key=%s filters=%s ms=%s",
        entity,
        key_norm,
        _filters_payload(context_filters),
        now_ms() - t0,
    )
    return out


@app.get("/api/geo/qa", response_model=GeoOverlayQaResponse)
def geo_overlay_qa(
    request: Request,
    entity: GeoEntity = Query(...),
    key: str = Query(...),
    simplify: int = Query(default=1, ge=0, le=1),
    tol_m: Optional[float] = Query(default=None, ge=0.0),
):
    t0 = now_ms()
    key_norm = _norm_text(key)
    if not key_norm:
        raise HTTPException(status_code=400, detail="key is required")
    simplify_flag = bool(int(simplify))
    tol_value = float(tol_m if tol_m is not None else GEO_SIMPLIFY_DEFAULT_TOL_M)
    if tol_value < 0:
        raise HTTPException(status_code=400, detail="tol_m must be >= 0")
    source = _geo_source(entity)
    if source is None:
        raise HTTPException(status_code=404, detail="geometry source not configured")

    geometry_key = (
        f"/api/geo/shape?entity={entity}&key={key_norm}"
        f"&simplify={1 if simplify_flag else 0}&tol_m={tol_value:.6f}"
    )

    metrics = _cached(
        "geo_overlay_shape",
        geometry_key,
        lambda: _load_geo_shape_metrics(source, key_norm, simplify=simplify_flag, tol_m=tol_value),
        {"entity": entity, "key": key_norm, "simplify": simplify_flag, "tol_m": tol_value},
    )
    source_labels = _load_geo_labels(entity, [key_norm])
    source_label = source_labels.get(key_norm)
    out = {
        "entity": entity,
        "key": key_norm,
        "label": _clean_display_label(source_label or str(key_norm)),
        "simplify_param_received": int(simplify),
        "tol_m_received": float(tol_value),
        "simplify_applied": bool(metrics["simplify_applied"]),
        "n_parts_before_union": int(metrics["n_parts_before_union"]),
        "area_m2_union_before": float(metrics["area_m2_union_before"]),
        "area_m2_union_after": float(metrics["area_m2_union_after"]),
        "is_valid_before": bool(metrics["is_valid_before"]),
        "is_valid_after": bool(metrics["is_valid_after"]),
        "npoints_before_union": int(metrics["npoints_before_union"]),
        "npoints_out": int(metrics["npoints_out"]),
        "bbox_ratio": float(metrics["bbox_ratio"]),
        "warning_bbox_ratio": bool(metrics["warning_bbox_ratio"]),
        "coords_hash": str(metrics["coords_hash"]),
        "bbox": [float(x) for x in metrics["bbox"]],
    }
    logger.info("geo_overlay_qa entity=%s key=%s ms=%s", entity, key_norm, now_ms() - t0)
    return out


@app.get("/api/points", response_model=PointsResponse)
def points(
    request: Request,
    day_value: date = Query(..., alias="date"),
    bbox: str = Query(...),
    limit: int = Query(default=POINTS_LIMIT_DEFAULT, ge=1, le=POINTS_LIMIT_HARD_CAP),
    uf: Optional[str] = Query(default=None),
    bioma: Optional[str] = Query(default=None),
    mun: Optional[str] = Query(default=None),
    uc: Optional[str] = Query(default=None),
    ti: Optional[str] = Query(default=None),
):
    t0 = now_ms()
    bbox_tuple = _parse_bbox(bbox)
    filters = _normalize_filters(uf, bioma, mun, uc, ti)
    zoom_bucket = _points_zoom_bucket(bbox_tuple)
    base_key = _cache_key(request)
    key = f"{base_key}|zb={zoom_bucket}"

    def run():
        return _run_points_query(day_value, bbox_tuple, filters, limit)

    out = _cached_points(
        key,
        run,
        {
            "date": day_value,
            "bbox": bbox_tuple,
            "filters": _filters_payload(filters),
            "limit": limit,
            "zoom_bucket": zoom_bucket,
            "ms": now_ms() - t0,
        },
    )
    logger.info(
        "points date=%s bbox=%s filters=%s returned=%s limit=%s truncated=%s ms=%s",
        day_value,
        out.get("bbox"),
        _filters_payload(filters),
        out.get("returned"),
        out.get("limit"),
        out.get("truncated"),
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
    days = (to - from_date).days
    granularity = _timeseries_granularity(days)

    def run():
        where_sql, params = _build_fact_where(from_date, to, filters)
        if granularity == "week":
            bucket_expr = "date_trunc('week', day)::date"
        elif granularity == "month":
            bucket_expr = "date_trunc('month', day)::date"
        else:
            bucket_expr = "day::date"
        sql = f"""
        select
          {bucket_expr} as day_bucket,
          sum(n_focos)::bigint as n_focos
        from marts.mv_focos_day_dim
        where {where_sql}
        group by day_bucket
        order by day_bucket;
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return {
            "granularity": granularity,
            "items": [{"day": r[0], "n_focos": int(r[1] or 0)} for r in rows],
        }

    out = _cached(
        "timeseries_total",
        key,
        run,
        {
            "from": from_date,
            "to": to,
            "granularity": granularity,
            "filters": _filters_payload(filters),
            "ms": now_ms() - t0,
        },
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
        geo_labels: dict[str, str] = {}
        rank_keys = [str(r[0]) for r in rows if r[0] is not None]
        if group == "uc":
            geo_labels = _load_geo_labels("uc", rank_keys)
        elif group == "ti":
            geo_labels = _load_geo_labels("ti", rank_keys)
        items = []
        for k, lbl, v in rows:
            key_val = str(k)
            label_val = str(lbl) if lbl is not None and str(lbl).strip() else key_val
            if group == "uc" or group == "ti":
                label_val = geo_labels.get(key_val, label_val)
            label_val = _clean_display_label(label_val)
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

                cur.execute(
                    f"""
                    select
                      sum(n_focos)::bigint as n_focos
                    from marts.mv_focos_day_dim
                    where {where_sql}
                    group by uf;
                    """,
                    params,
                )
                uf_values = [int(r[0] or 0) for r in cur.fetchall()]

                mun_values: list[int] = []
                if filters.get("uf"):
                    cur.execute(
                        f"""
                        select
                          sum(n_focos)::bigint as n_focos
                        from marts.mv_focos_day_dim
                        where {where_sql}
                        group by cd_mun;
                        """,
                        params,
                    )
                    mun_values = [int(r[0] or 0) for r in cur.fetchall()]

        totals_n_focos = int(row[0] if row else 0)
        timeseries_sum_n_focos = int(row[1] if row else 0)
        choropleth_sum_n_focos = int(row[2] if row else 0)
        invalid_filter_state = bool(filters.get("mun") and not filters.get("uf"))

        break_monotonicity_ok = _legend_breaks_monotonic(uf_values) if uf_values else True
        if mun_values:
            break_monotonicity_ok = break_monotonicity_ok and _legend_breaks_monotonic(mun_values)

        bounds_vs_geo_bbox_ratio: float | None = None
        bounds_consistent: bool | None = None
        qa_entity: GeoEntity | None = None
        qa_key: str | None = None
        if filters.get("ti"):
            qa_entity = "ti"
            qa_key = str(filters["ti"])
        elif filters.get("uc"):
            qa_entity = "uc"
            qa_key = str(filters["uc"])

        if qa_entity and qa_key:
            source = _geo_source(qa_entity)
            if source is not None:
                try:
                    geometry_key = (
                        f"/api/geo/shape?entity={qa_entity}&key={qa_key}&simplify=0&tol_m=0.000000"
                    )
                    metrics = _cached(
                        "geo_overlay_shape",
                        geometry_key,
                        lambda: _load_geo_shape_metrics(source, qa_key, simplify=False, tol_m=0.0),
                        {"entity": qa_entity, "key": qa_key},
                    )
                    geo_bbox = [float(x) for x in metrics["bbox"]]
                    bounds_bbox = _load_bounds_bbox(qa_entity, qa_key, source)
                    geo_area = max(_bbox_area(geo_bbox), 1e-12)
                    bounds_area = max(_bbox_area(bounds_bbox), 1e-12)
                    bounds_vs_geo_bbox_ratio = float(max(bounds_area, geo_area) / min(bounds_area, geo_area))
                    bounds_consistent = bounds_vs_geo_bbox_ratio <= 50.0
                except HTTPException:
                    bounds_consistent = False

        (
            points_endpoint_ok,
            points_returned_le_limit,
            points_date_used,
            points_returned,
        ) = _points_smoke_validate(from_date, to, filters)

        return {
            "from": from_date,
            "to": to,
            "filters": _filters_payload(filters),
            "totals_n_focos": totals_n_focos,
            "timeseries_sum_n_focos": timeseries_sum_n_focos,
            "choropleth_sum_n_focos": choropleth_sum_n_focos,
            "consistent": totals_n_focos == timeseries_sum_n_focos == choropleth_sum_n_focos,
            "invalid_filter_state": invalid_filter_state,
            "break_monotonicity_ok": break_monotonicity_ok,
            "bounds_vs_geo_bbox_ratio": bounds_vs_geo_bbox_ratio,
            "bounds_consistent": bounds_consistent,
            "points_endpoint_ok": points_endpoint_ok,
            "points_returned_le_limit": points_returned_le_limit,
            "points_date_used": points_date_used,
            "points_returned": points_returned,
        }

    out = _cached(
        "validate",
        key,
        run,
        {"from": from_date, "to": to, "filters": _filters_payload(filters), "ms": now_ms() - t0},
    )
    return out
