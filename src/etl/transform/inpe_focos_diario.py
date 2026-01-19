from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _find_col(df: pd.DataFrame, preferred: list[str], contains: list[str]) -> str | None:
    cols = list(df.columns)

    for c in preferred:
        if c in cols:
            return c

    for key in contains:
        for c in cols:
            if key in c:
                return c

    return None


def _to_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce")


def _clean_value(v: Any) -> Any:
    # convert non-json-safe values (nan/na/inf) to none
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None

    if isinstance(v, str):
        vv = v.strip().lower()
        if vv in ("nan", "na", "null", "none", ""):
            return None

    return v


def _json_dumps_safe(d: dict[str, Any]) -> str:
    # ensure valid json for postgres (no nan)
    return json.dumps(d, ensure_ascii=False, default=str, allow_nan=False)


@dataclass(frozen=True)
class Record:
    event_hash: str
    file_date: date
    view_ts: str | None
    satelite: str | None
    municipio: str | None
    estado: str | None
    bioma: str | None
    lat: float
    lon: float
    props_json: str


def transform_inpe_csv(path: str, file_date: date) -> list[Record]:
    # parse, clean, and shape records from INPE CSV
    p = Path(path)

    log.info("transform start | file_date=%s | path=%s", file_date.isoformat(), p.as_posix())
    try:
        df = pd.read_csv(p, sep=None, engine="python", dtype=str)
    except Exception:
        log.exception("read_csv failed | path=%s", p.as_posix())
        raise
    df = _norm_cols(df)

    lat_col = _find_col(df, preferred=["lat", "latitude"], contains=["lat"])
    lon_col = _find_col(df, preferred=["lon", "long", "longitude"], contains=["lon", "long"])
    if not lat_col or not lon_col:
        log.error("missing lat/lon columns | cols=%s", list(df.columns)[:80])
        raise ValueError(f"não encontrei colunas de lat/lon. colunas: {list(df.columns)[:80]}")

    ts_col = _find_col(df, preferred=["datahora", "data_hora_gmt", "data_hora"], contains=["datahora", "hora", "gmt"])
    sat_col = _find_col(df, preferred=["satelite"], contains=["satel"])
    mun_col = _find_col(df, preferred=["municipio"], contains=["municip"])
    uf_col = _find_col(df, preferred=["estado", "uf"], contains=["estado", "uf"])
    bio_col = _find_col(df, preferred=["bioma"], contains=["bioma"])

    log.info(
        "columns picked | lat=%s | lon=%s | ts=%s | sat=%s | mun=%s | uf=%s | bioma=%s",
        lat_col,
        lon_col,
        ts_col,
        sat_col,
        mun_col,
        uf_col,
        bio_col,
    )

    rows_in = len(df)

    df[lat_col] = _to_float(df[lat_col])
    df[lon_col] = _to_float(df[lon_col])

    before_dropna = len(df)
    df = df.dropna(subset=[lat_col, lon_col])
    dropped_na = before_dropna - len(df)

    before_range = len(df)
    df = df[(df[lat_col].between(-90, 90)) & (df[lon_col].between(-180, 180))]
    dropped_range = before_range - len(df)

    log.info(
        "coord filter | rows_in=%s | dropped_na=%s | dropped_range=%s | rows_out=%s",
        rows_in,
        dropped_na,
        dropped_range,
        len(df),
    )

    if len(df) == 0:
        log.warning("no valid rows after coord filter | file_date=%s", file_date.isoformat())
        return []

    recs: list[Record] = []
    seen_hash: set[str] = set()
    dup_count = 0
    json_fallback = 0

    for i, row_raw in enumerate(df.to_dict(orient="records")):
        row = {str(k): v for k, v in row_raw.items()}
        props: dict[str, Any] = {k: _clean_value(v) for k, v in row.items()}

        lat = float(props[lat_col])
        lon = float(props[lon_col])

        view_ts = props.get(ts_col) if ts_col else None
        sat = props.get(sat_col) if sat_col else None

        payload: dict[str, Any] = {
            "file_date": str(file_date),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "view_ts": view_ts,
            "satelite": sat,
        }

        event_hash = hashlib.md5(_json_dumps_safe(payload).encode("utf-8")).hexdigest()

        if event_hash in seen_hash:
            dup_count += 1
            continue
        seen_hash.add(event_hash)

        try:
            props_json = _json_dumps_safe(props)
        except ValueError:
            json_fallback += 1
            props = {str(k): _clean_value(v) for k, v in props.items()}
            props_json = _json_dumps_safe(props)

        recs.append(
            Record(
                event_hash=event_hash,
                file_date=file_date,
                view_ts=view_ts,
                satelite=sat,
                municipio=(props.get(mun_col) if mun_col else None),
                estado=(props.get(uf_col) if uf_col else None),
                bioma=(props.get(bio_col) if bio_col else None),
                lat=lat,
                lon=lon,
                props_json=props_json,
            )
        )


    log.info("transform done | records=%s | dup_in_file=%s | json_fallback=%s", len(recs), dup_count, json_fallback)

    return recs
