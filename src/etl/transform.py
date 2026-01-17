# src/etl/transform.py
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd


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
    """
    Converte valores não representáveis em JSON (NaN/NA/inf) em None,
    para permitir json.dumps(..., allow_nan=False).
    """
    # pandas NA/NaT/NaN
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # floats especiais
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None

    # string "NaN"
    if isinstance(v, str) and v.strip().lower() == "nan":
        return None

    return v


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
    df = pd.read_csv(path, sep=None, engine="python", dtype=str)
    df = _norm_cols(df)

    lat_col = _find_col(df, preferred=["lat", "latitude"], contains=["lat"])
    lon_col = _find_col(df, preferred=["lon", "long", "longitude"], contains=["lon", "long"])
    if not lat_col or not lon_col:
        raise ValueError(f"Não encontrei colunas de lat/lon. Colunas: {list(df.columns)[:50]}")

    df[lat_col] = _to_float(df[lat_col])
    df[lon_col] = _to_float(df[lon_col])
    df = df.dropna(subset=[lat_col, lon_col])

    # timestamp (se existir)
    ts_col = _find_col(df, preferred=["datahora", "data_hora_gmt", "data_hora"], contains=["datahora", "hora", "gmt"])
    sat_col = _find_col(df, preferred=["satelite"], contains=["satel"])
    mun_col = _find_col(df, preferred=["municipio"], contains=["municip"])
    uf_col = _find_col(df, preferred=["estado", "uf"], contains=["estado", "uf"])
    bio_col = _find_col(df, preferred=["bioma"], contains=["bioma"])

    recs: list[Record] = []
    for _, row in df.iterrows():
        props: dict[str, Any] = row.to_dict()
        props = {k: _clean_value(v) for k, v in props.items()}

        lat = float(props[lat_col])
        lon = float(props[lon_col])
        view_ts = props.get(ts_col) if ts_col else None
        sat = props.get(sat_col) if sat_col else None

        payload = {
            "file_date": str(file_date),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "view_ts": view_ts,
            "satelite": sat,
        }
        event_hash = hashlib.md5(
            json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        ).hexdigest()

        props_json = json.dumps(props, ensure_ascii=False, default=str, allow_nan=False)

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

    return recs
