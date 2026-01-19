from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import psycopg

from ..config import settings
from ..transform.inpe_focos_diario import Record

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


@dataclass(frozen=True)
class LoadResult:
    inserted: int
    attempted: int


# schema and indexes for raw and curated tables
DDL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS raw.inpe_focos (
  event_hash text PRIMARY KEY,
  source text NOT NULL,
  file_date date NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  view_ts text,
  satelite text,
  municipio text,
  estado text,
  bioma text,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  geom geometry(Point,4326),
  props jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_inpe_focos_geom ON raw.inpe_focos USING gist(geom);
CREATE INDEX IF NOT EXISTS idx_raw_inpe_focos_file_date ON raw.inpe_focos (file_date);

CREATE TABLE IF NOT EXISTS curated.inpe_focos (
  event_hash text PRIMARY KEY,
  file_date date NOT NULL,
  view_ts text,
  satelite text,
  municipio text,
  estado text,
  bioma text,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  geom geometry(Point,4326),
  inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_curated_inpe_focos_geom ON curated.inpe_focos USING gist(geom);
CREATE INDEX IF NOT EXISTS idx_curated_inpe_focos_file_date ON curated.inpe_focos (file_date);
"""

RAW_SQL = """
INSERT INTO raw.inpe_focos (
  event_hash, source, file_date, view_ts, satelite, municipio, estado, bioma,
  lat, lon, geom, props
)
VALUES (
  %(event_hash)s, %(source)s, %(file_date)s, %(view_ts)s, %(satelite)s, %(municipio)s, %(estado)s, %(bioma)s,
  %(lat)s, %(lon)s,
  ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
  %(props)s::jsonb
)
ON CONFLICT (event_hash) DO NOTHING;
"""

CURATED_SQL = """
INSERT INTO curated.inpe_focos (
  event_hash, file_date, view_ts, satelite, municipio, estado, bioma,
  lat, lon, geom
)
VALUES (
  %(event_hash)s, %(file_date)s, %(view_ts)s, %(satelite)s, %(municipio)s, %(estado)s, %(bioma)s,
  %(lat)s, %(lon)s,
  ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
)
ON CONFLICT (event_hash) DO NOTHING;
"""


def _conn_str() -> str:
    return (
        f"host={settings.db_host} port={settings.db_port} dbname={settings.db_name} "
        f"user={settings.db_user} password={settings.db_password}"
    )


def _conn_str_safe() -> str:
    return (
        f"host={settings.db_host} port={settings.db_port} dbname={settings.db_name} "
        f"user={settings.db_user}"
    )


# ensure database schemas and tables exist
def ensure_db() -> None:
    t0 = time.perf_counter()
    log.debug("ensure_db start | %s", _conn_str_safe())

    with psycopg.connect(_conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    log.info("ensure_db ok | dt=%.2fs", time.perf_counter() - t0)


def _chunks(rows: list[dict], size: int) -> Iterable[list[dict]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _count_raw_by_file_date(conn: psycopg.Connection, file_dates: list[date]) -> dict[date, int]:
    if not file_dates:
        return {}

    sql = """
    select file_date, count(*)::int as n
    from raw.inpe_focos
    where file_date = any(%s)
    group by file_date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_dates,))
        rows = cur.fetchall()

    out: dict[date, int] = {d: 0 for d in file_dates}
    for d, n in rows:
        out[d] = int(n)
    return out


def load_records(
    records: Iterable[Record],
    source: str = "inpe_diario_brasil",
    chunk_size: int = 5000,
) -> LoadResult:
    t0 = time.perf_counter()

    rec_list = list(records)
    attempted = len(rec_list)
    log.info("load start | records=%s | source=%s | chunk_size=%s", attempted, source, chunk_size)
    if not rec_list:
        log.warning("load skip | empty records")
        return LoadResult(inserted=0, attempted=0)

    ensure_db()

    file_dates = sorted({r.file_date for r in rec_list})
    log.debug("load file_dates | %s", [d.isoformat() for d in file_dates])

    rows: list[dict] = []
    for r in rec_list:
        rows.append(
            {
                "event_hash": r.event_hash,
                "source": source,
                "file_date": r.file_date,
                "view_ts": r.view_ts,
                "satelite": r.satelite,
                "municipio": r.municipio,
                "estado": r.estado,
                "bioma": r.bioma,
                "lat": r.lat,
                "lon": r.lon,
                "props": r.props_json,
            }
        )

    inserted_total = 0
    try:
        with psycopg.connect(_conn_str()) as conn:
            raw_before = _count_raw_by_file_date(conn, file_dates)
            log.info("raw counts (before) | %s", {d.isoformat(): n for d, n in raw_before.items()})

            with conn.cursor() as cur:
                for idx, chunk in enumerate(_chunks(rows, chunk_size), start=1):
                    log.debug("chunk start | idx=%s | size=%s", idx, len(chunk))
                    t_chunk = time.perf_counter()

                    cur.executemany(RAW_SQL, chunk)
                    cur.executemany(CURATED_SQL, chunk)

                    log.debug("chunk ok | idx=%s | dt=%.2fs", idx, time.perf_counter() - t_chunk)

            conn.commit()

            raw_after = _count_raw_by_file_date(conn, file_dates)
            log.info("raw counts (after) | %s", {d.isoformat(): n for d, n in raw_after.items()})

            inserted_total = sum(raw_after[d] - raw_before.get(d, 0) for d in file_dates)

    except Exception:
        log.exception("load failed")
        raise

    log.info(
        "load done | inserted=%s | attempted=%s | dt=%.2fs",
        inserted_total,
        attempted,
        time.perf_counter() - t0,
    )
    return LoadResult(inserted=inserted_total, attempted=attempted)
