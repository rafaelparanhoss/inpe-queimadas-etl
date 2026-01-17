from __future__ import annotations

from typing import Iterable, List

import psycopg

from .config import settings
from .transform import Record


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


def ensure_db() -> None:
    with psycopg.connect(_conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()


def _chunks(rows: List[dict], size: int = 5000):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def load_records(records: Iterable[Record], source: str = "inpe_diario_brasil") -> int:
    ensure_db()

    rows: List[dict] = []
    for r in records:
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

    inserted = 0
    with psycopg.connect(_conn_str()) as conn:
        with conn.cursor() as cur:
            for chunk in _chunks(rows, 5000):
                cur.executemany(RAW_SQL, chunk)
                cur.executemany(CURATED_SQL, chunk)
                inserted += len(chunk)
        conn.commit()

    return inserted
