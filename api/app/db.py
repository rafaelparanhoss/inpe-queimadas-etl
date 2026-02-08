from __future__ import annotations

import os
from dataclasses import dataclass

from psycopg_pool import ConnectionPool


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str


def load_db_config() -> DbConfig:
    return DbConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
    )


def make_pool(cfg: DbConfig) -> ConnectionPool:
    dsn = (
        f"host={cfg.host} port={cfg.port} dbname={cfg.name} "
        f"user={cfg.user} password={cfg.password} sslmode={cfg.sslmode}"
    )
    return ConnectionPool(conninfo=dsn, min_size=1, max_size=10, timeout=10)
