from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from .config import settings

log = logging.getLogger("sql_runner")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_sql_path(sql_path: str) -> Path:
    path = Path(sql_path)
    if not path.is_absolute():
        path = _repo_root() / path
    if not path.exists():
        raise FileNotFoundError(f"sql file not found: {path}")
    return path


def run_sql_file(sql_path: str, vars: dict[str, str] | None = None) -> None:
    path = _resolve_sql_path(sql_path)
    container = os.getenv("DB_CONTAINER", "geoetl_postgis")
    db_user = os.getenv("DB_USER", settings.db_user)
    db_name = os.getenv("DB_NAME", settings.db_name)

    cmd = [
        "docker",
        "exec",
        "-e",
        "PAGER=cat",
        "-i",
        container,
        "psql",
        "-U",
        db_user,
        "-d",
        db_name,
        "-v",
        "ON_ERROR_STOP=1",
    ]

    if vars:
        for key, value in vars.items():
            cmd.extend(["-v", f"{key}={value}"])

    log.info("run sql | path=%s", path)
    with path.open("rb") as handle:
        subprocess.run(cmd, check=True, stdin=handle)
