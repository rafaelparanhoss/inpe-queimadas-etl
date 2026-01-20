from __future__ import annotations

import logging
import os
import subprocess
import time

from .config import settings

log = logging.getLogger("db_bootstrap")


def _run(cmd: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=capture,
    )


def _docker_exec(container: str, args: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    cmd = ["docker", "exec", "-i", container, *args]
    return _run(cmd, capture=capture)


def _psql_scalar(container: str, sql: str) -> str:
    result = _docker_exec(
        container,
        [
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
            "-c",
            sql,
        ],
        capture=True,
    )
    return result.stdout.strip()


def ensure_database(timeout_sec: int = 60, interval_sec: float = 2.0) -> None:
    container = os.getenv("DB_CONTAINER", "geoetl_postgis")
    db_user = os.getenv("DB_USER", settings.db_user)
    db_name = os.getenv("DB_NAME", settings.db_name)
    db_password = os.getenv("DB_PASSWORD", settings.db_password)

    deadline = time.time() + timeout_sec
    while True:
        try:
            _docker_exec(container, ["pg_isready", "-U", "postgres", "-d", "postgres"])
            break
        except subprocess.CalledProcessError:
            if time.time() >= deadline:
                raise TimeoutError("pg_isready timed out")
            time.sleep(interval_sec)

    role_exists = _psql_scalar(
        container, f"select 1 from pg_roles where rolname = '{db_user}';"
    )
    if role_exists == "1":
        log.info("[db_bootstrap] role exists")
    else:
        _docker_exec(
            container,
            [
                "psql",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"create role {db_user} login password '{db_password}';",
            ],
        )
        log.info("[db_bootstrap] role created")

    db_exists = _psql_scalar(
        container, f"select 1 from pg_database where datname = '{db_name}';"
    )
    if db_exists == "1":
        log.info("[db_bootstrap] db exists")
    else:
        _docker_exec(
            container,
            [
                "psql",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"create database {db_name} owner {db_user};",
            ],
        )
        log.info("[db_bootstrap] db created")

    log.info("[db_bootstrap] ready")
