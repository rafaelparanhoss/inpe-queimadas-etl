from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from .config import settings

log = logging.getLogger("sql_runner")

_TRANSIENT_RE = re.compile(
    r"(connection to server .* failed|the database system is starting up|"
    r"could not connect to server|the database system is shutting down|"
    r"terminating connection due to administrator command)",
    re.IGNORECASE,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_sql_path(sql_path: str) -> Path:
    path = Path(sql_path)
    if not path.is_absolute():
        path = _repo_root() / path
    if not path.exists():
        raise FileNotFoundError(f"sql file not found: {path}")
    return path


def _is_transient_error(returncode: int, output: str) -> bool:
    if returncode == 137:
        return True
    return bool(_TRANSIENT_RE.search(output))


def _summarize_output(output: str, limit: int = 200) -> str:
    cleaned = output.strip().replace("\r", "")
    if not cleaned:
        return "no output"
    first_line = cleaned.splitlines()[0]
    return first_line[:limit]


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
    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        with path.open("rb") as handle:
            result = subprocess.run(
                cmd,
                stdin=handle,
                text=True,
                capture_output=True,
            )
        if result.returncode == 0:
            if result.stdout:
                print(result.stdout, end="")
            if result.stderr:
                print(result.stderr, end="", file=sys.stderr)
            return

        output = "\n".join(part for part in [result.stderr, result.stdout] if part)
        if _is_transient_error(result.returncode, output) and attempt < max_attempts:
            summary = _summarize_output(output)
            log.warning("[sql_runner] retry %s/%s | %s", attempt, max_attempts, summary)
            time.sleep(1.5)
            continue

        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )
