from __future__ import annotations

from pathlib import Path

from .ensure_ref_ibge import ensure_ref_ibge
from .sql_runner import run_sql_file


def _log(message: str) -> None:
    print(f"[run_ref] {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_ref() -> None:
    repo_root = _repo_root()
    sql_dir = repo_root / "sql" / "ref"
    files = sorted(sql_dir.glob("*.sql"))
    schema_file = sql_dir / "01_ref_schema.sql"

    if not files:
        raise RuntimeError("no sql/ref files")

    if schema_file.exists():
        _log(f"run {schema_file.as_posix()}")
        run_sql_file(str(schema_file))

    _log("ensure ref ibge")
    ensure_ref_ibge()

    for file in files:
        if file == schema_file:
            continue
        _log(f"run {file.as_posix()}")
        run_sql_file(str(file))

    _log(f"done | files={len(files)}")
