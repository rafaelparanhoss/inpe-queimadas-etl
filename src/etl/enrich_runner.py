from __future__ import annotations

from pathlib import Path

from .sql_runner import run_sql_file


def _log(message: str) -> None:
    print(f"[run_enrich] {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_enrich(date_str: str) -> None:
    sql_dir = _repo_root() / "sql" / "enrich"
    files = sorted(sql_dir.glob("*.sql"))

    if not files:
        raise RuntimeError("no sql/enrich files")

    for file in files:
        _log(f"run {file.as_posix()} | date={date_str}")
        run_sql_file(str(file), {"DATE": date_str})

    _log(f"done | files={len(files)}")
