from __future__ import annotations

from pathlib import Path

from .sql_runner import run_sql_file


def _log(message: str) -> None:
    print(f"[run_marts] {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_marts(date_str: str) -> None:
    repo_root = _repo_root()
    files = [
        repo_root / "sql" / "marts" / "10_focos_diario_municipio.sql",
        repo_root / "sql" / "marts" / "11_focos_mensal_municipio.sql",
        repo_root / "sql" / "marts" / "20_focos_diario_uf.sql",
        repo_root / "sql" / "marts" / "21_focos_mensal_uf.sql",
        repo_root / "sql" / "marts" / "30_focos_diario_uf_trend.sql",
    ]

    for file in files:
        if not file.exists():
            raise FileNotFoundError(f"missing file: {file}")

    for file in files:
        _log(f"run {file.as_posix()} | date={date_str}")
        run_sql_file(str(file), {"DATE": date_str})

    _log(f"done | files={len(files)}")
