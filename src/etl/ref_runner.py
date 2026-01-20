from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from .sql_runner import run_sql_file


def _log(message: str) -> None:
    print(f"[run_ref] {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _find_bash() -> str:
    if os.name == "nt":
        candidates = [
            r"C:\Program Files\Git\bin\bash.exe",
            r"C:\Program Files (x86)\Git\bin\bash.exe",
        ]
        for candidate in candidates:
            if Path(candidate).exists():
                return candidate

    bash = shutil.which("bash")
    if bash:
        if os.name != "nt":
            return bash
        if "system32\\bash.exe" not in bash.lower():
            return bash

    raise FileNotFoundError("bash not found on PATH")


def _run_script(script: Path) -> None:
    bash = _find_bash()
    subprocess.run([bash, str(script)], check=True, cwd=_repo_root())


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

    ensure_script = repo_root / "scripts" / "ensure_ref_ibge.sh"
    if ensure_script.exists():
        _log("ensure ref ibge")
        _run_script(ensure_script)

    for file in files:
        if file == schema_file:
            continue
        _log(f"run {file.as_posix()}")
        run_sql_file(str(file))

    _log(f"done | files={len(files)}")
