from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from .config import settings


def _log(message: str) -> None:
    print(f"[run_today] {message}", flush=True)


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


def _run_with_tee(cmd: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            cmd,
            cwd=_repo_root(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            handle.write(line)
        returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)


def run_today(date_str: str) -> None:
    log_dir = Path(settings.data_dir) / "logs"
    report_dir = Path(settings.data_dir) / "reports" / date_str
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    _log(f"start | date={date_str}")

    bash = _find_bash()
    run_all_cmd = [bash, str(_repo_root() / "scripts" / "run_all.sh"), "--date", date_str]
    checks_cmd = [bash, str(_repo_root() / "scripts" / "checks.sh"), "--date", date_str]
    report_cmd = [bash, str(_repo_root() / "scripts" / "report_day.sh"), "--date", date_str]

    _run_with_tee(run_all_cmd, log_dir / f"run_all_{date_str}.log")
    _run_with_tee(checks_cmd, log_dir / f"checks_{date_str}.log")
    _run_with_tee(report_cmd, log_dir / f"report_{date_str}.log")

    _log(f"done | date={date_str}")
