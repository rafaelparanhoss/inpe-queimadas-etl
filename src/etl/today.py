from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path

from .checks import run_checks
from .cli import run as run_cli
from .config import settings
from .db_bootstrap import ensure_database
from .enrich_runner import run_enrich
from .marts_runner import run_marts
from .ref_runner import run_ref
from .report import run_report


def _log(message: str) -> None:
    print(f"[run_today] {message}", flush=True)


class _Tee:
    def __init__(self, *streams) -> None:
        self._streams = streams

    def write(self, value: str) -> int:
        for stream in self._streams:
            stream.write(value)
        return len(value)

    def flush(self) -> None:
        for stream in self._streams:
            stream.flush()


@contextmanager
def _tee_output(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = _Tee(old_stdout, handle)
        sys.stderr = _Tee(old_stderr, handle)
        try:
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


def run_today(date_str: str) -> None:
    log_dir = Path(settings.data_dir) / "logs"
    report_dir = Path(settings.data_dir) / "reports" / date_str
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    _log(f"start | date={date_str}")

    with _tee_output(log_dir / f"run_all_{date_str}.log"):
        ensure_database()
        run_ref()
        run_cli(date_str)
        run_enrich(date_str)
        run_marts(date_str)

    with _tee_output(log_dir / f"checks_{date_str}.log"):
        run_checks(date_str)

    with _tee_output(log_dir / f"report_{date_str}.log"):
        run_report(date_str)

    _log(f"done | date={date_str}")
