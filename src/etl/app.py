from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from .config import settings
from .checks import run_checks
from .reprocess import run_reprocess
from .report import run_report

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


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


def _run_script(script: Path, args: list[str]) -> None:
    if not script.exists():
        raise FileNotFoundError(f"script not found: {script}")

    bash = _find_bash()
    cmd = [bash, str(script), *args]
    log.info("run script | %s", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=_repo_root())


def _setup_logging() -> Path:
    log_dir = Path(settings.data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "etl.log"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)

    return log_file


def _try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def _validate_date(date_str: str) -> str:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        raise ValueError(f"invalid --date format: {date_str} (expected YYYY-MM-DD)")
    return date_str


def _resolve_today(date_str: str | None) -> str:
    if date_str:
        return _validate_date(date_str)

    if ZoneInfo is not None:
        try:
            tz = ZoneInfo("America/Sao_Paulo")
            return dt.datetime.now(tz).date().isoformat()
        except Exception:
            pass

    return dt.date.today().isoformat()


def cmd_checks(date_str: str | None) -> None:
    if date_str:
        date_str = _validate_date(date_str)
    run_checks(date_str)


def cmd_report(date_str: str) -> None:
    run_report(_validate_date(date_str))


def cmd_reprocess(date_str: str, dry_run: bool) -> None:
    run_reprocess(_validate_date(date_str), dry_run)


def cmd_run(date_str: str, checks: bool) -> None:
    args = ["--date", _validate_date(date_str)]
    if checks:
        args.append("--checks")
    _run_script(_repo_root() / "scripts" / "run_all.sh", args)


def cmd_today(date_str: str | None) -> None:
    resolved = _resolve_today(date_str)
    _run_script(_repo_root() / "scripts" / "run_all.sh", ["--date", resolved])
    cmd_checks(resolved)
    cmd_report(resolved)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="etl command runner")
    sub = parser.add_subparsers(dest="command", required=True)

    checks = sub.add_parser("checks", help="run checks")
    checks.add_argument("--date", help="date in YYYY-MM-DD", required=False)

    report = sub.add_parser("report", help="generate report for a date")
    report.add_argument("--date", help="date in YYYY-MM-DD", required=True)

    reprocess = sub.add_parser("reprocess", help="reprocess a date")
    reprocess.add_argument("--date", help="date in YYYY-MM-DD", required=True)
    reprocess.add_argument("--dry-run", action="store_true", help="print actions only")

    run = sub.add_parser("run", help="run pipeline for a date")
    run.add_argument("--date", help="date in YYYY-MM-DD", required=True)
    run.add_argument("--checks", action="store_true", help="run checks after")

    today = sub.add_parser("today", help="run for today")
    today.add_argument("--date", help="date in YYYY-MM-DD", required=False)

    return parser


def main(argv: list[str] | None = None) -> None:
    _try_load_dotenv()
    _setup_logging()

    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "checks":
            cmd_checks(args.date)
        elif args.command == "report":
            cmd_report(args.date)
        elif args.command == "reprocess":
            cmd_reprocess(args.date, args.dry_run)
        elif args.command == "run":
            cmd_run(args.date, args.checks)
        elif args.command == "today":
            cmd_today(args.date)
        else:
            parser.error(f"unknown command: {args.command}")
    except Exception as exc:
        log.error("command failed | %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
