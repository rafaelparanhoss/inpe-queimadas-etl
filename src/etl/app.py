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
from .backfill import run_backfill
from .checks import run_checks
from .db_bootstrap import ensure_database
from .enrich_runner import run_enrich
from .marts_runner import run_marts
from .ref_runner import run_ref
from . import validate_marts

import psycopg
from psycopg import sql as psql

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


def _run_cli(date_str: str, no_cache: bool = False) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    uv_bin = shutil.which("uv")
    extra = ["--no-cache"] if no_cache else []
    if uv_bin:
        cmd = [uv_bin, "run", "python", "-m", "etl.cli", "--date", date_str, *extra]
    else:
        cmd = [sys.executable, "-m", "uv", "run", "python", "-m", "etl.cli", "--date", date_str, *extra]
    log.info("run cli | %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, cwd=_repo_root(), env=env)
        return
    except Exception as exc:
        log.warning("uv run failed, fallback to python -m etl.cli | err=%s", exc)
        fallback = [sys.executable, "-m", "etl.cli", "--date", date_str, *extra]
        subprocess.run(fallback, check=True, cwd=_repo_root(), env=env)


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

def cmd_checks(date_str: str | None) -> None:
    if date_str:
        date_str = _validate_date(date_str)
    run_checks(date_str)


def _drop_schemas(
    engine: str | None,
    drop_raw: bool,
    drop_curated: bool,
    drop_marts: bool,
) -> None:
    targets: list[str] = []
    if drop_raw:
        targets.append("raw")
    if drop_curated:
        targets.append("curated")
    if drop_marts:
        targets.append("marts")
    if not targets:
        log.info("drop schemas | skipped (none selected)")
        return

    sql = " ".join([f"drop schema if exists {name} cascade;" for name in targets])
    log.info("drop schemas | %s", ", ".join(targets))
    if engine == "docker":
        container = os.getenv("DB_CONTAINER", "geoetl_postgis")
        db_user = os.getenv("DB_USER", settings.db_user)
        db_name = os.getenv("DB_NAME", settings.db_name)
        cmd = [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "-U",
            db_user,
            "-d",
            db_name,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ]
        subprocess.run(cmd, check=True)
        return

    with psycopg.connect(
        host=os.getenv("DB_HOST", settings.db_host),
        port=os.getenv("DB_PORT", settings.db_port),
        dbname=os.getenv("DB_NAME", settings.db_name),
        user=os.getenv("DB_USER", settings.db_user),
        password=os.getenv("DB_PASSWORD", settings.db_password),
    ) as conn, conn.cursor() as cur:
        for name in targets:
            stmt = psql.SQL("drop schema if exists {} cascade;").format(psql.Identifier(name))
            cur.execute(stmt)
        conn.commit()


def _reset_state_files() -> None:
    state_dir = Path(settings.data_dir) / "state"
    if not state_dir.exists():
        return
    for path in state_dir.glob("*.json"):
        path.unlink()


def _clear_raw_cache() -> None:
    raw_dir = Path(settings.data_dir) / "raw"
    if not raw_dir.exists():
        return
    log.info("clear raw cache | path=%s", raw_dir.as_posix())
    shutil.rmtree(raw_dir, ignore_errors=True)


def _smoke_superset_objects() -> None:
    conn = psycopg.connect(
        host=os.getenv("DB_HOST", settings.db_host),
        port=os.getenv("DB_PORT", settings.db_port),
        dbname=os.getenv("DB_NAME", settings.db_name),
        user=os.getenv("DB_USER", settings.db_user),
        password=os.getenv("DB_PASSWORD", settings.db_password),
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            select
              to_regclass('marts.mv_focos_day_dim'),
              to_regclass('marts.v_chart_focos_scatter'),
              to_regclass('marts.v_chart_uf_choropleth_day'),
              to_regclass('marts.v_chart_mun_choropleth_day');
            """
        )
        obj_row = cur.fetchone()
        if not obj_row or any(v is None for v in obj_row):
            raise RuntimeError(f"superset objects missing | got={obj_row}")
        log.info(
            "smoke objects ok | mv=%s | scatter=%s | uf=%s | mun=%s",
            *obj_row,
        )

        cur.execute(
            """
            select day, count(*) as rows, count(*) filter (where poly_coords is null) as null_poly
            from marts.v_chart_uf_choropleth_day
            where day = (select max(day) from marts.v_chart_uf_choropleth_day)
            group by day;
            """
        )
        uf_row = cur.fetchone()
        log.info("smoke uf | day=%s | rows=%s | null_poly=%s", *uf_row)

        cur.execute(
            """
            select day, count(distinct cd_mun) as features, count(*) as rows,
                   count(*) filter (where poly_coords is null) as null_poly
            from marts.v_chart_mun_choropleth_day
            where day = (select max(day) from marts.v_chart_mun_choropleth_day)
            group by day;
            """
        )
        mun_row = cur.fetchone()
        log.info("smoke mun | day=%s | features=%s | rows=%s | null_poly=%s", *mun_row)

        cur.execute(
            """
            select day, count(*) as rows
            from marts.v_chart_focos_scatter
            where day = (select max(day) from marts.v_chart_focos_scatter)
            group by day;
            """
        )
        scatter_row = cur.fetchone()
        log.info("smoke scatter | day=%s | rows=%s", *scatter_row)


def _run_validate_marts(engine: str | None, date_str: str | None = None) -> None:
    args: list[str] = ["--apply-minimal"]
    if engine:
        args += ["--engine", engine]
    if date_str:
        args += ["--date", date_str]
    validate_marts.main(args)


def cmd_run(
    date_str: str | None,
    checks: bool,
    engine: str | None,
    start_str: str | None = None,
    end_str: str | None = None,
    from_scratch: bool = False,
    reset_state: bool = False,
    no_cache: bool = False,
    clear_raw_cache: bool = False,
    mode: str = "dashboard",
) -> None:
    if start_str or end_str:
        if not start_str or not end_str:
            raise ValueError("--start and --end must be provided together")
        start_str = _validate_date(start_str)
        end_str = _validate_date(end_str)
        if from_scratch:
            _drop_schemas(engine, drop_raw=True, drop_curated=True, drop_marts=True)
        if from_scratch or reset_state:
            _reset_state_files()
        if clear_raw_cache:
            _clear_raw_cache()
        backfill_checks = checks if mode != "dashboard" else False
        run_backfill(
            start_str,
            end_str,
            backfill_checks,
            resume=False,
            engine=engine,
            no_cache=no_cache,
        )
        if mode == "dashboard":
            _run_validate_marts(engine)
            if checks:
                _smoke_superset_objects()
        return

    if not date_str:
        raise ValueError("missing --date")
    date_str = _validate_date(date_str)
    if from_scratch:
        _drop_schemas(engine, drop_raw=True, drop_curated=True, drop_marts=True)
    if from_scratch or reset_state:
        _reset_state_files()
    if clear_raw_cache:
        _clear_raw_cache()
    ensure_database(engine=engine)
    run_ref(engine=engine)
    _run_cli(date_str, no_cache=no_cache)
    run_enrich(date_str, engine=engine)
    run_marts(date_str, engine=engine)
    if mode == "dashboard":
        _run_validate_marts(engine)
        if checks:
            _smoke_superset_objects()
    elif checks:
        run_checks(date_str)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="etl command runner")
    sub = parser.add_subparsers(dest="command", required=True)

    ref = sub.add_parser("ref", help="run ref sql and reference data")
    ref.add_argument("--date", help="date in YYYY-MM-DD", required=False)
    ref.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    backfill = sub.add_parser("backfill", help="run backfill for a date range")
    backfill.add_argument("--start", help="start date in YYYY-MM-DD", required=True)
    backfill.add_argument("--end", help="end date in YYYY-MM-DD", required=True)
    backfill.add_argument("--checks", action="store_true", help="run checks per day")
    backfill.add_argument("--resume", action="store_true", help="resume from state file")
    backfill.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    checks = sub.add_parser("checks", help="run checks")
    checks.add_argument("--date", help="date in YYYY-MM-DD", required=False)
    checks.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    enrich = sub.add_parser("enrich", help="run enrich sql for a date")
    enrich.add_argument("--date", help="date in YYYY-MM-DD", required=True)
    enrich.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    marts = sub.add_parser("marts", help="run marts sql for a date")
    marts.add_argument("--date", help="date in YYYY-MM-DD", required=True)
    marts.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    reset = sub.add_parser("reset", help="drop schemas and clear local state")
    reset.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")
    reset.add_argument("--drop-raw", action="store_true", help="drop raw schema")
    reset.add_argument("--drop-curated", action="store_true", help="drop curated schema")
    reset.add_argument("--drop-marts", action="store_true", help="drop marts schema")
    reset.add_argument("--reset-state", action="store_true", help="clear data/state/*.json")
    reset.add_argument("--clear-raw-cache", action="store_true", help="clear data/raw cache")

    run = sub.add_parser("run", help="run pipeline for a date or range")
    run.add_argument("--date", help="date in YYYY-MM-DD", required=False)
    run.add_argument("--start", help="start date in YYYY-MM-DD", required=False)
    run.add_argument("--end", help="end date in YYYY-MM-DD", required=False)
    run.add_argument("--from-scratch", action="store_true", help="drop raw/curated/marts before run")
    run.add_argument("--reset-state", action="store_true", help="clear data/state/*.json")
    run.add_argument("--clear-raw-cache", action="store_true", help="clear data/raw cache before run")
    run.add_argument("--no-cache", action="store_true", help="force re-download even if cached")
    run.add_argument("--checks", action="store_true", help="run checks after")
    run.add_argument("--mode", choices=["dashboard", "full"], default="dashboard")
    run.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")

    return parser


def main(argv: list[str] | None = None) -> None:
    _try_load_dotenv()
    _setup_logging()

    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "ref":
            engine = None if args.engine == "auto" else args.engine
            ensure_database(engine=engine)
            run_ref(engine=engine)
        elif args.command == "backfill":
            run_backfill(
                _validate_date(args.start),
                _validate_date(args.end),
                args.checks,
                args.resume,
                engine=None if args.engine == "auto" else args.engine,
            )
        elif args.command == "checks":
            cmd_checks(args.date)
        elif args.command == "enrich":
            run_enrich(_validate_date(args.date), engine=None if args.engine == "auto" else args.engine)
        elif args.command == "marts":
            run_marts(_validate_date(args.date), engine=None if args.engine == "auto" else args.engine)
        elif args.command == "run":
            cmd_run(
                args.date,
                args.checks,
                engine=None if args.engine == "auto" else args.engine,
                start_str=getattr(args, "start", None),
                end_str=getattr(args, "end", None),
                from_scratch=getattr(args, "from_scratch", False),
                reset_state=getattr(args, "reset_state", False),
                no_cache=getattr(args, "no_cache", False),
                clear_raw_cache=getattr(args, "clear_raw_cache", False),
                mode=getattr(args, "mode", "dashboard"),
            )
        elif args.command == "reset":
            engine = None if args.engine == "auto" else args.engine
            _drop_schemas(
                engine,
                drop_raw=bool(args.drop_raw),
                drop_curated=bool(args.drop_curated),
                drop_marts=bool(args.drop_marts),
            )
            if args.reset_state:
                _reset_state_files()
            if args.clear_raw_cache:
                _clear_raw_cache()
        else:
            parser.error(f"unknown command: {args.command}")
    except Exception as exc:
        log.error("command failed | %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
