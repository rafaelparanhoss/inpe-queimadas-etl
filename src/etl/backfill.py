from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg

from .checks import run_checks
from .config import settings
from .db_bootstrap import ensure_database
from .enrich_runner import run_enrich
from .marts_runner import run_marts
from .ref_runner import run_ref

log = logging.getLogger("backfill")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_cli(date_str: str, no_cache: bool = False) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    uv_bin = shutil.which("uv")
    extra = ["--no-cache"] if no_cache else []
    if uv_bin:
        cmd = [uv_bin, "run", "python", "-m", "etl.cli", "--date", date_str, *extra]
    else:
        cmd = [sys.executable, "-m", "uv", "run", "python", "-m", "etl.cli", "--date", date_str, *extra]
    try:
        subprocess.run(cmd, check=True, cwd=_repo_root(), env=env)
        return
    except Exception as exc:
        log.warning("uv run failed, fallback to python -m etl.cli | err=%s", exc)
        fallback = [sys.executable, "-m", "etl.cli", "--date", date_str, *extra]
        subprocess.run(fallback, check=True, cwd=_repo_root(), env=env)


def _state_path(start: date, end: date) -> Path:
    state_dir = Path(settings.data_dir) / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / f"backfill_{start.isoformat()}_{end.isoformat()}.json"


def _read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log.warning("state read failed | path=%s", path.as_posix())
        return {}


def _write_state(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _check_day_counts(day: date) -> tuple[float, int]:
    conn_str = (
        f"host={settings.db_host} port={settings.db_port} dbname={settings.db_name} "
        f"user={settings.db_user} password={settings.db_password}"
    )
    sql = """
    select
      (select count(*) from raw.inpe_focos where file_date = %s::date) as raw_n,
      (select count(*) from curated.inpe_focos_enriched where file_date = %s::date) as curated_total,
      (select count(*) from curated.inpe_focos_enriched
       where file_date = %s::date and mun_cd_mun is not null) as curated_com_mun,
      (select coalesce(sum(n_focos), 0)
       from marts.focos_diario_municipio
       where day = %s::date) as marts_mun_sum,
      (select coalesce(sum(n_focos), 0)
       from marts.focos_diario_uf
       where day = %s::date) as marts_uf_sum
    """
    with psycopg.connect(conn_str) as conn, conn.cursor() as cur:
        cur.execute(sql, (day, day, day, day, day))
        raw_n, curated_total, curated_com_mun, marts_mun_sum, marts_uf_sum = cur.fetchone()

    curated_total = int(curated_total or 0)
    curated_com_mun = int(curated_com_mun or 0)
    marts_mun_sum = int(marts_mun_sum or 0)
    marts_uf_sum = int(marts_uf_sum or 0)
    raw_n = int(raw_n or 0)

    if raw_n != curated_total:
        raise RuntimeError(
            "raw_n != curated_total"
            f" | raw_n={raw_n} | curated_total={curated_total} | date={day.isoformat()}"
        )

    if curated_total == 0:
        if marts_mun_sum != 0 or marts_uf_sum != 0:
            raise RuntimeError(
                "marts sums != 0 with empty curated"
                f" | marts_mun_sum={marts_mun_sum} | marts_uf_sum={marts_uf_sum} | date={day.isoformat()}"
            )
        return 0.0, 0

    if marts_mun_sum != curated_com_mun:
        raise RuntimeError(
            "marts_mun_sum != curated_com_mun"
            f" | marts_mun_sum={marts_mun_sum} | curated_com_mun={curated_com_mun}"
            f" | date={day.isoformat()}"
        )

    if marts_uf_sum != curated_com_mun:
        raise RuntimeError(
            "marts_uf_sum != curated_com_mun"
            f" | marts_uf_sum={marts_uf_sum} | curated_com_mun={curated_com_mun}"
            f" | date={day.isoformat()}"
        )

    missing_mun = curated_total - curated_com_mun
    pct_mun = round(100.0 * curated_com_mun / curated_total, 2)
    if missing_mun > 0:
        log.warning(
            "missing mun_cd_mun | date=%s | missing=%s | pct_com_mun=%s",
            day.isoformat(),
            missing_mun,
            pct_mun,
        )

    return pct_mun, missing_mun


def run_backfill(
    start_str: str,
    end_str: str,
    checks: bool,
    resume: bool,
    engine: str | None = None,
    no_cache: bool = False,
) -> None:
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    if start > end:
        raise ValueError("start date must be <= end date")

    state_file = _state_path(start, end)
    state = _read_state(state_file)

    current = start
    if resume:
        last_completed = state.get("last_completed")
        if last_completed:
            last_date = date.fromisoformat(last_completed)
            current = last_date + timedelta(days=1)
        if current > end:
            log.info("backfill already complete | start=%s | end=%s", start, end)
            return

    ensure_database(engine=engine)
    run_ref(engine=engine)

    n_ok = 0
    n_fail = 0
    first_fail = None
    pct_min = None
    pct_sum = 0.0
    pct_count = 0
    missing_total = 0

    while current <= end:
        t0 = time.perf_counter()
        try:
            _run_cli(current.isoformat(), no_cache=no_cache)
            run_enrich(current.isoformat(), engine=engine)
            run_marts(current.isoformat(), engine=engine)
            if checks:
                run_checks(current.isoformat())
                pct_mun, missing_mun = _check_day_counts(current)
                pct_min = pct_mun if pct_min is None else min(pct_min, pct_mun)
                pct_sum += pct_mun
                pct_count += 1
                missing_total += missing_mun
            n_ok += 1
            _write_state(
                state_file,
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "last_completed": current.isoformat(),
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
            )
            log.info(
                "day ok | date=%s | dt=%.2fs",
                current.isoformat(),
                time.perf_counter() - t0,
            )
        except Exception as exc:
            n_fail += 1
            first_fail = first_fail or current.isoformat()
            log.error(
                "day fail | date=%s | err=%s",
                current.isoformat(),
                exc,
            )
            break
        current = current + timedelta(days=1)

    log.info(
        "summary | n_ok=%s | n_fail=%s | first_fail=%s | pct_min=%s | pct_avg=%s | missing_mun_total=%s",
        n_ok,
        n_fail,
        first_fail or "-",
        "-" if pct_min is None else f"{pct_min:.2f}",
        "-" if pct_count == 0 else f"{(pct_sum / pct_count):.2f}",
        missing_total,
    )
    if n_fail:
        raise SystemExit(1)
