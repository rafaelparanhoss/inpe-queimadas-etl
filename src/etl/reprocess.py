from __future__ import annotations

import datetime as dt
import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg

from .config import settings
from .enrich_runner import run_enrich
from .marts_runner import run_marts


def _ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(message: str) -> None:
    print(f"[{_ts()}] reprocess_day | {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_cli(date_str: str) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    uv_bin = shutil.which("uv")
    if uv_bin:
        cmd = [uv_bin, "run", "python", "-m", "etl.cli", "--date", date_str]
    else:
        cmd = [sys.executable, "-m", "uv", "run", "python", "-m", "etl.cli", "--date", date_str]
    subprocess.run(cmd, check=True, cwd=_repo_root(), env=env)


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _format_table(headers: list[str], rows: list[tuple]) -> str:
    str_rows = [[str(value) if value is not None else "" for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in str_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    header_line = " " + " | ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    sep_line = "-" + "-+-".join("-" * widths[idx] for idx in range(len(headers)))
    lines = [header_line, sep_line]
    for row in str_rows:
        lines.append(" " + " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(row)))
    lines.append(f"({len(str_rows)} row{'s' if len(str_rows) != 1 else ''})")
    return "\n".join(lines)


def _counts_basic(cursor: psycopg.Cursor, date_val: dt.date) -> tuple[int, int]:
    cursor.execute(
        """
        select
          (select count(*) from raw.inpe_focos where file_date = %s::date) as raw_n,
          (select count(*) from curated.inpe_focos_enriched where file_date = %s::date) as curated_n;
        """,
        (date_val, date_val),
    )
    row = cursor.fetchone()
    return int(row[0]), int(row[1])


def _counts_full(cursor: psycopg.Cursor, date_val: dt.date) -> tuple[int, int, int]:
    cursor.execute(
        """
        select
          (select count(*) from raw.inpe_focos where file_date = %s::date) as raw_n,
          (select count(*) from curated.inpe_focos_enriched where file_date = %s::date) as curated_n,
          (select coalesce(sum(n_focos),0)
           from marts.focos_diario_municipio
           where day = %s::date) as marts_day_sum;
        """,
        (date_val, date_val, date_val),
    )
    row = cursor.fetchone()
    return int(row[0]), int(row[1]), int(row[2])


def _delete_day(cursor: psycopg.Cursor, date_val: dt.date) -> None:
    cursor.execute("begin;")
    cursor.execute("delete from curated.inpe_focos_enriched where file_date = %s::date;", (date_val,))
    cursor.execute("delete from raw.inpe_focos where file_date = %s::date;", (date_val,))
    cursor.execute("commit;")


def run_reprocess(date_str: str, dry_run: bool) -> None:
    date_val = dt.date.fromisoformat(date_str)
    _log(f"start | date={date_str} | dry_run={1 if dry_run else 0}")

    _log("counts before")
    if dry_run:
        _log("dry-run sql:")
        print(
            """
select
  (select count(*) from raw.inpe_focos where file_date = '{date}'::date) as raw_n,
  (select count(*) from curated.inpe_focos_enriched where file_date = '{date}'::date) as curated_n,
  (select coalesce(sum(n_focos),0) from marts.focos_diario_municipio where day = '{date}'::date) as marts_day_sum;
""".format(date=date_str).strip(),
            flush=True,
        )
        print(flush=True)
    else:
        with _connect() as conn, conn.cursor() as cur:
            raw_n, curated_n, marts_day_sum = _counts_full(cur, date_val)
        print(
            _format_table(
                ["raw_n", "curated_n", "marts_day_sum"],
                [(raw_n, curated_n, marts_day_sum)],
            ),
            flush=True,
        )
        print(flush=True)

    _log("delete day (raw + curated)")
    if dry_run:
        _log("dry-run sql:")
        print(
            """
begin;
delete from curated.inpe_focos_enriched where file_date = '{date}'::date;
delete from raw.inpe_focos where file_date = '{date}'::date;
commit;
""".format(date=date_str).strip(),
            flush=True,
        )
        print(flush=True)
    else:
        with _connect() as conn, conn.cursor() as cur:
            _delete_day(cur, date_val)
            conn.commit()

    _log("counts after delete (marts may be stale)")
    if dry_run:
        _log("dry-run sql:")
        print(
            """
select
  (select count(*) from raw.inpe_focos where file_date = '{date}'::date) as raw_n,
  (select count(*) from curated.inpe_focos_enriched where file_date = '{date}'::date) as curated_n;
""".format(date=date_str).strip(),
            flush=True,
        )
        print(flush=True)
    else:
        with _connect() as conn, conn.cursor() as cur:
            raw_n, curated_n = _counts_basic(cur, date_val)
        print(_format_table(["raw_n", "curated_n"], [(raw_n, curated_n)]), flush=True)
        print(flush=True)

    if dry_run:
        _log("done")
        return

    _log("run etl cli (raw load)")
    _run_cli(date_str)

    _log("run enrich")
    run_enrich(date_str)

    _log("run marts")
    run_marts(date_str)

    _log("final checks")
    with _connect() as conn, conn.cursor() as cur:
        raw_n, curated_n, marts_day_sum = _counts_full(cur, date_val)
    _log(f"final counts | raw_n={raw_n} | curated_n={curated_n} | marts_day_sum={marts_day_sum}")

    if raw_n != curated_n:
        _log(f"error | raw_n != curated_n | raw_n={raw_n} | curated_n={curated_n}")
        raise SystemExit(1)

    if marts_day_sum != curated_n:
        _log(
            "error | marts_day_sum != curated_n"
            f" | marts_day_sum={marts_day_sum} | curated_n={curated_n}"
        )
        raise SystemExit(1)

    _log("done")
