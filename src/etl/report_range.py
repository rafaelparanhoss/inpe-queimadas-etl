from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import psycopg

from .config import settings


def _log(message: str) -> None:
    print(f"[report_range] {message}")


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _fetch_one(cursor: psycopg.Cursor, sql: str, params: tuple) -> tuple | None:
    cursor.execute(sql, params)
    return cursor.fetchone()


def _fetch_all(cursor: psycopg.Cursor, sql: str, params: tuple) -> list[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


def _write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def run_report_range(start_str: str, end_str: str) -> None:
    start = dt.date.fromisoformat(start_str)
    end = dt.date.fromisoformat(end_str)
    if start > end:
        raise ValueError("start date must be <= end date")

    report_dir = Path(settings.data_dir) / "reports" / f"range_{start_str}_{end_str}"
    report_dir.mkdir(parents=True, exist_ok=True)

    summary_path = report_dir / "summary.txt"
    br_daily_csv = report_dir / "br_daily.csv"
    uf_daily_csv = report_dir / "uf_daily.csv"
    uf_top_csv = report_dir / "uf_top.csv"
    mun_top_csv = report_dir / "mun_top.csv"

    with _connect() as conn, conn.cursor() as cur:
        summary_row = _fetch_one(
            cur,
            """
            with daily as (
              select
                coalesce(view_ts::date, file_date) as day,
                count(*) as total,
                count(*) filter (where mun_cd_mun is not null) as com_mun
              from curated.inpe_focos_enriched
              where coalesce(view_ts::date, file_date) between %s::date and %s::date
              group by 1
            )
            select
              coalesce(sum(total), 0) as total_focos,
              count(*) as days_with_data,
              coalesce(round(avg(100.0 * com_mun / nullif(total, 0)), 2), 0) as pct_avg,
              coalesce(round(min(100.0 * com_mun / nullif(total, 0)), 2), 0) as pct_min,
              coalesce(sum(total - com_mun), 0) as missing_total
            from daily;
            """,
            (start, end),
        )
        summary_row = summary_row or (0, 0, 0, 0, 0)

        br_daily_rows = _fetch_all(
            cur,
            """
            with base as (
              select
                coalesce(view_ts::date, file_date) as day,
                (mun_cd_mun is not null) as has_mun
              from curated.inpe_focos_enriched
              where coalesce(view_ts::date, file_date) between %s::date and %s::date
            )
            select
              day,
              count(*) as n_focos_total,
              sum(case when has_mun then 1 else 0 end) as n_focos_com_mun,
              round(
                100.0 * sum(case when has_mun then 1 else 0 end) / nullif(count(*), 0),
                2
              ) as pct_com_mun,
              (count(*) - sum(case when has_mun then 1 else 0 end)) as missing_mun
            from base
            group by day
            order by day;
            """,
            (start, end),
        )

        uf_daily_rows = _fetch_all(
            cur,
            """
            select day, uf, n_focos, focos_por_100km2
            from marts.focos_diario_uf
            where day between %s::date and %s::date
            order by day, uf;
            """,
            (start, end),
        )

        uf_top_rows = _fetch_all(
            cur,
            """
            select
              uf,
              sum(n_focos) as n_focos_total,
              row_number() over (order by sum(n_focos) desc) as rank
            from marts.focos_diario_uf
            where day between %s::date and %s::date
            group by uf
            order by n_focos_total desc;
            """,
            (start, end),
        )

        mun_top_rows = _fetch_all(
            cur,
            """
            select
              mun_cd_mun,
              mun_nm_mun,
              mun_uf,
              sum(n_focos) as n_focos_total,
              row_number() over (order by sum(n_focos) desc) as rank
            from marts.focos_diario_municipio
            where day between %s::date and %s::date
            group by mun_cd_mun, mun_nm_mun, mun_uf
            order by n_focos_total desc;
            """,
            (start, end),
        )

    total_focos, days_with_data, pct_avg, pct_min, missing_total = summary_row
    summary_lines = [
        f"start: {start_str}",
        f"end: {end_str}",
        f"total_focos: {total_focos}",
        f"days_with_data: {days_with_data}",
        f"pct_com_mun_avg: {pct_avg}",
        f"pct_com_mun_min: {pct_min}",
        f"missing_mun_total: {missing_total}",
        "",
        "notes:",
        "- total_focos inclui focos sem municipio",
        "- pct_com_mun e missing_mun medem a qualidade do join espacial",
        "",
    ]
    summary_text = "\n".join(summary_lines)

    _log(f"write summary | path={summary_path}")
    summary_path.write_text(summary_text, encoding="utf-8")
    print(summary_text, end="")

    _write_csv(
        br_daily_csv,
        ["day", "n_focos_total", "n_focos_com_mun", "pct_com_mun", "missing_mun"],
        br_daily_rows,
    )
    _write_csv(uf_daily_csv, ["day", "uf", "n_focos", "focos_por_100km2"], uf_daily_rows)
    _write_csv(uf_top_csv, ["uf", "n_focos_total", "rank"], uf_top_rows)
    _write_csv(
        mun_top_csv,
        ["mun_cd_mun", "mun_nm_mun", "mun_uf", "n_focos_total", "rank"],
        mun_top_rows,
    )

    _log("done")
