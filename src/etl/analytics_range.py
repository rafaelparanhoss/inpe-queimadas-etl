from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import psycopg

from .config import settings


def _log(message: str) -> None:
    print(f"[analytics_range] {message}")


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _fetch_all(cursor: psycopg.Cursor, sql: str, params: tuple) -> list[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


def _write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def run_analytics_range(
    start_str: str,
    end_str: str,
    out_dir: str | None,
    top_n: int,
    shifts_top: int,
    shifts_sort: str,
) -> None:
    start = dt.date.fromisoformat(start_str)
    end = dt.date.fromisoformat(end_str)
    if start > end:
        raise ValueError("start date must be <= end date")

    if out_dir:
        report_dir = Path(out_dir)
    else:
        report_dir = Path(settings.data_dir) / "reports" / f"analytics_{start_str}_{end_str}"
    report_dir.mkdir(parents=True, exist_ok=True)

    _log(
        "out_dir=%s | top_n=%s | shifts_top=%s | shifts_sort=%s",
        report_dir.as_posix(),
        top_n,
        shifts_top,
        shifts_sort,
    )

    with _connect() as conn, conn.cursor() as cur:
        quality_rows = _fetch_all(
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
              count(*) as n_total,
              sum(case when has_mun then 1 else 0 end) as n_com_mun,
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

        seasonality_rows = _fetch_all(
            cur,
            """
            with monthly as (
              select
                date_trunc('month', day)::date as month,
                uf,
                sum(n_focos) as n_focos,
                round(
                  (100 * sum(n_focos)::numeric) / nullif(max(uf_area_km2)::numeric, 0),
                  4
                ) as focos_por_100km2
              from marts.focos_diario_uf
              where day between %s::date and %s::date
              group by 1, 2
            )
            select
              uf,
              month,
              n_focos,
              focos_por_100km2,
              row_number() over (partition by month order by n_focos desc) as rank_no_mes
            from monthly
            order by month, rank_no_mes;
            """,
            (start, end),
        )

        span_days = min(90, (end - start).days + 1)
        q1_start = start
        q1_end = start + dt.timedelta(days=span_days - 1)
        q4_start = end - dt.timedelta(days=span_days - 1)
        q4_end = end

        hotspots_rows = _fetch_all(
            cur,
            """
            select
              mun_cd_mun,
              mun_nm_mun,
              mun_uf,
              max(mun_area_km2) as mun_area_km2,
              sum(n_focos) as n_focos_total,
              round(
                (100 * sum(n_focos)::numeric) / nullif(max(mun_area_km2)::numeric, 0),
                4
              ) as focos_por_100km2
            from marts.focos_diario_municipio
            where day between %s::date and %s::date
            group by mun_cd_mun, mun_nm_mun, mun_uf
            order by n_focos_total desc
            limit %s;
            """,
            (start, end, top_n),
        )

        shifts_sort_key = "delta_abs"
        if shifts_sort == "pct":
            shifts_sort_key = "abs(delta_pct)"

        shifts_sql = f"""
            with q1 as (
              select uf, sum(n_focos) as n_q1
              from marts.focos_diario_uf
              where day between %s::date and %s::date
              group by uf
            ),
            q4 as (
              select uf, sum(n_focos) as n_q4
              from marts.focos_diario_uf
              where day between %s::date and %s::date
              group by uf
            )
            select
              coalesce(q1.uf, q4.uf) as uf,
              coalesce(q1.n_q1, 0) as n_q1,
              coalesce(q4.n_q4, 0) as n_q4,
              coalesce(q4.n_q4, 0) - coalesce(q1.n_q1, 0) as delta_abs,
              case
                when coalesce(q1.n_q1, 0) = 0 then null
                else round(
                  100.0 * (coalesce(q4.n_q4, 0) - coalesce(q1.n_q1, 0))
                  / nullif(q1.n_q1, 0),
                  2
                )
              end as delta_pct
            from q1
            full join q4 on q4.uf = q1.uf
            order by {shifts_sort_key} desc, uf
            limit %s;
            """

        shifts_rows = _fetch_all(
            cur,
            shifts_sql,
            (q1_start, q1_end, q4_start, q4_end, shifts_top),
        )

    _write_csv(
        report_dir / "quality_daily.csv",
        ["day", "n_total", "n_com_mun", "pct_com_mun", "missing_mun"],
        quality_rows,
    )
    _write_csv(
        report_dir / "seasonality_uf.csv",
        ["uf", "month", "n_focos", "focos_por_100km2", "rank_no_mes"],
        seasonality_rows,
    )
    _write_csv(
        report_dir / "hotspots_mun_period.csv",
        ["mun_cd_mun", "mun_nm_mun", "mun_uf", "mun_area_km2", "n_focos_total", "focos_por_100km2"],
        hotspots_rows,
    )
    _write_csv(
        report_dir / "top_shifts_uf.csv",
        ["uf", "n_q1", "n_q4", "delta_abs", "delta_pct"],
        shifts_rows,
    )

    _log("done")
