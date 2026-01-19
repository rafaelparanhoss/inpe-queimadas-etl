from __future__ import annotations

import datetime as dt

import psycopg

from .config import settings


def _log(message: str) -> None:
    print(f"[checks] {message}")


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _fetch_one(cursor: psycopg.Cursor, sql: str, params: tuple | None = None):
    cursor.execute(sql, params or ())
    row = cursor.fetchone()
    return row[0] if row else None


def _fetch_all(cursor: psycopg.Cursor, sql: str, params: tuple | None = None):
    cursor.execute(sql, params or ())
    return cursor.fetchall()


def _format_pct(value: object) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):.2f}"


def run_checks(date_str: str | None) -> None:
    date_val = dt.date.fromisoformat(date_str) if date_str else None
    _log(f"start | date={date_val.isoformat() if date_val else 'all'}")

    with _connect() as conn, conn.cursor() as cur:
        ref_count = _fetch_one(cur, "select count(*) from ref.ibge_municipios;")
        _log(f"ref ibge municipios | count={ref_count}")

        _log("raw counts by file_date (top 10)")
        _log("file_date | n")
        for file_date, n in _fetch_all(
            cur,
            """
            select file_date, count(*) as n
            from raw.inpe_focos
            group by 1
            order by 2 desc
            limit 10;
            """,
        ):
            _log(f"{file_date} | {n}")

        _log("curated pct_com_mun (global)")
        pct_global = _fetch_one(
            cur,
            """
            select round(
              100.0 * count(*) filter (where mun_cd_mun is not null)
              / nullif(count(*), 0),
              2
            )
            from curated.inpe_focos_enriched;
            """,
        )
        _log(f"pct_com_mun_global={_format_pct(pct_global)}")

        pct_check = pct_global
        if date_val:
            _log(f"curated pct_com_mun (date={date_val.isoformat()})")
            pct_day = _fetch_one(
                cur,
                """
                select round(
                  100.0 * count(*) filter (where mun_cd_mun is not null)
                  / nullif(count(*), 0),
                  2
                )
                from curated.inpe_focos_enriched
                where file_date = %s::date;
                """,
                (date_val,),
            )
            _log(f"pct_com_mun_day={_format_pct(pct_day)}")
            pct_check = pct_day

        if pct_check is None or float(pct_check) <= 0:
            _log("error | pct_com_mun is zero")
            raise SystemExit(1)

        if date_val:
            marts_count = _fetch_one(
                cur,
                "select count(*) from marts.focos_diario_uf where day = %s::date;",
                (date_val,),
            )
        else:
            marts_count = _fetch_one(cur, "select count(*) from marts.focos_diario_uf;")

        if not marts_count:
            _log("error | marts uf totals empty")
            raise SystemExit(1)

        _log("marts uf totals (top 10)")
        _log("uf | n_focos")
        for uf, n_focos in _fetch_all(
            cur,
            """
            select uf, sum(n_focos) as n_focos
            from marts.focos_diario_uf
            group by 1
            order by 2 desc
            limit 10;
            """,
        ):
            _log(f"{uf} | {n_focos}")

    _log("done")
