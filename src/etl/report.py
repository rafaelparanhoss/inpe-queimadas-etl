from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import psycopg

from .config import settings


def _log(message: str) -> None:
    print(f"[report_day] {message}")


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


def _format_table(headers: list[str], rows: list[tuple]) -> str:
    str_rows = [[str(value) if value is not None else "" for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in str_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    header_line = " | ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    sep_line = "-+-".join("-" * widths[idx] for idx in range(len(headers)))
    lines = [header_line, sep_line]
    for row in str_rows:
        lines.append(" | ".join(value.ljust(widths[idx]) for idx, value in enumerate(row)))
    lines.append(f"({len(str_rows)} rows)")
    return "\n".join(lines)


def run_report(date_str: str) -> None:
    date_val = dt.date.fromisoformat(date_str)
    report_dir = Path(settings.data_dir) / "reports" / date_str
    report_dir.mkdir(parents=True, exist_ok=True)

    summary_path = report_dir / "summary.txt"
    top_uf_csv = report_dir / "top_uf.csv"
    top_mun_csv = report_dir / "top_mun.csv"

    with _connect() as conn, conn.cursor() as cur:
        raw_count = _fetch_one(
            cur,
            "select count(*) from raw.inpe_focos where file_date = %s::date;",
            (date_val,),
        )
        curated_count = _fetch_one(
            cur,
            "select count(*) from curated.inpe_focos_enriched where file_date = %s::date;",
            (date_val,),
        )
        pct_com_mun = _fetch_one(
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
        top_uf_rows = _fetch_all(
            cur,
            """
            select uf, n_focos
            from marts.focos_diario_uf
            where day = %s::date
            order by n_focos desc
            limit 10;
            """,
            (date_val,),
        )
        top_mun_rows = _fetch_all(
            cur,
            """
            select mun_cd_mun, mun_nm_mun, mun_uf, n_focos
            from marts.focos_diario_municipio
            where day = %s::date
            order by n_focos desc
            limit 20;
            """,
            (date_val,),
        )

    summary_lines = [
        f"date: {date_str}",
        f"raw_count: {raw_count}",
        f"curated_count: {curated_count}",
        f"pct_com_mun: {_format_pct(pct_com_mun)}",
        "",
        "top_uf:",
        _format_table(["uf", "n_focos"], list(top_uf_rows)),
        "",
        "top_mun:",
        _format_table(["mun_cd_mun", "mun_nm_mun", "mun_uf", "n_focos"], list(top_mun_rows)),
        "",
    ]
    summary_text = "\n".join(summary_lines)

    _log(f"write summary | path={summary_path}")
    summary_path.write_text(summary_text, encoding="utf-8")
    print(summary_text, end="")

    with top_uf_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["uf", "n_focos"])
        writer.writerows(top_uf_rows)

    with top_mun_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["mun_cd_mun", "mun_nm_mun", "mun_uf", "n_focos"])
        writer.writerows(top_mun_rows)

    _log("done")
