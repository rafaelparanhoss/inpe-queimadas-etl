from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import psycopg

from .config import settings
import json

from .apply_sql import ApplyStats, apply_dirs
from .sql_runner import run_sql_file

log = logging.getLogger("validate_marts")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _write_report(stats_marts, stats_checks, check_results, counts) -> Path:
    path = Path("docs") / "validation_last_run.md"
    now = datetime.utcnow().isoformat() + "Z"
    lines = [
        "# validation last run",
        "",
        f"timestamp_utc: {now}",
        "",
        "marts:",
        f"- applied: {stats_marts.applied}",
        f"- skipped_date: {stats_marts.skipped_date}",
        f"- skipped_stub: {stats_marts.skipped_stub}",
        f"- failed: {stats_marts.failed}",
        "",
        "checks:",
        f"- applied: {stats_checks.applied}",
        f"- skipped_date: {stats_checks.skipped_date}",
        f"- skipped_stub: {stats_checks.skipped_stub}",
        f"- failed: {stats_checks.failed}",
        "",
        "check_results:",
    ]
    for name, ok, err in check_results:
        status = "ok" if ok else "fail"
        detail = f" | {err}" if err else ""
        lines.append(f"- {name}: {status}{detail}")
    lines += [
        "",
        "last_day_counts:",
        f"- uf_day: {counts.get('uf_day')}",
        f"- uf_rows: {counts.get('uf_rows')}",
        f"- mun_day: {counts.get('mun_day')}",
        f"- mun_features: {counts.get('mun_features')}",
        f"- scatter_day: {counts.get('scatter_day')}",
        f"- scatter_rows: {counts.get('scatter_rows')}",
        "",
        "status:",
        "- ok: " + ("true" if (stats_marts.failed + stats_checks.failed) == 0 else "false"),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _run_checks(
    check_dir: Path,
    vars_dict: dict[str, str] | None,
    dry_run: bool,
    engine: str | None,
    dsn: str | None,
):
    files = sorted(check_dir.glob("*.sql"))
    results = []
    stats = apply_dirs([check_dir], vars_dict, True) if dry_run else ApplyStats()
    if dry_run:
        for file in files:
            results.append((file.name, True, "dry-run"))
        return results, stats

    applied = 0
    failed = 0
    for file in files:
        try:
            run_sql_file(str(file), vars_dict, engine=engine, dsn=dsn)
            results.append((file.name, True, None))
            applied += 1
        except Exception as exc:
            results.append((file.name, False, str(exc)))
            failed += 1
            break
    stats.applied = applied
    stats.failed = failed
    return results, stats


def _fetch_counts() -> dict:
    counts = {
        "uf_day": None,
        "uf_rows": None,
        "mun_day": None,
        "mun_features": None,
        "scatter_day": None,
        "scatter_rows": None,
    }
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("select max(day) from marts.focos_diario_uf;")
        row = cur.fetchone()
        counts["uf_day"] = row[0] if row else None
        if counts["uf_day"]:
            cur.execute(
                "select count(*) from marts.v_chart_uf_choropleth_day where day = %s;",
                (counts["uf_day"],),
            )
            row = cur.fetchone()
            counts["uf_rows"] = row[0] if row else None

        cur.execute("select max(day) from marts.focos_diario_municipio;")
        row = cur.fetchone()
        counts["mun_day"] = row[0] if row else None
        if counts["mun_day"]:
            cur.execute(
                """
                select count(distinct cd_mun)
                from marts.v_chart_mun_choropleth_day
                where day = %s;
                """,
                (counts["mun_day"],),
            )
            row = cur.fetchone()
            counts["mun_features"] = row[0] if row else None

        cur.execute("select max(file_date) from curated.inpe_focos_enriched;")
        row = cur.fetchone()
        counts["scatter_day"] = row[0] if row else None
        if counts["scatter_day"]:
            cur.execute(
                "select count(*) from marts.v_chart_focos_scatter where file_date = %s;",
                (counts["scatter_day"],),
            )
            row = cur.fetchone()
            counts["scatter_rows"] = row[0] if row else None
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="apply marts and checks")
    parser.add_argument("--date", help="date in YYYY-MM-DD (optional for checks)")
    parser.add_argument("--dry-run", action="store_true", help="show order without executing")
    parser.add_argument(
        "--apply-minimal",
        action="store_true",
        help="apply minimal ref/enrich/marts before checks (default behavior)",
    )
    parser.add_argument(
        "--engine",
        choices=["docker", "direct", "auto"],
        default="auto",
        help="execution engine (default: auto)",
    )
    parser.add_argument("--dsn", help="direct connection dsn (optional)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    vars_dict = {"DATE": args.date} if args.date else None
    repo_root = _repo_root()

    if args.apply_minimal:
        log.info("apply minimal | enabled")

    engine = None if args.engine == "auto" else args.engine
    stats_marts = apply_dirs(
        [
            repo_root / "sql" / "minimal" / "ref_core",
            repo_root / "sql" / "minimal" / "enrich",
            repo_root / "sql" / "minimal" / "marts",
        ],
        vars_dict,
        args.dry_run,
        engine=engine,
        dsn=args.dsn,
    )
    check_results, stats_checks = _run_checks(
        repo_root / "sql" / "checks",
        vars_dict,
        args.dry_run,
        engine=engine,
        dsn=args.dsn,
    )
    counts = _fetch_counts() if not args.dry_run else {}

    report = _write_report(stats_marts, stats_checks, check_results, counts)
    log.info("report | path=%s", report.as_posix())

    total_failed = stats_marts.failed + stats_checks.failed
    log.info(
        "validate summary | marts_applied=%s | checks_applied=%s | failed=%s",
        stats_marts.applied,
        stats_checks.applied,
        total_failed,
    )
    try:
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        def _stringify(value):
            if hasattr(value, "isoformat"):
                return value.isoformat()
            return value
        safe_counts = {k: _stringify(v) for k, v in counts.items()}
        payload = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "marts": stats_marts.__dict__,
            "checks": stats_checks.__dict__,
            "check_results": [
                {"name": name, "ok": ok, "error": err} for name, ok, err in check_results
            ],
            "counts": safe_counts,
        }
        (log_dir / "last_run.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as exc:
        log.warning("failed to write logs/last_run.json | %s", exc)
    if total_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
