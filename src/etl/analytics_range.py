from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from .config import settings


def _log(message: str) -> None:
    print(f"[analytics_range] {message}")


def _write_csv(path: Path, headers: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)


def run_analytics_range(
    start_str: str,
    end_str: str,
    out_dir: str | None,
    top_n: int,
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

    _log(f"out_dir={report_dir.as_posix()} | top_n={top_n}")

    _write_csv(
        report_dir / "quality_daily.csv",
        ["day", "n_total", "n_com_mun", "pct_com_mun", "missing_mun"],
    )
    _write_csv(
        report_dir / "seasonality_uf.csv",
        ["uf", "month", "n_focos", "focos_por_100km2", "rank_no_mes"],
    )
    _write_csv(
        report_dir / "hotspots_mun_period.csv",
        ["mun_cd_mun", "mun_nm_mun", "mun_uf", "mun_area_km2", "n_focos_total", "focos_por_100km2"],
    )
    _write_csv(
        report_dir / "top_shifts_uf.csv",
        ["uf", "n_q1", "n_q4", "delta_abs", "delta_pct"],
    )

    _log("done")
