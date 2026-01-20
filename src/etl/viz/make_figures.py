from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from ..config import settings


def _log(message: str, *args: object) -> None:
    if args:
        message = message % args
    print(f"[make_figures] {message}")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"csv not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _to_int(value: str) -> int:
    return int(value) if value else 0


def _to_float(value: str) -> float:
    return float(value) if value else 0.0


def _get_pyplot():
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise RuntimeError(
            "matplotlib is required for make-figures; install with `uv pip install matplotlib`"
        ) from exc
    return plt


def _rolling_mean(values: list[float], window: int) -> list[float]:
    if window <= 1:
        return values[:]
    out: list[float] = []
    acc = 0.0
    for idx, value in enumerate(values):
        acc += value
        if idx >= window:
            acc -= values[idx - window]
        denom = min(idx + 1, window)
        out.append(acc / denom)
    return out


def _save_fig(plt, fig, out_path: Path, dpi: int) -> None:
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def _plot_quality(plt, quality_rows: list[dict[str, str]], out_path: Path, dpi: int) -> None:
    days = [_parse_date(row["day"]) for row in quality_rows]
    pct_com = [_to_float(row["pct_com_mun"]) for row in quality_rows]
    pct_missing = [max(0.0, 100.0 - value) for value in pct_com]
    missing = [_to_int(row["missing_mun"]) for row in quality_rows]
    max_pct_missing = max(pct_missing) if pct_missing else 0.0

    fig, ax_pct = plt.subplots(figsize=(12, 4))
    ax_miss = ax_pct.twinx()

    ax_pct.plot(days, pct_missing, color="#1f77b4", linewidth=1.5, label="pct_missing")
    ax_miss.bar(days, missing, color="#d62728", alpha=0.35, label="missing_mun")

    miss_days = [day for day, value in zip(days, missing) if value > 0]
    miss_values = [value for value in missing if value > 0]
    if miss_days:
        ax_miss.scatter(miss_days, miss_values, color="#d62728", s=18, zorder=3)

    ax_pct.set_ylabel("pct_missing")
    ax_miss.set_ylabel("missing_mun")
    ax_pct.set_xlabel("day")

    if max_pct_missing <= 0.5:
        ax_pct.set_ylim(0, 0.5)
    else:
        ax_pct.set_ylim(0, max_pct_missing * 1.1)

    fig.autofmt_xdate()
    _save_fig(plt, fig, out_path, dpi)


def _plot_total_vs_com(
    plt,
    br_rows: list[dict[str, str]],
    out_path: Path,
    smooth_days: int,
    dpi: int,
) -> None:
    days = [_parse_date(row["day"]) for row in br_rows]
    total = [_to_int(row["n_focos_total"]) for row in br_rows]
    com_mun = [_to_int(row["n_focos_com_mun"]) for row in br_rows]
    total_smooth = _rolling_mean([float(value) for value in total], smooth_days)
    com_smooth = _rolling_mean([float(value) for value in com_mun], smooth_days)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(days, total, color="#1f77b4", linewidth=0.8, alpha=0.3, label="total_daily")
    ax.plot(days, com_mun, color="#2ca02c", linewidth=0.8, alpha=0.3, label="com_mun_daily")
    ax.plot(days, total_smooth, color="#1f77b4", linewidth=1.8, label=f"total_{smooth_days}d")
    ax.plot(days, com_smooth, color="#2ca02c", linewidth=1.8, label=f"com_mun_{smooth_days}d")
    ax.set_ylabel("n_focos")
    ax.set_xlabel("day")
    ax.legend(ncol=2, fontsize=8)
    fig.autofmt_xdate()
    _save_fig(plt, fig, out_path, dpi)


def _plot_seasonality(plt, uf_rows: list[dict[str, str]], out_path: Path) -> None:
    totals: dict[str, int] = {}
    monthly: dict[str, dict[dt.date, int]] = {}

    for row in uf_rows:
        uf = row["uf"]
        month = _parse_date(row["month"])
        n_focos = _to_int(row["n_focos"])
        totals[uf] = totals.get(uf, 0) + n_focos
        monthly.setdefault(uf, {})[month] = n_focos

    top_ufs = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:10]
    months = sorted({month for rows in monthly.values() for month in rows})

    fig, ax = plt.subplots(figsize=(12, 5))
    for uf, _ in top_ufs:
        series = [monthly.get(uf, {}).get(month, 0) for month in months]
        ax.plot(months, series, linewidth=1.2, label=uf)

    ax.set_ylabel("n_focos")
    ax.set_xlabel("month")
    ax.legend(ncol=2, fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_hotspots(plt, hotspot_rows: list[dict[str, str]], out_path: Path) -> None:
    def sort_key(row: dict[str, str]) -> tuple:
        return (
            -_to_int(row["n_focos_total"]),
            -_to_float(row["focos_por_100km2"]),
            row["mun_cd_mun"],
        )

    rows = sorted(hotspot_rows, key=sort_key)
    labels = [f"{row['mun_uf']}-{row['mun_cd_mun']}" for row in rows]
    values = [_to_int(row["n_focos_total"]) for row in rows]

    fig_height = max(4.0, 0.2 * len(rows))
    fig, ax = plt.subplots(figsize=(10, fig_height))
    ax.barh(labels[::-1], values[::-1], color="#1f77b4")
    ax.set_xlabel("n_focos_total")
    ax.set_ylabel("municipio")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_shifts(plt, shift_rows: list[dict[str, str]], out_path: Path) -> None:
    labels = [row["uf"] for row in shift_rows]
    values = [_to_int(row["delta_abs"]) for row in shift_rows]

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(labels, values, color="#ff7f0e")
    ax.set_ylabel("delta_abs")
    ax.set_xlabel("uf")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def run_make_figures(start_str: str, end_str: str, out_dir: str | None) -> None:
    report_root = Path(settings.data_dir) / "reports"
    analytics_dir = report_root / f"analytics_{start_str}_{end_str}"
    range_dir = report_root / f"range_{start_str}_{end_str}"

    if out_dir:
        figures_dir = Path(out_dir)
    else:
        figures_dir = Path(settings.data_dir) / "figures" / f"{start_str}_{end_str}"
    figures_dir.mkdir(parents=True, exist_ok=True)

    smooth_days = 7
    dpi = 200
    _log(
        "analytics_dir=%s | range_dir=%s | out_dir=%s | smooth_days=%s | dpi=%s",
        analytics_dir.as_posix(),
        range_dir.as_posix(),
        figures_dir.as_posix(),
        smooth_days,
        dpi,
    )

    plt = _get_pyplot()
    quality_rows = _read_csv(analytics_dir / "quality_daily.csv")
    seasonality_rows = _read_csv(analytics_dir / "seasonality_uf.csv")
    hotspots_rows = _read_csv(analytics_dir / "hotspots_mun_period.csv")
    shifts_rows = _read_csv(analytics_dir / "top_shifts_uf.csv")
    br_rows = _read_csv(range_dir / "br_daily.csv")

    _plot_quality(plt, quality_rows, figures_dir / "quality_pct_missing.png", dpi)
    _plot_total_vs_com(plt, br_rows, figures_dir / "total_vs_com_mun.png", smooth_days, dpi)
    _plot_seasonality(plt, seasonality_rows, figures_dir / "seasonality_uf_top10.png")
    _plot_hotspots(plt, hotspots_rows, figures_dir / "hotspots_topn.png")
    _plot_shifts(plt, shifts_rows, figures_dir / "shifts_topn.png")

    _log("done")
