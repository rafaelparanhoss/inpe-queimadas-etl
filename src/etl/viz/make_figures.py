from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from typing import Sequence

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

def _fmt_int_ptbr(value: float | int) -> str:
    # 1234567 -> 1.234.567
    return f"{int(round(value)):,}".replace(",", ".")


def _fmt_float_ptbr(value: float, decimals: int = 2) -> str:
    # 1234.56 -> 1.234,56
    s = f"{value:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_int_pt(n: int) -> str:
    # 3517016 -> 3.517.016
    return f"{n:,}".replace(",", ".")


def _fmt_pct_pt(x: float, nd: int = 2) -> str:
    # 0.1234 -> 0,12%
    s = f"{x:.{nd}f}".replace(".", ",")
    return f"{s}%"


def _get_pyplot():
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise RuntimeError(
            "matplotlib is required for make-figures; install with `uv pip install matplotlib`"
        ) from exc
    return plt


def _apply_style(plt, dpi: int) -> None:
    plt.rcParams.update(
        {
            "figure.dpi": dpi,
            "savefig.dpi": dpi,
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "axes.titlesize": 16,
            "axes.titleweight": "bold",
            "axes.labelsize": 12,
            "xtick.labelsize": 10,
            "ytick.labelsize": 11,
            "legend.fontsize": 10,
            "axes.grid": False,  # grid a gente liga por-eixo em cada gráfico
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )


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


def _save_fig(
    plt,
    fig,
    out_path: Path,
    dpi: int,
    rect: tuple[float, float, float, float] | None = None,
) -> None:
    if rect:
        fig.tight_layout(rect=rect)
    else:
        fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _add_subtitle(ax, text: str) -> None:
    ax.text(
        0.0,
        1.02,
        text,
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=10,
        color="#444444",
    )


def _plot_quality(
    plt,
    quality_rows: list[dict[str, str]],
    out_path: Path,
    dpi: int,
    start_str: str,
    end_str: str,
    n_total: int,
    missing_total: int,
) -> None:
    from matplotlib import ticker as mticker

    days = [_parse_date(row["day"]) for row in quality_rows]
    pct_com = [_to_float(row["pct_com_mun"]) for row in quality_rows]  # em %
    pct_sem_mun = [max(0.0, 100.0 - value) for value in pct_com]  # em %
    missing = [_to_int(row["missing_mun"]) for row in quality_rows]

    pct_sem_mun_max = max(pct_sem_mun) if pct_sem_mun else 0.0
    pct_sem_mun_lim = max(0.20, pct_sem_mun_max * 1.25)  # deixa legível mesmo com valores bem baixos

    fig, ax_pct = plt.subplots(figsize=(13, 4.2))
    ax_cnt = ax_pct.twinx()

    # linha (%)
    ax_pct.plot(
        days,
        pct_sem_mun,
        linewidth=2.2,
        label="% de focos sem município",
    )

    # barras (contagem)
    ax_cnt.bar(
        days,
        missing,
        alpha=0.25,
        label="Focos sem município (contagem)",
    )

    # destacar dias com missing > 0
    miss_days = [day for day, value in zip(days, missing) if value > 0]
    miss_vals = [value for value in missing if value > 0]
    if miss_days:
        ax_cnt.scatter(miss_days, miss_vals, s=18, zorder=3)

    ax_pct.set_ylim(0, pct_sem_mun_lim)
    ax_pct.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt_pct_pt(x, 2)))
    ax_cnt.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt_int_pt(int(x))))

    ax_pct.set_title("Focos sem município identificado (qualidade da atribuição espacial)")
    _add_subtitle(
        ax_pct,
        f"Período: {start_str} a {end_str} • Total: {_fmt_int_pt(n_total)} • Sem município: {_fmt_int_pt(missing_total)} ({_fmt_pct_pt((missing_total / n_total * 100.0) if n_total else 0.0, 3)})",
    )

    ax_pct.set_xlabel("Dia")
    ax_pct.set_ylabel("% sem município")
    ax_cnt.set_ylabel("Sem município (contagem)")

    # legenda única (sem duplicar)
    h1, l1 = ax_pct.get_legend_handles_labels()
    h2, l2 = ax_cnt.get_legend_handles_labels()
    ax_pct.legend(h1 + h2, l1 + l2, loc="upper right", ncol=2)

    fig.autofmt_xdate()
    _save_fig(plt, fig, out_path, dpi)


def _plot_total_vs_com(
    plt,
    br_rows: list[dict[str, str]],
    out_path: Path,
    smooth_days: int,
    dpi: int,
    start_str: str,
    end_str: str,
) -> None:
    from matplotlib import ticker as mticker

    days = [_parse_date(row["day"]) for row in br_rows]
    total = [_to_int(row["n_focos_total"]) for row in br_rows]
    com_mun = [_to_int(row["n_focos_com_mun"]) for row in br_rows]

    total_smooth = _rolling_mean([float(value) for value in total], smooth_days)
    com_smooth = _rolling_mean([float(value) for value in com_mun], smooth_days)

    fig, ax = plt.subplots(figsize=(13, 4.2))

    # diário como “fundo”, sem entrar na legenda
    ax.plot(days, total, linewidth=0.8, alpha=0.15, label="_nolegend_")
    ax.plot(days, com_mun, linewidth=0.8, alpha=0.15, label="_nolegend_")

    # linhas principais (interpretáveis)
    ax.plot(days, total_smooth, linewidth=2.4, label="Total de focos")
    ax.plot(days, com_smooth, linewidth=2.4, label="Focos com município identificado")

    ax.set_title("Evolução diária de focos no Brasil")
    _add_subtitle(
        ax,
        f"Período: {start_str} a {end_str} • Linha suavizada: média de {smooth_days} dias • Sem município = diferença entre as linhas",
    )

    ax.set_xlabel("Dia")
    ax.set_ylabel("Focos por dia")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt_int_pt(int(x))))
    ax.legend(loc="upper left", ncol=2)

    fig.autofmt_xdate()
    _save_fig(plt, fig, out_path, dpi)


def _plot_seasonality(
    plt,
    uf_rows: list[dict[str, str]],
    out_path: Path,
    dpi: int,
) -> None:
    from matplotlib import dates as mdates
    from matplotlib import ticker as mticker

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

    smooth_months = 3

    fig, ax = plt.subplots(figsize=(14, 6))
    for uf, _ in top_ufs:
        series = [float(monthly.get(uf, {}).get(month, 0)) for month in months]
        series_smooth = _rolling_mean(series, smooth_months)
        ax.plot(months, series_smooth, linewidth=2.0, label=uf)

    ax.set_title("Sazonalidade mensal de focos por UF (Top 10 no período)")
    _add_subtitle(ax, f"Linha suavizada: média de {smooth_months} meses (para leitura)")

    ax.set_ylabel("Focos por mês")
    ax.set_xlabel("Mês")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt_int_pt(int(x))))

    ax.legend(
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
    )
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

    fig.autofmt_xdate()
    _save_fig(plt, fig, out_path, dpi, rect=(0, 0, 0.82, 1))


def _truncate_label(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _plot_hotspots(
    plt,
    hotspot_rows: list[dict[str, str]],
    out_path_count: Path,
    out_path_density: Path,
    fig_top_n: int,
    dpi: int,
    start_str: str,
    end_str: str,
) -> None:
    from matplotlib import ticker as mticker

    def sort_key_count(row: dict[str, str]) -> tuple:
        return (
            -_to_int(row["n_focos_total"]),
            -_to_float(row["focos_por_100km2"]),
            row["mun_cd_mun"],
        )

    def sort_key_density(row: dict[str, str]) -> tuple:
        return (
            -_to_float(row["focos_por_100km2"]),
            -_to_int(row["n_focos_total"]),
            row["mun_cd_mun"],
        )

    top_n = min(fig_top_n, len(hotspot_rows))
    rows_count = sorted(hotspot_rows, key=sort_key_count)[:top_n]
    rows_density = sorted(hotspot_rows, key=sort_key_density)[:top_n]

    def build_labels(rows: list[dict[str, str]]) -> list[str]:
        labels = []
        for row in rows:
            label = f"{row['mun_nm_mun']} ({row['mun_uf']})"
            labels.append(_truncate_label(label, 34))
        return labels

    def add_note(ax, text: str) -> None:
        ax.text(
            0.99,
            0.02,
            text,
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=10,
            color="#333333",
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", alpha=0.75, edgecolor="none"),
        )

    def annotate_barh(ax, values: Sequence[float]) -> None:
        xmax = max(values) if values else 0.0
        pad = xmax * 0.012 if xmax > 0 else 1.0
        for i, v in enumerate(values):
            ax.text(v + pad, i, _fmt_int_ptbr(v), va="center", ha="left", fontsize=10, color="#222222")


    # ---------------------------
    # hotspots por contagem
    # ---------------------------
    labels_count = build_labels(rows_count)
    values_count = [_to_int(row["n_focos_total"]) for row in rows_count]

    # count
    fig, ax = plt.subplots(figsize=(12, 8))

    # titulo centralizado no png inteiro
    fig.suptitle(
        "HOTSPOTS por MUNICÍPIO — maior número de focos",
        fontsize=16,
        fontweight="bold",
        x=0.5,
        y=0.98,
        ha="center",
    )

    bars = ax.barh(
        labels_count[::-1],
        values_count[::-1],
        height=0.72,
        color="#2E6F9E",        # azul mais agradável
        alpha=0.92,            # tira o “opaco pesado”
        edgecolor="#1E4E70",   # borda sutil
        linewidth=0.8,
        zorder=2,
    )

    # sombra leve nas barras
    import matplotlib.patheffects as pe
    for b in bars:
        b.set_path_effects(
            [
                pe.SimplePatchShadow(offset=(1.5, -1.5), alpha=0.20, shadow_rgbFace=(0, 0, 0)),
                pe.Normal(),
            ]
        )

    ax.set_xlabel("FOCOS")
    ax.set_ylabel("MUNICÍPIO (UF)")

    # grade só no eixo x (ajuda leitura sem poluir)
    ax.grid(axis="x", alpha=0.25, linestyle="--", linewidth=0.7, zorder=0)
    ax.grid(axis="y", alpha=0.0)

    # observação no canto inferior direito (dentro)
    ax.text(
        0.995,
        0.02,
        f"Período: {start_str} A {end_str}  •  TOP {top_n}",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
        color="#333333",
    )

    _save_fig(plt, fig, out_path_count, dpi)


    # ---------------------------
    # hotspots por densidade
    # ---------------------------
    labels_density = build_labels(rows_density)
    values_density = [_to_float(row["focos_por_100km2"]) for row in rows_density]

    fig, ax = plt.subplots(figsize=(12, 8))

    fig.suptitle(
        "HOTSPOTS por MUNICÍPIO — maior densidade de focos",
        fontsize=16,
        fontweight="bold",
        x=0.5,
        y=0.98,
        ha="center",
    )

    bars = ax.barh(
        labels_density[::-1],
        values_density[::-1],
        height=0.72,
        color="#6A3FA0",        # roxo mais “premium”
        alpha=0.92,
        edgecolor="#4B2A78",
        linewidth=0.8,
        zorder=2,
    )

    import matplotlib.patheffects as pe
    for b in bars:
        b.set_path_effects(
            [
                pe.SimplePatchShadow(offset=(1.5, -1.5), alpha=0.20, shadow_rgbFace=(0, 0, 0)),
                pe.Normal(),
            ]
        )

    ax.set_xlabel("FOCOS POR 100 KM²")
    ax.set_ylabel("MUNICÍPIO (UF)")

    ax.grid(axis="x", alpha=0.25, linestyle="--", linewidth=0.7, zorder=0)
    ax.grid(axis="y", alpha=0.0)

    ax.text(
        0.995,
        0.02,
        f"Período: {start_str} A {end_str}  •  TOP {top_n}  •  Densidade = focos/área",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
        color="#333333",
    )

    _save_fig(plt, fig, out_path_density, dpi)



def _plot_shifts(
    plt,
    shift_rows: list[dict[str, str]],
    out_path: Path,
    shifts_top_n: int,
    dpi: int,
    start_str: str,
    end_str: str,
    window_days: int,
) -> None:
    from matplotlib import ticker as mticker

    top_k = min(shifts_top_n, len(shift_rows))
    rows = sorted(
        shift_rows,
        key=lambda row: (-abs(_to_int(row["delta_abs"])), row["uf"]),
    )[:top_k]

    labels = [row["uf"] for row in rows]
    values = [_to_int(row["delta_abs"]) for row in rows]

    # cor por sinal ajuda leitura “bateu o olho”
    colors = ["#1f77b4" if v >= 0 else "#d62728" for v in values]

    fig, ax = plt.subplots(figsize=(11, 7.2))
    ax.barh(labels[::-1], values[::-1], color=colors[::-1])
    ax.axvline(0, color="#333333", linewidth=0.9)

    ax.set_title("Mudança de focos por UF (início vs fim do período)")
    _add_subtitle(
        ax,
        f"Comparação: primeiros {window_days} dias vs últimos {window_days} dias • Valor positivo = aumentou",
    )

    ax.set_xlabel("Diferença de focos (últimos − primeiros)")
    ax.set_ylabel("UF")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt_int_pt(int(x))))

    _save_fig(plt, fig, out_path, dpi)


def run_make_figures(
    start_str: str,
    end_str: str,
    out_dir: str | None,
    fig_top_n: int = 20,
    dpi: int = 200,
    smooth_days: int = 7,
) -> None:
    report_root = Path(settings.data_dir) / "reports"
    analytics_dir = report_root / f"analytics_{start_str}_{end_str}"
    range_dir = report_root / f"range_{start_str}_{end_str}"

    if out_dir:
        figures_dir = Path(out_dir)
    else:
        figures_dir = Path(settings.data_dir) / "figures" / f"{start_str}_{end_str}"
    figures_dir.mkdir(parents=True, exist_ok=True)

    legacy_hotspots = figures_dir / "hotspots_topn.png"
    if legacy_hotspots.exists():
        legacy_hotspots.unlink()

    shifts_top_n = 15
    _log(
        "analytics_dir=%s | range_dir=%s | out_dir=%s | smooth_days=%s | dpi=%s | fig_top_n=%s | shifts_top_n=%s",
        analytics_dir.as_posix(),
        range_dir.as_posix(),
        figures_dir.as_posix(),
        smooth_days,
        dpi,
        fig_top_n,
        shifts_top_n,
    )

    plt = _get_pyplot()
    _apply_style(plt, dpi)

    window_days = min(90, (_parse_date(end_str) - _parse_date(start_str)).days + 1)

    quality_rows = _read_csv(analytics_dir / "quality_daily.csv")
    seasonality_rows = _read_csv(analytics_dir / "seasonality_uf.csv")
    hotspots_rows = _read_csv(analytics_dir / "hotspots_mun_period.csv")
    shifts_rows = _read_csv(analytics_dir / "top_shifts_uf.csv")
    br_rows = _read_csv(range_dir / "br_daily.csv")

    n_total = sum(_to_int(row["n_total"]) for row in quality_rows)
    missing_total = sum(_to_int(row["missing_mun"]) for row in quality_rows)

    _plot_quality(
        plt,
        quality_rows,
        figures_dir / "quality_pct_missing.png",
        dpi,
        start_str,
        end_str,
        n_total,
        missing_total,
    )
    _plot_total_vs_com(
        plt,
        br_rows,
        figures_dir / "total_vs_com_mun.png",
        smooth_days,
        dpi,
        start_str,
        end_str,
    )
    _plot_seasonality(plt, seasonality_rows, figures_dir / "seasonality_uf_top10.png", dpi)
    _plot_hotspots(
        plt,
        hotspots_rows,
        figures_dir / "hotspots_top_count.png",
        figures_dir / "hotspots_top_density.png",
        fig_top_n,
        dpi,
        start_str,
        end_str,
    )

    _plot_shifts(
        plt,
        shifts_rows,
        figures_dir / "shifts_topn.png",
        shifts_top_n,
        dpi,
        start_str,
        end_str,
        window_days,
    )

    _log("done")
