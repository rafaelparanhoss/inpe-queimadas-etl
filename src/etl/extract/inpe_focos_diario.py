from __future__ import annotations

import csv
import logging
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

from ..config import settings

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


@dataclass(frozen=True)
class ExtractResult:
    file_date: date
    url: str
    path: Path


_MONTHLY_CANDIDATES = [
    "focos_mensal_br_{ym}.csv",
    "focos_mensal_br_{ym}.zip",
    "focos_mensal_{ym}.csv",
    "focos_mensal_{ym}.zip",
]


# build INPE daily CSV URL for a date
def build_daily_brasil_url(d: date) -> str:
    fname = f"focos_diario_br_{d.strftime('%Y%m%d')}.csv"
    base = settings.inpe_base_url.rstrip("/") + "/"
    url = urljoin(base, fname)
    log.debug("build url | date=%s | url=%s", d.isoformat(), url)
    return url


def _build_monthly_urls(d: date) -> list[str]:
    ym = d.strftime("%Y%m")
    base = settings.inpe_monthly_base_url.rstrip("/") + "/"
    return [urljoin(base, pattern.format(ym=ym)) for pattern in _MONTHLY_CANDIDATES]


def _download_file(
    url: str,
    out_path: Path,
    *,
    timeout: int,
    session: requests.Session,
) -> bool:
    r = session.get(url, timeout=timeout, stream=True)
    if r.status_code == 404:
        return False
    r.raise_for_status()

    with out_path.open("wb") as handle:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    return True


def _extract_zip_to_csv(zip_path: Path, csv_path: Path) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"no csv found in zip: {zip_path}")
        name = csv_names[0]
        with zf.open(name) as src, csv_path.open("wb") as dst:
            while True:
                data = src.read(1024 * 1024)
                if not data:
                    break
                dst.write(data)


def _detect_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,")
    except csv.Error:
        return csv.excel


def _find_date_col(cols: list[str]) -> int:
    def norm(c: str) -> str:
        return c.strip().lower().replace(" ", "_")

    normalized = [norm(c) for c in cols]
    preferred = [
        "data_hora_gmt",
        "datahora",
        "data_hora",
        "datahora_gmt",
        "data_hora_utc",
        "datahora_utc",
    ]
    for key in preferred:
        if key in normalized:
            return normalized.index(key)

    for idx, name in enumerate(normalized):
        if "data" in name and ("hora" in name or "gmt" in name):
            return idx

    raise ValueError(f"data_hora_gmt column not found | cols={cols[:50]}")


def _extract_date(value: str) -> Optional[date]:
    v = value.strip()
    if not v:
        return None

    iso_match = re.search(r"\d{4}-\d{2}-\d{2}", v)
    if iso_match:
        return date.fromisoformat(iso_match.group(0))

    br_match = re.search(r"\d{2}/\d{2}/\d{4}", v)
    if br_match:
        dstr = br_match.group(0)
        day_s, month_s, year_s = dstr.split("/")
        return date(int(year_s), int(month_s), int(day_s))

    return None


def _filter_monthly_to_daily(monthly_csv: Path, d: date, out_path: Path) -> int:
    rows = 0
    with monthly_csv.open("r", newline="", encoding="utf-8-sig", errors="replace") as src:
        sample = src.read(4096)
        src.seek(0)
        dialect = _detect_dialect(sample)
        reader = csv.reader(src, dialect)
        header = next(reader)
        date_idx = _find_date_col(header)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as dst:
            writer = csv.writer(dst, dialect)
            writer.writerow(header)
            for row in reader:
                if date_idx >= len(row):
                    continue
                day = _extract_date(row[date_idx])
                if day == d:
                    writer.writerow(row)
                    rows += 1

    return rows


def _download_monthly_csv(
    d: date,
    *,
    timeout: int,
    force: bool,
    session: requests.Session,
) -> tuple[Path, str]:
    out_dir = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "mensal_brasil"
    out_dir.mkdir(parents=True, exist_ok=True)

    month_key = d.strftime("%Y-%m")
    csv_path = out_dir / f"{month_key}.csv"
    zip_path = out_dir / f"{month_key}.zip"

    if csv_path.exists() and not force:
        size = csv_path.stat().st_size
        if size > 0:
            log.info("monthly cache hit | month=%s | size_bytes=%s | path=%s", month_key, size, csv_path.as_posix())
            return csv_path, "cache"

    urls = _build_monthly_urls(d)
    for url in urls:
        if url.endswith(".zip"):
            target_path = zip_path
        else:
            target_path = csv_path

        if target_path.exists() and not force:
            size = target_path.stat().st_size
            if size > 0:
                if target_path.suffix == ".zip" and not csv_path.exists():
                    _extract_zip_to_csv(zip_path, csv_path)
                log.info("monthly cache hit | month=%s | size_bytes=%s | path=%s", month_key, size, target_path.as_posix())
                return csv_path if csv_path.exists() else target_path, url

        log.info("monthly download start | month=%s | url=%s", month_key, url)
        ok = _download_file(url, target_path, timeout=timeout, session=session)
        if not ok:
            log.info("monthly not found | url=%s", url)
            continue

        size = target_path.stat().st_size
        log.info("monthly download ok | month=%s | size_bytes=%s | path=%s", month_key, size, target_path.as_posix())

        if target_path.suffix == ".zip":
            _extract_zip_to_csv(zip_path, csv_path)
            return csv_path, url

        return csv_path, url

    raise FileNotFoundError(f"monthly source not found for {d.isoformat()}")


def _download_daily_csv(
    d: date,
    *,
    timeout: int,
    force: bool,
    session: requests.Session,
) -> ExtractResult:
    url = build_daily_brasil_url(d)

    out_dir = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "diario_brasil"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{d.isoformat()}.csv"

    if out_path.exists() and not force:
        size = out_path.stat().st_size
        if size > 0:
            log.info(
                "extract cache hit | date=%s | size_bytes=%s | path=%s",
                d.isoformat(),
                size,
                out_path.as_posix(),
            )
            return ExtractResult(file_date=d, url=url, path=out_path)

    t0 = time.perf_counter()
    log.info("extract download start | date=%s", d.isoformat())

    r = session.get(url, timeout=timeout)
    if r.status_code == 404:
        raise requests.HTTPError("daily not found", response=r)
    r.raise_for_status()

    out_path.write_bytes(r.content)
    size = out_path.stat().st_size
    dt = time.perf_counter() - t0

    log.info(
        "extract download ok | date=%s | dt=%.2fs | size_bytes=%s | path=%s",
        d.isoformat(),
        dt,
        size,
        out_path.as_posix(),
    )
    return ExtractResult(file_date=d, url=url, path=out_path)


# download and cache the daily CSV
def download_daily_csv(
    d: date,
    *,
    timeout: int = 60,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> ExtractResult:
    sess = session or requests.Session()

    cutoff = date.today() - timedelta(days=settings.inpe_retention_days)
    try_monthly_first = d <= cutoff

    if try_monthly_first:
        try:
            monthly_csv, monthly_url = _download_monthly_csv(
                d, timeout=timeout, force=force, session=sess
            )
            out_path = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "diario_brasil" / f"{d.isoformat()}.csv"
            rows = _filter_monthly_to_daily(monthly_csv, d, out_path)
            log.info(
                "extract source=monthly | date=%s | monthly_path=%s | rows=%s | path=%s",
                d.isoformat(),
                monthly_csv.as_posix(),
                rows,
                out_path.as_posix(),
            )
            return ExtractResult(file_date=d, url=monthly_url, path=out_path)
        except requests.HTTPError:
            log.info("monthly not available | date=%s | fallback=daily", d.isoformat())
        except FileNotFoundError:
            log.info("monthly not found | date=%s | fallback=daily", d.isoformat())

    try:
        ex = _download_daily_csv(d, timeout=timeout, force=force, session=sess)
        log.info("extract source=daily | date=%s | path=%s", d.isoformat(), ex.path.as_posix())
        return ex
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            log.warning("daily not found | date=%s | fallback=monthly", d.isoformat())
        else:
            raise

    monthly_csv, monthly_url = _download_monthly_csv(d, timeout=timeout, force=force, session=sess)
    out_path = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "diario_brasil" / f"{d.isoformat()}.csv"
    rows = _filter_monthly_to_daily(monthly_csv, d, out_path)
    log.info(
        "extract source=monthly | date=%s | monthly_path=%s | rows=%s | path=%s",
        d.isoformat(),
        monthly_csv.as_posix(),
        rows,
        out_path.as_posix(),
    )
    return ExtractResult(file_date=d, url=monthly_url, path=out_path)
