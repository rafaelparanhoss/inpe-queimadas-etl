from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

from .config import settings

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


@dataclass(frozen=True)
class ExtractResult:
    file_date: date
    url: str
    path: Path


# build INPE daily CSV URL for a date
def build_daily_brasil_url(d: date) -> str:
    fname = f"focos_diario_br_{d.strftime('%Y%m%d')}.csv"
    base = settings.inpe_base_url.rstrip("/") + "/"
    url = urljoin(base, fname)
    log.debug("build url | date=%s | url=%s", d.isoformat(), url)
    return url


# download and cache the daily CSV
def download_daily_csv(
    d: date,
    *,
    timeout: int = 60,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> ExtractResult:
    url = build_daily_brasil_url(d)

    out_dir = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "diario_brasil"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{d.isoformat()}.csv"

    log.debug("extract paths | out_dir=%s | out_path=%s", out_dir.as_posix(), out_path.as_posix())

    if out_path.exists() and not force:
        size = out_path.stat().st_size
        if size > 0:
            log.info("extract cache hit | date=%s | size_bytes=%s | path=%s", d.isoformat(), size, out_path.as_posix())
            return ExtractResult(file_date=d, url=url, path=out_path)
        log.warning("extract cache empty file | date=%s | path=%s", d.isoformat(), out_path.as_posix())

    sess = session or requests.Session()
    t0 = time.perf_counter()

    log.info("extract download start | date=%s", d.isoformat())
    log.debug("http get | url=%s | timeout=%ss", url, timeout)

    try:
        r = sess.get(url, timeout=timeout)
    except requests.RequestException as e:
        log.exception("http error | date=%s | url=%s", d.isoformat(), url)
        raise RuntimeError(f"falha ao baixar csv do INPE para {d.isoformat()}") from e

    dt = time.perf_counter() - t0
    log.debug(
        "http response | status=%s | dt=%.2fs | content_length=%s",
        r.status_code,
        dt,
        r.headers.get("Content-Length"),
    )

    if r.status_code == 404:
        log.warning("inpe file not found | date=%s | url=%s", d.isoformat(), url)
        raise FileNotFoundError(f"arquivo não encontrado no INPE para {d.isoformat()}: {url}")

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        log.error("http non-200 | date=%s | status=%s | url=%s", d.isoformat(), r.status_code, url)
        raise

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp_path.write_bytes(r.content)
    tmp_size = tmp_path.stat().st_size

    if tmp_size == 0:
        log.error("download produced empty file | date=%s | url=%s | tmp=%s", d.isoformat(), url, tmp_path.as_posix())
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"download vazio do INPE para {d.isoformat()}: {url}")

    tmp_path.replace(out_path)

    log.info("extract download ok | date=%s | dt=%.2fs | size_bytes=%s | path=%s", d.isoformat(), dt, tmp_size, out_path.as_posix())
    return ExtractResult(file_date=d, url=url, path=out_path)
