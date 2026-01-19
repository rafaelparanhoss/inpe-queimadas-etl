from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
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

    if out_path.exists() and not force:
        size = out_path.stat().st_size
        if size > 0:
            log.info("extract cache hit | date=%s | size_bytes=%s | path=%s", d.isoformat(), size, out_path.as_posix())
            return ExtractResult(file_date=d, url=url, path=out_path)

    sess = session or requests.Session()
    t0 = time.perf_counter()

    log.info("extract download start | date=%s", d.isoformat())

    r = sess.get(url, timeout=timeout)
    r.raise_for_status()

    out_path.write_bytes(r.content)
    size = out_path.stat().st_size
    dt = time.perf_counter() - t0

    log.info("extract download ok | date=%s | dt=%.2fs | size_bytes=%s | path=%s", d.isoformat(), dt, size, out_path.as_posix())
    return ExtractResult(file_date=d, url=url, path=out_path)
