from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import requests

from .config import settings


@dataclass(frozen=True)
class ExtractResult:
    file_date: date
    url: str
    path: Path


def build_daily_brasil_url(d: date) -> str:
    fname = f"focos_diario_br_{d.strftime('%Y%m%d')}.csv"
    base = settings.inpe_base_url.rstrip("/") + "/"
    return urljoin(base, fname)


def download_daily_csv(d: date) -> ExtractResult:
    url = build_daily_brasil_url(d)

    out_dir = Path(settings.data_dir) / "raw" / "inpe" / "focos" / "diario_brasil"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{d.isoformat()}.csv"

    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        raise FileNotFoundError(f"Arquivo não encontrado no INPE para {d.isoformat()}: {url}")
    r.raise_for_status()

    out_path.write_bytes(r.content)
    return ExtractResult(file_date=d, url=url, path=out_path)
