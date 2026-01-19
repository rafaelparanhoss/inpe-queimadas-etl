from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

from .config import settings
from .extract.inpe_focos_diario import download_daily_csv
from .load.postgis import load_records
from .transform.inpe_focos_diario import transform_inpe_csv

_filename = Path(__file__).stem
log = logging.getLogger(_filename)


def _setup_logging() -> Path:
    # set up console and file logging
    log_dir = Path(settings.data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "etl.log"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)

    return log_file


def _try_load_dotenv() -> None:
    # load .env if python-dotenv is installed; not a hard dependency
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def run(date_str: str) -> None:
    # run the ETL flow for a single date
    _try_load_dotenv()
    _setup_logging()

    t0 = time.perf_counter()
    file_date = date.fromisoformat(date_str)

    log.info("etl start | date=%s", file_date.isoformat())

    # extract csv from the INPE source
    t_extract = time.perf_counter()
    ex = download_daily_csv(file_date)
    dt_extract = time.perf_counter() - t_extract

    log.info("extract ok | dt=%.2fs | url=%s", dt_extract, ex.url)

    # transform into normalized records
    t_transform = time.perf_counter()
    records = transform_inpe_csv(str(ex.path), file_date=file_date)
    dt_transform = time.perf_counter() - t_transform

    log.info("transform ok | dt=%.2fs | rows=%s", dt_transform, len(records))

    # load into the database
    t_load = time.perf_counter()
    attempted = load_records(records)
    dt_load = time.perf_counter() - t_load

    log.info("load ok | dt=%.2fs | rows_attempted=%s", dt_load, attempted)
    log.info("etl done | total_dt=%.2fs", time.perf_counter() - t0)

    print(f"Downloaded: {ex.url}")
    print(f"Rows parsed: {len(records)}")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    # parse command line arguments
    parser = argparse.ArgumentParser(description="run inpe queimadas etl")
    parser.add_argument("--date", required=True, help="date in YYYY-MM-DD")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    # main entrypoint
    args = _parse_args(argv)
    run(args.date)


if __name__ == "__main__":
    main()
