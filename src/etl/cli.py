from __future__ import annotations

from datetime import date

import typer

from .inpe_extract import download_daily_csv
from .transform import transform_inpe_csv
from .load import load_records


def main(d: str = typer.Option(..., "--date", help="Data no formato YYYY-MM-DD")):
    file_date = date.fromisoformat(d)

    ex = download_daily_csv(file_date)
    records = transform_inpe_csv(str(ex.path), file_date=file_date)
    load_records(records)

    typer.echo(f"Downloaded: {ex.url}")
    typer.echo(f"Rows parsed: {len(records)}")


if __name__ == "__main__":
    typer.run(main)
