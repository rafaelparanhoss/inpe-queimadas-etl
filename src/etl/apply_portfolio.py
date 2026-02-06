from __future__ import annotations

import argparse

from . import validate_marts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="apply minimal portfolio sql and validate")
    parser.add_argument("--engine", choices=["docker", "direct", "auto"], default="auto")
    parser.add_argument("--dsn", help="direct connection dsn (optional)")
    parser.add_argument("--date", help="date in YYYY-MM-DD (optional for DATE-param SQL)")
    parser.add_argument("--dry-run", action="store_true", help="show order without executing")
    args = parser.parse_args(argv)

    v_args: list[str] = ["--apply-minimal"]
    if args.engine:
        v_args += ["--engine", args.engine]
    if args.dsn:
        v_args += ["--dsn", args.dsn]
    if args.date:
        v_args += ["--date", args.date]
    if args.dry_run:
        v_args += ["--dry-run"]

    validate_marts.main(v_args)


if __name__ == "__main__":
    main()
