from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path

from .sql_runner import run_sql_file

log = logging.getLogger("apply_sql")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sort_key(path: Path) -> tuple[int, str]:
    match = re.match(r"^(\d+)_", path.name)
    prefix = int(match.group(1)) if match else 999999
    return (prefix, path.name)


def _parse_vars(values: list[str]) -> dict[str, str]:
    vars_dict: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"invalid --var {item} (expected KEY=VALUE)")
        key, value = item.split("=", 1)
        vars_dict[key] = value
    return vars_dict


def _requires_date(path: Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return (":'DATE'" in text) or (":DATE" in text)


def _is_stub(path: Path) -> bool:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    active = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        active.append(stripped)
    if len(active) != 1:
        return False
    return bool(re.match(r"^\\\i\s+\S+", active[0]))


@dataclass
class ApplyStats:
    applied: int = 0
    skipped_date: int = 0
    skipped_dry: int = 0
    skipped_stub: int = 0
    failed: int = 0


def _run_dir(
    dir_path: Path,
    vars_dict: dict[str, str] | None,
    dry_run: bool,
    stats: ApplyStats,
    engine: str | None,
    dsn: str | None,
) -> None:
    if not dir_path.exists():
        raise FileNotFoundError(f"dir not found: {dir_path}")

    files = sorted(
        [p for p in dir_path.rglob("*.sql") if p.is_file()],
        key=lambda p: (_sort_key(p), p.relative_to(dir_path).as_posix()),
    )
    if not files:
        raise RuntimeError(f"no sql files in {dir_path}")

    for path in files:
        if _is_stub(path):
            log.info("skip stub | path=%s", path.name)
            stats.skipped_stub += 1
            continue
        if _requires_date(path) and (not vars_dict or "DATE" not in vars_dict):
            log.info("skip sql | missing var DATE | path=%s", path.name)
            stats.skipped_date += 1
            continue
        if dry_run:
            log.info("dry-run | would apply | path=%s", path.name)
            stats.skipped_dry += 1
            continue
        t0 = time.perf_counter()
        log.info("apply sql | path=%s", path.as_posix())
        try:
            run_sql_file(str(path), vars_dict, engine=engine, dsn=dsn)
            stats.applied += 1
            log.info("apply ok | path=%s | dt=%.2fs", path.name, time.perf_counter() - t0)
        except Exception:
            stats.failed += 1
            raise


def apply_dirs(
    dir_paths: list[Path],
    vars_dict: dict[str, str] | None,
    dry_run: bool,
    engine: str | None = None,
    dsn: str | None = None,
) -> ApplyStats:
    stats = ApplyStats()
    for dir_path in dir_paths:
        _run_dir(dir_path, vars_dict, dry_run, stats, engine, dsn)
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="apply sql files in a directory")
    parser.add_argument("--dir", action="append", required=True, help="sql directory (repeatable)")
    parser.add_argument("--var", action="append", default=[], help="psql var KEY=VALUE")
    parser.add_argument("--date", help="date in YYYY-MM-DD (sets DATE var)")
    parser.add_argument("--dry-run", action="store_true", help="show order without executing")
    parser.add_argument(
        "--engine",
        choices=["docker", "direct", "auto"],
        default="auto",
        help="execution engine (default: auto)",
    )
    parser.add_argument("--dsn", help="direct connection dsn (optional)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    vars_dict = _parse_vars(args.var) if args.var else {}
    if args.date and "DATE" not in vars_dict:
        vars_dict["DATE"] = args.date
    if not vars_dict:
        vars_dict = None
    repo_root = _repo_root()

    dir_paths: list[Path] = []
    for dir_value in args.dir:
        dir_path = Path(dir_value)
        if not dir_path.is_absolute():
            dir_path = repo_root / dir_path
        dir_paths.append(dir_path)

    engine = None if args.engine == "auto" else args.engine
    stats = apply_dirs(dir_paths, vars_dict, args.dry_run, engine=engine, dsn=args.dsn)
    log.info(
        "summary | applied=%s | skipped_date=%s | skipped_dry=%s | skipped_stub=%s | failed=%s",
        stats.applied,
        stats.skipped_date,
        stats.skipped_dry,
        stats.skipped_stub,
        stats.failed,
    )


if __name__ == "__main__":
    main()
