from __future__ import annotations

import argparse
import logging
import os
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("validate_repo")


@dataclass
class RepoValidation:
    sql_checked: int = 0
    sql_failed: int = 0
    dirs_checked: int = 0
    dirs_failed: int = 0
    git_dry_run_ok: bool = True
    git_failure_kind: str | None = None
    git_failure_detail: str | None = None


REQUIRED_SQL_FILES = [
    "sqlm/ref_core/00_build_ref_core.sql",
    "sqlm/ref_core/01_ref_schema.sql",
    "sqlm/ref_core/05_ref_uf_area.sql",
    "sqlm/ref_core/10_ref_geo_prepare.sql",
    "sqlm/marts/prereq/010_mv_uf_geom_mainland.sql",
    "sqlm/marts/prereq/020_mv_uf_mainland_poly_noholes.sql",
    "sqlm/marts/prereq/030_mv_uf_polycoords_polygon_superset.sql",
    "sqlm/marts/canonical/040_v_chart_uf_choropleth_day.sql",
    "sqlm/marts/canonical/050_v_chart_mun_choropleth_day.sql",
    "sqlm/marts/canonical/055_v_focos_enriched_full.sql",
    "sqlm/marts/canonical/060_v_chart_focos_scatter.sql",
    "sqlm/marts/canonical/065_mv_focos_day_dim.sql",
    "sql/enrich/20_enrich_municipio.sql",
    "sql/marts/10_focos_diario_municipio.sql",
    "sql/marts/20_focos_diario_uf.sql",
    "sql/checks/010_superset_uf_choropleth.sql",
    "sql/checks/020_superset_mun_choropleth.sql",
    "sql/checks/030_superset_scatter.sql",
    "sql/checks/040_enriched_full_coverage.sql",
    "sql/checks/050_mv_focos_day_dim.sql",
    "sql/checks/060_mun_polycoords.sql",
]

REQUIRED_SQL_DIRS = [
    "sql/ref",
    "sql/enrich",
    "sql/marts",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _validate_sql_file(path: Path) -> tuple[bool, str | None]:
    if not path.exists():
        return False, "file not found"
    if path.stat().st_size <= 0:
        return False, "empty file"
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False, "not utf-8"
    if text.strip() == "":
        return False, "blank file"
    return True, None


def _check_git_add_dry_run(repo_root: Path) -> tuple[bool, str | None]:
    cmd = ["git", "add", "-A", "--dry-run"]
    result = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return True, None
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    message = stderr or stdout or "git add dry-run failed"
    return False, message


def _check_git_dir_writable(repo_root: Path) -> tuple[bool, str | None]:
    git_dir = repo_root / ".git"
    if not git_dir.exists():
        return False, ".git directory not found"
    if not os.access(git_dir, os.W_OK):
        return False, ".git directory is not writable"
    return True, None


def _check_index_lock_state(repo_root: Path) -> tuple[bool, str | None]:
    lock_path = repo_root / ".git" / "index.lock"
    if lock_path.exists():
        return False, f"index.lock present at {lock_path.as_posix()}"
    return True, None


def _classify_git_failure(message: str | None) -> tuple[str, str]:
    msg = (message or "").lower()
    if ".git directory is not writable" in msg:
        return (
            "git_dir_not_writable",
            "check folder permissions, antivirus lock, and whether terminal has write rights to .git",
        )
    if "index.lock" in msg and "permission denied" in msg:
        return (
            "index_lock_permission",
            "close git/vscode handles, then run powershell -ExecutionPolicy Bypass -File devtools/win_git_diag.ps1 and powershell -ExecutionPolicy Bypass -File devtools/win_git_index_recover.ps1",
        )
    if "index.lock" in msg:
        return (
            "index_lock_present",
            "run devtools/win_git_diag (ps1 or sh), then win_git_index_recover.ps1; remove lock only when no git process is running",
        )
    if "no such file or directory" in msg:
        return (
            "path_not_found",
            "check sql path references and manifest, run devtools/win_git_diag (ps1/sh) for target file, then run win_git_repair.sh or win_git_index_recover.ps1",
        )
    if "not a git repository" in msg:
        return (
            "not_git_repo",
            "run command from repository root",
        )
    return (
        "git_add_unknown",
        "inspect git stderr and run dry-run again",
    )


def _read_git_config(repo_root: Path, key: str) -> str | None:
    result = subprocess.run(
        ["git", "config", "--get", key],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    value = (result.stdout or "").strip()
    return value or None


def run_validation(check_git: bool = True) -> RepoValidation:
    repo_root = _repo_root()
    stats = RepoValidation()

    log.info("validate repo | root=%s", repo_root.as_posix())
    log.info(
        "platform | os=%s | release=%s | msystem=%s",
        platform.system(),
        platform.release(),
        os.getenv("MSYSTEM", "-"),
    )
    for key in (
        "core.longpaths",
        "core.protectNTFS",
        "core.precomposeunicode",
        "core.autocrlf",
    ):
        log.info("git config | %s=%s", key, _read_git_config(repo_root, key))
    log.info("expected sql files | count=%s", len(REQUIRED_SQL_FILES))
    for rel_path in REQUIRED_SQL_FILES:
        path = repo_root / rel_path
        ok, err = _validate_sql_file(path)
        stats.sql_checked += 1
        if ok:
            log.info("sql ok | %s", rel_path)
            continue
        stats.sql_failed += 1
        log.error("sql fail | %s | reason=%s", rel_path, err)

    for rel_dir in REQUIRED_SQL_DIRS:
        dir_path = repo_root / rel_dir
        stats.dirs_checked += 1
        if not dir_path.exists():
            stats.dirs_failed += 1
            log.error("dir fail | missing | %s", rel_dir)
            continue
        sql_files = list(dir_path.rglob("*.sql"))
        if not sql_files:
            stats.dirs_failed += 1
            log.error("dir fail | no sql files | %s", rel_dir)
            continue
        log.info("dir ok | %s | sql_count=%s", rel_dir, len(sql_files))

    if check_git and platform.system().lower().startswith("win"):
        lock_ok, lock_err = _check_index_lock_state(repo_root)
        if not lock_ok:
            stats.git_dry_run_ok = False
            kind, hint = _classify_git_failure(lock_err)
            stats.git_failure_kind = kind
            stats.git_failure_detail = lock_err
            log.error("git lock check | fail | kind=%s | %s", kind, lock_err)
            log.error("git lock hint | %s", hint)

        writable, writable_err = _check_git_dir_writable(repo_root)
        if not writable:
            stats.git_dry_run_ok = False
            kind, hint = _classify_git_failure(writable_err)
            stats.git_failure_kind = kind
            stats.git_failure_detail = writable_err
            log.error("git check | kind=%s | %s", kind, writable_err)
            log.error("git check hint | %s", hint)

        ok, err = _check_git_add_dry_run(repo_root)
        stats.git_dry_run_ok = stats.git_dry_run_ok and ok
        if ok:
            log.info("git add dry-run | ok")
        else:
            kind, hint = _classify_git_failure(err)
            stats.git_failure_kind = kind
            stats.git_failure_detail = err
            log.error("git add dry-run | fail | kind=%s | %s", kind, err)
            log.error("git add hint | %s", hint)
            log.error(
                "fix command | powershell -ExecutionPolicy Bypass -File devtools/win_git_index_recover.ps1"
            )

    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="validate repo sql files and windows git behavior")
    parser.add_argument(
        "--git-check",
        choices=["strict", "warn", "off"],
        default="strict",
        help="strict: fail on git dry-run issues; warn: report but do not fail; off: skip git check",
    )
    parser.add_argument("--no-git-dry-run", action="store_true", help="deprecated alias for --git-check off")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    git_mode = "off" if args.no_git_dry_run else args.git_check
    stats = run_validation(check_git=(git_mode != "off"))
    log.info(
        "summary | sql_checked=%s | sql_failed=%s | dirs_checked=%s | dirs_failed=%s | git_dry_run_ok=%s | git_failure_kind=%s",
        stats.sql_checked,
        stats.sql_failed,
        stats.dirs_checked,
        stats.dirs_failed,
        stats.git_dry_run_ok,
        stats.git_failure_kind,
    )
    git_failed = not stats.git_dry_run_ok
    if git_mode == "warn" and git_failed:
        log.warning("git check failed but mode=warn, continuing without failure")
        git_failed = False

    if stats.sql_failed or stats.dirs_failed or git_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
