from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg

from .config import settings
from .db_bootstrap import ensure_database
from .enrich_runner import run_enrich
from .marts_runner import run_marts

log = logging.getLogger("postprocess_range")


def _state_path() -> Path:
    state_dir = Path(settings.data_dir) / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / "postprocess_range.json"


def _read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log.warning("state read failed | path=%s", path.as_posix())
        return {}


def _write_state(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def _table_exists(cur: psycopg.Cursor, name: str) -> bool:
    cur.execute("select to_regclass(%s);", (name,))
    return cur.fetchone()[0] is not None


def _rename_table_if_needed(cur: psycopg.Cursor, old_name: str, new_name: str) -> None:
    if _table_exists(cur, new_name):
        return
    if not _table_exists(cur, old_name):
        log.info("skip rename | table missing | table=%s", old_name)
        return
    schema, table = old_name.split(".", 1)
    new_table = new_name.split(".", 1)[1]
    cur.execute(f"alter table {schema}.{table} rename to {new_table};")
    log.info("rename table | from=%s | to=%s", old_name, new_name)


def _ensure_gist_index(cur: psycopg.Cursor, table: str, index_name: str) -> bool:
    if not _table_exists(cur, table):
        log.info("skip index | table missing | table=%s", table)
        return False
    cur.execute(f"create index if not exists {index_name} on {table} using gist (geom);")
    return True


def _ensure_btree_index(cur: psycopg.Cursor, table: str, index_name: str, column: str) -> bool:
    if not _table_exists(cur, table):
        log.info("skip index | table missing | table=%s", table)
        return False
    cur.execute(f"create index if not exists {index_name} on {table} ({column});")
    return True


def _analyze(cur: psycopg.Cursor, table: str) -> None:
    if not _table_exists(cur, table):
        log.info("skip analyze | table missing | table=%s", table)
        return
    cur.execute(f"analyze {table};")


def _ensure_postprocess_prereqs() -> None:
    targets = [
        ("ref.biomas_4326", "idx_ref_biomas_4326_geom"),
        ("ref.ucs_4326", "idx_ref_ucs_4326_geom"),
        ("ref.tis_4326", "idx_ref_tis_4326_geom"),
        ("curated.inpe_focos_enriched", "idx_curated_inpe_focos_enriched_geom"),
    ]
    date_index = ("curated.inpe_focos_enriched", "idx_curated_inpe_focos_enriched_file_date", "file_date")

    with _connect() as conn, conn.cursor() as cur:
        _rename_table_if_needed(cur, "ref.biomas", "ref.biomas_4326")
        _rename_table_if_needed(cur, "ref.ucs", "ref.ucs_4326")
        _rename_table_if_needed(cur, "ref.tis", "ref.tis_4326")
        for table, index_name in targets:
            created = _ensure_gist_index(cur, table, index_name)
            if created:
                log.info("index ok | table=%s | index=%s", table, index_name)
        created = _ensure_btree_index(cur, *date_index)
        if created:
            log.info("index ok | table=%s | index=%s", date_index[0], date_index[1])
        for table, _ in targets:
            _analyze(cur, table)
        _analyze(cur, date_index[0])
        conn.commit()


def run_postprocess_range(start_str: str, end_str: str, resume: bool) -> None:
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    if start > end:
        raise ValueError("start date must be <= end date")

    state_file = _state_path()
    state = _read_state(state_file)

    current = start
    if resume and state:
        stored_start = state.get("start")
        stored_end = state.get("end")
        if stored_start == start_str and stored_end == end_str:
            last_completed = state.get("last_completed")
            if last_completed:
                current = date.fromisoformat(last_completed) + timedelta(days=1)
        else:
            log.warning(
                "state range mismatch | stored_start=%s | stored_end=%s | start=%s | end=%s",
                stored_start,
                stored_end,
                start_str,
                end_str,
            )

    if current > end:
        log.info("postprocess already complete | start=%s | end=%s", start_str, end_str)
        return

    ensure_database()
    _ensure_postprocess_prereqs()

    n_ok = 0
    n_fail = 0
    first_fail = None

    while current <= end:
        t0 = time.perf_counter()
        try:
            run_enrich(current.isoformat())
            run_marts(current.isoformat())
            n_ok += 1
            _write_state(
                state_file,
                {
                    "start": start_str,
                    "end": end_str,
                    "last_completed": current.isoformat(),
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
            )
            log.info("day ok | date=%s | dt=%.2fs", current.isoformat(), time.perf_counter() - t0)
        except Exception as exc:
            n_fail += 1
            first_fail = first_fail or current.isoformat()
            log.error("day fail | date=%s | err=%s", current.isoformat(), exc)
            break
        current = current + timedelta(days=1)

    log.info("summary | n_ok=%s | n_fail=%s | first_fail=%s", n_ok, n_fail, first_fail or "-")
    if n_fail:
        raise SystemExit(1)
