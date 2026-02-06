from __future__ import annotations

import os
import shutil
import subprocess
import urllib.request
import zipfile
from pathlib import Path

import psycopg


def _log(message: str) -> None:
    print(f"[ensure_ref_ibge] {message}", flush=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def _psql_value(container: str, db_user: str, db_name: str, sql: str) -> str:
    cmd = [
        "docker",
        "exec",
        "-i",
        "-e",
        "PAGER=cat",
        container,
        "psql",
        "-U",
        db_user,
        "-d",
        db_name,
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-c",
        sql,
    ]
    result = _run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "psql failed")
    return result.stdout.replace("\r", "").strip()


def _psql_exec(container: str, db_user: str, db_name: str, sql: str) -> None:
    cmd = [
        "docker",
        "exec",
        "-i",
        "-e",
        "PAGER=cat",
        container,
        "psql",
        "-U",
        db_user,
        "-d",
        db_name,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    result = _run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "psql failed")


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as resp, dest.open("wb") as handle:
        shutil.copyfileobj(resp, handle)


def _unpack_zip(src: Path, dest: Path) -> None:
    with zipfile.ZipFile(src, "r") as zf:
        zf.extractall(dest)


def _find_shp(data_dir: Path) -> Path:
    for shp in sorted(data_dir.rglob("*.shp")):
        return shp
    raise FileNotFoundError(f"shp not found in {data_dir}")


def _docker_network(container: str) -> str:
    cmd = [
        "docker",
        "inspect",
        "-f",
        "{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}",
        container,
    ]
    result = _run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker inspect failed")
    for line in result.stdout.splitlines():
        value = line.strip()
        if value:
            return value
    raise RuntimeError(f"docker network not found for {container}")


def _run_ogr2ogr(
    data_dir: Path,
    shp_path: Path,
    container: str,
    network: str,
    db_user: str,
    db_name: str,
    db_pass: str,
) -> None:
    rel_path = shp_path.relative_to(data_dir).as_posix()
    data_mount = f"{data_dir.resolve()}:/data"
    conn = f"PG:host={container} port=5432 dbname={db_name} user={db_user} password={db_pass}"

    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        data_mount,
        "ghcr.io/osgeo/gdal:alpine-small-latest",
        "ogr2ogr",
        "-overwrite",
        "-f",
        "PostgreSQL",
        conn,
        f"/data/{rel_path}",
        "-nln",
        "ref.ibge_municipios_raw",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=gid",
        "-nlt",
        "MULTIPOLYGON",
        "-t_srs",
        "EPSG:4326",
    ]
    result = _run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ogr2ogr failed")


def _detect_engine(engine: str | None) -> str:
    if engine:
        if engine not in ("docker", "direct"):
            raise ValueError(f"invalid engine: {engine}")
        return engine
    if os.getenv("DOCKER_CONTAINER") or os.getenv("DB_CONTAINER"):
        return "docker"
    return "direct"


def _direct_check() -> None:
    conn = psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "geoetl"),
        user=os.getenv("DB_USER", "geoetl"),
        password=os.getenv("DB_PASSWORD", "geoetl"),
    )
    with conn, conn.cursor() as cur:
        cur.execute("select to_regclass('ref.ibge_municipios');")
        if cur.fetchone()[0] is None:
            raise RuntimeError(
                "ref.ibge_municipios missing; load it first or run with --engine docker"
            )
        cur.execute("select count(*) from ref.ibge_municipios;")
        count = int(cur.fetchone()[0] or 0)
        if count < 5000:
            raise RuntimeError(
                f"ref.ibge_municipios count too low ({count}); load it first"
            )


def ensure_ref_ibge(engine: str | None = None) -> None:
    engine = _detect_engine(engine)
    if engine == "direct":
        _direct_check()
        _log("ok (direct)")
        return
    repo_root = _repo_root()
    data_dir = repo_root / "data" / "ref" / "ibge_municipios"
    zip_name = "BR_Municipios_2022.zip"
    zip_path = data_dir / zip_name
    url = (
        "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
        "malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/"
        "BR_Municipios_2022.zip"
    )

    db_user = os.getenv("DB_USER", "geoetl")
    db_name = os.getenv("DB_NAME", "geoetl")
    db_pass = os.getenv("DB_PASSWORD", "geoetl")
    container = os.getenv("DB_CONTAINER", "geoetl_postgis")

    _log("check ref.ibge_municipios")
    count_str = _psql_value(container, db_user, db_name, "select count(*) from ref.ibge_municipios;")
    count = int(count_str) if count_str.isdigit() else 0
    if count >= 5000:
        _log(f"ok | count={count}")
        return

    _log(f"load ref.ibge_municipios | current_count={count}")
    data_dir.mkdir(parents=True, exist_ok=True)

    if zip_path.exists() and zip_path.stat().st_size < 1_000_000:
        zip_path.unlink()

    if not zip_path.exists():
        _log(f"download | url={url}")
        _download(url, zip_path)

    _unpack_zip(zip_path, data_dir)
    shp_path = _find_shp(data_dir)
    network = _docker_network(container)

    _log(f"ogr2ogr import | shp={shp_path.name}")
    _run_ogr2ogr(data_dir, shp_path, container, network, db_user, db_name, db_pass)

    _psql_exec(
        container,
        db_user,
        db_name,
        """
truncate ref.ibge_municipios;
insert into ref.ibge_municipios (cd_mun, nm_mun, uf, area_km2, geom)
select
  cd_mun::text,
  nm_mun::text,
  sigla_uf::text,
  coalesce(area_km2, st_area(geom::geography) / 1000000.0),
  geom
from ref.ibge_municipios_raw;

drop table if exists ref.ibge_municipios_raw;
""".strip(),
    )

    count_str = _psql_value(container, db_user, db_name, "select count(*) from ref.ibge_municipios;")
    count = int(count_str) if count_str.isdigit() else 0
    if count < 5000:
        raise RuntimeError(f"ref.ibge_municipios load failed | count={count}")

    _log(f"done | count={count}")
