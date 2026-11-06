"""DuckDB connection with MinIO (S3) and Postgres access, shared by both generators."""
from __future__ import annotations

import os
import pathlib

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_DIR = ROOT / ".data" / "duckdb"

# Addresses as seen from the host (the k3d load balancer forwards the NodePorts)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "15432")


def load_env() -> None:
    """Load .env into the environment without overwriting existing values."""
    env = ROOT / ".env"
    if not env.exists():
        raise SystemExit(".env is missing. cp .env.example .env")
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def connect(db_name: str) -> duckdb.DuckDBPyConnection:
    """Return a connection with the extensions loaded and MinIO credentials set.

    Args:
        db_name: database file name under ./.data/duckdb.
    """
    load_env()
    DB_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_DIR / db_name))
    for ext in ("tpcds", "httpfs", "postgres"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_region='us-east-1'")
    con.execute(f"SET s3_access_key_id='{os.environ['MINIO_ROOT_USER']}'")
    con.execute(f"SET s3_secret_access_key='{os.environ['MINIO_ROOT_PASSWORD']}'")
    return con


def pg_dsn(database: str = "source") -> str:
    """Build a libpq DSN for the cluster's Postgres, reachable from the host."""
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={database} "
        f"user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
    )


def attach_pg(con: duckdb.DuckDBPyConnection, alias: str = "pg", database: str = "source") -> None:
    """Attach the Postgres database to this DuckDB connection under alias."""
    con.execute(f"DETACH DATABASE IF EXISTS {alias}")
    con.execute(f"ATTACH '{pg_dsn(database)}' AS {alias} (TYPE postgres)")
