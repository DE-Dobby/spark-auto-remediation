"""Generate TPC-DS with DuckDB dsdgen into MinIO landing and Postgres source.

Usage: .venv/bin/python data/gen_tpcds.py [--sf 1] [--regen]

The layout exists so the failure jobs have both source types to read:
    fact tables       -> Parquet in several files, giving the read stage its task count
    some dimensions   -> CSV, for a read path with schema inference and no compression
    some dimensions   -> Postgres, for the JDBC read path
"""
from __future__ import annotations

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
import _duck  # noqa: E402

FACT_PARQUET = ["store_sales", "store_returns", "catalog_sales"]
DIM_CSV = ["date_dim", "item"]
DIM_PG = ["customer", "customer_address", "store", "household_demographics"]

S3_PARQUET = "s3://landing/tpcds"
S3_CSV = "s3://landing/tpcds_csv"


def generate(con, sf: float, regen: bool) -> None:
    """Run DuckDB dsdgen at the given scale factor, skipping if data exists."""
    existing = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "store_sales" in existing and not regen:
        n = con.execute("SELECT count(*) FROM store_sales").fetchone()[0]
        print(f"[skip] dsdgen - store_sales already has {n:,} rows (use --regen)")
        return
    print(f"[gen] dsdgen(sf={sf}) ...", flush=True)
    t0 = time.time()
    con.execute(f"CALL dsdgen(sf={sf})")
    print(f"[gen] done in {time.time() - t0:.1f}s")


def export_parquet(con) -> None:
    """Write the fact tables to MinIO landing as multi-file Parquet."""
    for t in FACT_PARQUET:
        dst = f"{S3_PARQUET}/{t}"
        print(f"[parquet] {t} → {dst}/", flush=True)
        t0 = time.time()
        con.execute(
            f"COPY {t} TO '{dst}' (FORMAT PARQUET, PER_THREAD_OUTPUT true, OVERWRITE_OR_IGNORE true)"
        )
        print(f"           {time.time() - t0:.1f}s")


def export_csv(con) -> None:
    """Write some dimensions to MinIO landing as CSV."""
    for t in DIM_CSV:
        dst = f"{S3_CSV}/{t}.csv"
        print(f"[csv] {t} → {dst}", flush=True)
        con.execute(f"COPY {t} TO '{dst}' (FORMAT CSV, HEADER true)")


def export_pg(con) -> None:
    """Write some dimensions to the Postgres source database."""
    _duck.attach_pg(con, "pg", "source")
    for t in DIM_PG:
        print(f"[postgres] {t} → source.public.{t}", flush=True)
        con.execute(f"DROP TABLE IF EXISTS pg.public.{t}")
        con.execute(f"CREATE TABLE pg.public.{t} AS SELECT * FROM {t}")


def verify(con) -> bool:
    """Compare row counts between DuckDB and each export. Returns True if all match."""
    print("\n=== row count check ===")
    print(f"{'table':<26} {'source':>12} {'read back':>12} {'target':<10} result")
    ok = True

    for t in FACT_PARQUET:
        src = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        got = con.execute(
            f"SELECT count(*) FROM read_parquet('{S3_PARQUET}/{t}/*.parquet')"
        ).fetchone()[0]
        files = con.execute(
            f"SELECT count(DISTINCT filename) FROM read_parquet('{S3_PARQUET}/{t}/*.parquet', filename=true)"
        ).fetchone()[0]
        good = src == got
        ok &= good
        print(f"{t:<26} {src:>12,} {got:>12,} {'parquet':<10} {'OK' if good else 'MISMATCH'} ({files} files)")

    for t in DIM_CSV:
        src = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        got = con.execute(
            f"SELECT count(*) FROM read_csv_auto('{S3_CSV}/{t}.csv')"
        ).fetchone()[0]
        good = src == got
        ok &= good
        print(f"{t:<26} {src:>12,} {got:>12,} {'csv':<10} {'OK' if good else 'MISMATCH'}")

    for t in DIM_PG:
        src = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        got = con.execute(f"SELECT count(*) FROM pg.public.{t}").fetchone()[0]
        good = src == got
        ok &= good
        print(f"{t:<26} {src:>12,} {got:>12,} {'postgres':<10} {'OK' if good else 'MISMATCH'}")

    print(f"\ncheck result: {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> int:
    """CLI entry point: generate, export and verify."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, default=1)
    ap.add_argument("--regen", action="store_true")
    args = ap.parse_args()

    con = _duck.connect("tpcds.db")
    generate(con, args.sf, args.regen)
    export_parquet(con)
    export_csv(con)
    export_pg(con)
    return 0 if verify(con) else 1


if __name__ == "__main__":
    sys.exit(main())
