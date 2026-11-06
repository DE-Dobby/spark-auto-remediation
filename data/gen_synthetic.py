"""Synthetic data for the conditions TPC-DS cannot produce.

Usage: .venv/bin/python data/gen_synthetic.py [--regen]

| Output | Location | Used by |
|---|---|---|
| skew_events | s3://landing/synthetic/skew_events/ (Parquet) | skew_agg - one key is 60% |
| wide_rows   | s3://landing/synthetic/wide_rows/   (Parquet) | udf_oom_killed / udf_pyspark_memory |
| big_events  | Postgres source.public.big_events            | jdbc_single_partition |

wide_rows is built to be small on disk and large in memory: 40 string columns of 256
characters is about 10 KB per row, but each column has only 50 distinct values, so
Parquet dictionary encoding compresses it well. Materialised through pandas or Arrow it
grows back in proportion to the batch size.
"""
from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _duck  # noqa: E402

SKEW_ROWS = 6_000_000
WIDE_ROWS = 500_000
WIDE_COLS = 40
PG_ROWS = 1_500_000

S3 = "s3://landing/synthetic"


def build_skew(con, regen: bool) -> None:
    """Build skew_events, where one key holds 60% of the rows."""
    if _exists(con, "skew_events") and not regen:
        print("[skip] skew_events")
    else:
        print(f"[gen] skew_events {SKEW_ROWS:,} rows ...", flush=True)
        t0 = time.time()
        con.execute(
            f"""
            CREATE OR REPLACE TABLE skew_events AS
            SELECT
                CASE
                    WHEN r < 0.60 THEN 0                                    -- one key, 60%
                    WHEN r < 0.80 THEN CAST(floor(random() * 4) AS INT) + 1  -- top 5, 80%
                    ELSE CAST(floor(random() * 2000) AS INT) + 5
                END                                   AS k,
                random() * 100                        AS v,
                'seg' || CAST(CAST(floor(random() * 8) AS INT) AS VARCHAR) AS seg
            FROM (SELECT random() AS r FROM range({SKEW_ROWS}))
            """
        )
        print(f"      {time.time() - t0:.1f}s")
    _copy(con, "skew_events", f"{S3}/skew_events")


def build_wide(con, regen: bool) -> None:
    """Build wide_rows: small on disk, large once materialised in memory."""
    if _exists(con, "wide_rows") and not regen:
        print("[skip] wide_rows")
    else:
        print(f"[gen] wide_rows {WIDE_ROWS:,} rows x {WIDE_COLS} cols ...", flush=True)
        cols = ",\n                ".join(
            f"repeat(md5(CAST((i + {j}) % 50 AS VARCHAR)), 8) AS c{j:02d}"
            for j in range(WIDE_COLS)
        )
        t0 = time.time()
        con.execute(
            f"""
            CREATE OR REPLACE TABLE wide_rows AS
            SELECT
                i AS id,
                CAST(i % 500 AS INT) AS grp,
                {cols}
            FROM range({WIDE_ROWS}) t(i)
            """
        )
        print(f"      {time.time() - t0:.1f}s")
    _copy(con, "wide_rows", f"{S3}/wide_rows")


def build_pg(con, regen: bool) -> None:
    """Build big_events in Postgres, too large for a single JDBC partition."""
    if _exists(con, "big_events") and not regen:
        print("[skip] big_events")
    else:
        print(f"[gen] big_events {PG_ROWS:,} rows ...", flush=True)
        t0 = time.time()
        con.execute(
            f"""
            CREATE OR REPLACE TABLE big_events AS
            SELECT
                i                                        AS id,
                CAST(i % 1000 AS INT)                    AS customer_sk,
                random() * 1000                          AS amount,
                repeat(md5(CAST(i % 97 AS VARCHAR)), 30) AS payload  -- 960 chars: too big for one partition
            FROM range({PG_ROWS}) t(i)
            """
        )
        print(f"      {time.time() - t0:.1f}s")

    print("[postgres] big_events → source.public.big_events", flush=True)
    _duck.attach_pg(con, "pg", "source")
    t0 = time.time()
    con.execute("DROP TABLE IF EXISTS pg.public.big_events")
    con.execute("CREATE TABLE pg.public.big_events AS SELECT * FROM big_events")
    print(f"           {time.time() - t0:.1f}s")


def _exists(con, table: str) -> bool:
    """Return True if the table is already present."""
    return table in {r[0] for r in con.execute("SHOW TABLES").fetchall()}


def _copy(con, table: str, dst: str) -> None:
    """Copy a DuckDB table to the given destination."""
    print(f"[parquet] {table} → {dst}/", flush=True)
    t0 = time.time()
    con.execute(
        f"COPY {table} TO '{dst}' (FORMAT PARQUET, PER_THREAD_OUTPUT true, OVERWRITE_OR_IGNORE true)"
    )
    print(f"           {time.time() - t0:.1f}s")


def verify(con) -> bool:
    """Check row counts, the skew distribution and wide_rows' in-memory size."""
    print("\n=== row count check ===")
    ok = True

    for t in ("skew_events", "wide_rows"):
        src = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        got = con.execute(f"SELECT count(*) FROM read_parquet('{S3}/{t}/*.parquet')").fetchone()[0]
        files, nbytes = con.execute(
            f"""SELECT count(DISTINCT file_name), sum(total_compressed_size)
                FROM parquet_metadata('{S3}/{t}/*.parquet')"""
        ).fetchone()
        good = src == got
        ok &= good
        print(
            f"{t:<14} source={src:>10,} parquet={got:>10,} files={files:>2} "
            f"compressed={nbytes / 1e6:>7.1f} MB  {'OK' if good else 'MISMATCH'}"
        )

    src = con.execute("SELECT count(*) FROM big_events").fetchone()[0]
    got = con.execute("SELECT count(*) FROM pg.public.big_events").fetchone()[0]
    good = src == got
    ok &= good
    print(f"{'big_events':<14} source={src:>10,} postgres={got:>10,}  {'OK' if good else 'MISMATCH'}")

    print("\n=== skew distribution (top 5 keys) ===")
    rows = con.execute(
        """
        SELECT k, count(*) AS n, count(*) * 100.0 / (SELECT count(*) FROM skew_events) AS pct
        FROM skew_events GROUP BY k ORDER BY n DESC LIMIT 5
        """
    ).fetchall()
    cum = 0.0
    for k, n, pct in rows:
        cum += pct
        print(f"  k={k:<5} {n:>10,} rows  {pct:>5.1f}%  (cumulative {cum:>5.1f}%)")
    distinct_k = con.execute("SELECT count(DISTINCT k) FROM skew_events").fetchone()[0]
    print(f"  distinct k = {distinct_k:,}")
    top_pct = rows[0][2]
    skew_ok = top_pct > 50
    ok &= skew_ok
    print(f"  top key share > 50% : {'OK' if skew_ok else 'FAIL'}")

    print("\n=== wide_rows in-memory size ===")
    raw = con.execute(
        f"SELECT sum(strlen(c00)) * {WIDE_COLS} / count(*) FROM read_parquet('{S3}/wide_rows/*.parquet')"
    ).fetchone()[0]
    print(f"  string bytes per row ~ {raw:,.0f} B -> a 100k-row batch ~ {raw * 100_000 / 1e9:.2f} GB")

    print(f"\ncheck result: {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> int:
    """CLI entry point: build everything and verify."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--regen", action="store_true")
    args = ap.parse_args()

    con = _duck.connect("synthetic.db")
    build_skew(con, args.regen)
    build_wide(con, args.regen)
    build_pg(con, args.regen)
    return 0 if verify(con) else 1


if __name__ == "__main__":
    sys.exit(main())
