"""Casts a random string column to int: fails with [CAST_INVALID_INPUT]."""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402


def main() -> int:
    spark = _common.session("ansi_error")
    src = spark.read.parquet(f"{_common.LANDING}/synthetic/wide_rows")
    out = (src.select("id", "grp", F.col("c00").cast("int").alias("c00_as_int")))
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
