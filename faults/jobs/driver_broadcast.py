"""Forces a broadcast of a ~100MB table: fails in the driver, not the executors."""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402


def main() -> int:
    spark = _common.session("driver_broadcast")
    # The broadcast side has to be the large one for the driver to run out of heap.
    big = spark.read.parquet(f"{_common.LANDING}/tpcds/catalog_sales")
    small = spark.read.parquet(f"{_common.LANDING}/tpcds/store_returns")

    # Defeat column pruning: broadcasting only the used columns shrinks it to 23MB,
    # which the driver survives.
    blob = F.concat_ws(",", *[F.col(c).cast("string") for c in big.columns])
    big_wide = big.select(F.col("cs_item_sk"), blob.alias("row_blob"))

    joined = small.join(F.broadcast(big_wide), small.sr_item_sk == big_wide.cs_item_sk, "inner")
    out = (joined.groupBy("sr_store_sk")
           .agg(F.count("*").alias("n"), F.max(F.length("row_blob")).alias("blob_len")))
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
