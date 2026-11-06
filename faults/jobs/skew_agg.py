"""collect_list on a hot key holding 60% of rows: fails with a JVM heap OOM."""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402


def main() -> int:
    spark = _common.session("skew_agg")
    src = spark.read.parquet(f"{_common.LANDING}/synthetic/skew_events")

    # Widen each row to 64 characters so one group holds more memory. 128 characters
    # did not finish within 15 minutes even at 2g; 32 characters succeeded at 640m.
    widened = src.withColumn("payload", F.repeat(F.md5(F.col("seg")), 2))

    out = (widened.groupBy("k")
           .agg(F.count("*").alias("n"),
                F.sum("v").alias("v_sum"),
                F.collect_list("payload").alias("payloads"))
           .select("k", "n", "v_sum", F.size("payloads").alias("payload_count")))
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
