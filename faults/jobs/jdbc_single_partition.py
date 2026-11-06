"""Reads 1.5M rows over JDBC with no partition column: fails with a JVM heap OOM."""
from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402

TABLE = "public.big_events"
UPPER_BOUND = 1_500_000


def main() -> int:
    spark = _common.session("jdbc_single_partition")
    props = _common.pg_props()

    num_partitions = os.environ.get("JDBC_NUM_PARTITIONS")
    if num_partitions:
        src = spark.read.jdbc(_common.PG_URL, TABLE, column="id", lowerBound=0,
                              upperBound=UPPER_BOUND, numPartitions=int(num_partitions),
                              properties=props)
    else:
        src = spark.read.jdbc(_common.PG_URL, TABLE, properties=props)
    print(f"JOB jdbc_partitions={src.rdd.getNumPartitions()}", flush=True)

    out = (src.groupBy("customer_sk")
           .agg(F.count("*").alias("n"),
                F.sum("amount").alias("amount_sum"),
                F.max(F.length("payload")).alias("payload_len")))
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
