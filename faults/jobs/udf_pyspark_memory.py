"""pandas UDF under a tight pyspark.memory: fails on RLIMIT_AS or OOMKilled."""
from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql.types import LongType, StructField, StructType

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402

OUT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("nbytes", LongType()),
])


def widen(batches):
    """Scan every string column with the batch materialised as pandas.

    The batch itself is small; what binds first is RLIMIT_AS from pyspark.memory.
    """
    for pdf in batches:
        cols = [c for c in pdf.columns if c.startswith("c")]
        total = sum(pdf[c].str.len() for c in cols)
        print(f"PYBATCH rows={len(pdf)} cols={len(cols)}", flush=True)
        yield pd.DataFrame({"id": pdf["id"].astype("int64"), "nbytes": total.astype("int64")})


def main() -> int:
    spark = _common.session("udf_pyspark_memory")
    src = spark.read.parquet(f"{_common.LANDING}/synthetic/wide_rows/data_1.parquet")
    out = src.mapInPandas(widen, schema=OUT_SCHEMA)
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
