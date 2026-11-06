"""pandas UDF with a 1g Arrow batch over wide rows: fails OOMKilled (exit 137)."""
from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql.types import LongType, StructField, StructType

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402

OUT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("grp", LongType()),
    StructField("nbytes", LongType()),
])


def row_bytes(batches):
    """Scan every string column with the whole batch held in memory."""
    for pdf in batches:
        cols = [c for c in pdf.columns if c.startswith("c")]
        total = sum(pdf[c].str.len() for c in cols)
        print(f"PYBATCH rows={len(pdf)} cols={len(cols)}", flush=True)
        yield pd.DataFrame({
            "id": pdf["id"].astype("int64"),
            "grp": pdf["grp"].astype("int64"),
            "nbytes": total.astype("int64"),
        })


def main() -> int:
    spark = _common.session("udf_oom_killed")
    src = spark.read.parquet(f"{_common.LANDING}/synthetic/wide_rows")
    out = src.mapInPandas(row_bytes, schema=OUT_SCHEMA)
    _common.run(out)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
