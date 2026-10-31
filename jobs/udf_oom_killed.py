"""udf_oom_killed — pandas UDF + 넓은 행 + 큰 Arrow batch, overhead 작게.

소스: s3a://landing/synthetic/wide_rows (문자열 40열 × 256자, 행당 약 10KB)
실패: mapInPandas 한 배치가 수십만 행 → Python 프로세스가 overhead 한도를 넘김
      → 컨테이너가 커널에 SIGKILL (exit 137 / OOMKilled)
기대: error_code = python_mem
대처: arrow.maxBytesPerBatch↓  (메모리 증가가 아니다)
"""
from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

sys.path.insert(0, "/opt/jobs")
import _common  # noqa: E402

OUT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("grp", LongType()),
    StructField("nbytes", LongType()),
])


def row_bytes(batches):
    """배치를 전부 메모리에 올린 상태로 문자열 열 전체를 훑는다."""
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
    out = src.mapInPandas(row_bytes, schema=OUT_SCHEMA).withColumn("run_id", F.lit(_common.RUN_ID))
    _common.write_iceberg(out, "wide_rows_bytes")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
