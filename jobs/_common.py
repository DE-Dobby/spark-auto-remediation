"""적재 잡 공통부: SparkSession, Iceberg 쓰기, run_id."""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

RUN_ID = os.environ.get("RUN_ID", "local")
JOB_NAME = os.environ.get("JOB_NAME", "job")

LANDING = "s3a://landing"


def session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(app).getOrCreate()
    print(f"JOB name={JOB_NAME} run_id={RUN_ID} spark={spark.version}", flush=True)
    return spark


def write_iceberg(df: DataFrame, table: str) -> None:
    """대상 Iceberg 테이블에 쓴다. 스냅샷 summary 에 run_id 를 남겨
    나중에 '커밋 후 실패' 를 판정할 수 있게 한다."""
    spark = df.sparkSession
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.marts")
    full = f"lake.marts.{table}"
    df.writeTo(full).option("snapshot-property.run_id", RUN_ID).using("iceberg").createOrReplace()
    print(f"JOB wrote={full}", flush=True)
