"""Shared helpers for the failure-reproduction jobs. No load target."""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

LANDING = "s3a://landing"
PG_URL = "jdbc:postgresql://postgres.storage.svc.cluster.local:5432/source"


def session(app: str) -> SparkSession:
    """Build a SparkSession. The appName is a job-name candidate for the collector."""
    spark = SparkSession.builder.appName(app).getOrCreate()
    print(f"JOB app={app} spark={spark.version}", flush=True)
    return spark


def pg_props() -> dict[str, str]:
    """Return JDBC properties for the Postgres source database."""
    return {
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def run(df: DataFrame) -> None:
    """Force the plan to execute. The sink is noop."""
    df.write.format("noop").mode("overwrite").save()
    print("JOB action=done", flush=True)
