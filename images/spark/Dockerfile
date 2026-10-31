# Spark 4.1.4 + Iceberg 1.12.0 + Postgres JDBC + pandas/pyarrow
# 베이스: Ubuntu 22.04 / Python 3.10.12 / uid 185(spark)
#
# AWS SDK v2 bundle(612 MB)은 일부러 넣지 않는다. 노드마다 복제되기 때문에
# 호스트 ./.data/jars → 모든 노드 /var/lib/rancher/k3s/storage/jars 에서 hostPath 로 붙인다.
# Iceberg 쓰기는 iceberg-aws-bundle(S3FileIO, 자체 SDK 포함)로 처리되고,
# event log 의 s3a 만 그 bundle 이 필요하다. docs/versions.md 참조.
FROM apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.4.2
ARG PG_JDBC_VERSION=42.7.14
ARG NUMPY_VERSION=2.2.6
ARG PANDAS_VERSION=2.3.3
ARG PYARROW_VERSION=25.0.1

USER root

ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    for url in \
      "$MAVEN/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "$MAVEN/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
    ; do curl -fsSL -o "/opt/spark/jars/$(basename "$url")" "$url"; done; \
    ls -l /opt/spark/jars/iceberg-* /opt/spark/jars/hadoop-aws-* /opt/spark/jars/postgresql-*

# PySpark 4.1.4 요구 최소치: pandas>=2.2.0, pyarrow>=15.0.0
# Python 3.10(cp310) 휠이 있는 최신으로 고정
RUN set -eux; \
    pip3 install --no-cache-dir \
      "numpy==${NUMPY_VERSION}" \
      "pandas==${PANDAS_VERSION}" \
      "pyarrow==${PYARROW_VERSION}"; \
    python3 -c "import numpy, pandas, pyarrow; print(numpy.__version__, pandas.__version__, pyarrow.__version__)"

USER spark
