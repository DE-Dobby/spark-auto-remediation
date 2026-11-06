# Spark 4.1.4 + Postgres JDBC + pandas/pyarrow
# Base image: Ubuntu 22.04 / Python 3.10.12 / uid 185 (spark)
#
# Iceberg is left out: the jobs have no load target (sink=noop), so nothing uses it.
# The AWS SDK v2 bundle (612 MB) is left out too, because it would be copied to every
# node; it is mounted by hostPath from ./.data/jars instead.
# hadoop-aws is needed for the event log's s3a, postgresql for the JDBC source reads.
# Pinned versions and their reasons are in the README.
FROM apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

ARG HADOOP_VERSION=3.4.2
ARG PG_JDBC_VERSION=42.7.14
ARG NUMPY_VERSION=2.2.6
ARG PANDAS_VERSION=2.3.3
ARG PYARROW_VERSION=25.0.1

USER root

ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    for url in \
      "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "$MAVEN/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
    ; do curl -fsSL -o "/opt/spark/jars/$(basename "$url")" "$url"; done; \
    ls -l /opt/spark/jars/hadoop-aws-* /opt/spark/jars/postgresql-*

# PySpark 4.1.4 requires pandas>=2.2.0 and pyarrow>=15.0.0.
# Pinned to the newest versions that publish a Python 3.10 (cp310) wheel.
RUN set -eux; \
    pip3 install --no-cache-dir \
      "numpy==${NUMPY_VERSION}" \
      "pandas==${PANDAS_VERSION}" \
      "pyarrow==${PYARROW_VERSION}"; \
    python3 -c "import numpy, pandas, pyarrow; print(numpy.__version__, pandas.__version__, pyarrow.__version__)"

USER spark
