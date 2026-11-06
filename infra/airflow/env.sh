# Airflow environment, sourced by the Makefile targets.
# Secrets live only in .env; this file is committed.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../.." && pwd)"
set -a
. "$ROOT/.env"
set +a

export AIRFLOW_HOME="$ROOT/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/dags"
# LocalExecutor runs tasks inside the scheduler process. SequentialExecutor was removed in 3.0.
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# Metadata DB in a host Docker container, independent of k3d so history survives a cluster delete
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@localhost:${AIRFLOW_DB_PORT}/airflow"
# Host port 8080 is already taken
export AIRFLOW_API_PORT=8081
# Airflow 3 runs tasks through the Execution API, whose default address is
# http://localhost:8080. Changing the API server port without these two makes workers
# unable to pick up tasks, and the task fails with no log at all.
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:${AIRFLOW_API_PORT}/execution/"
export AIRFLOW__API__BASE_URL="http://localhost:${AIRFLOW_API_PORT}"
