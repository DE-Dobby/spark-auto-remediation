#!/usr/bin/env bash
# Start Airflow on the host. 3.x runs four separate processes.
#   api-server     UI/REST
#   scheduler       LocalExecutor runs the tasks inside this process
#   dag-processor   a separate process since 3.0; parses the DAGs
#   triggerer       for deferrable tasks; unused here but warns if absent
set -euo pipefail
cd "$(dirname "$0")/../.."
. infra/airflow/env.sh

VENV=.venv/bin
RUN="$AIRFLOW_HOME/run"
mkdir -p "$RUN"

# Metadata DB container, independent of k3d so history survives a cluster delete
if ! docker ps --format '{{.Names}}' | grep -qx airflow-postgres; then
  if docker ps -a --format '{{.Names}}' | grep -qx airflow-postgres; then
    docker start airflow-postgres >/dev/null && echo "  airflow-postgres restarted"
  else
    echo "No airflow-postgres container. Create it first:" >&2
    echo "  docker run -d --name airflow-postgres -e POSTGRES_USER=\$AIRFLOW_DB_USER \\" >&2
    echo "    -e POSTGRES_PASSWORD=\$AIRFLOW_DB_PASSWORD -e POSTGRES_DB=airflow \\" >&2
    echo "    -p 127.0.0.1:\$AIRFLOW_DB_PORT:5432 -v airflow-pg-data:/var/lib/postgresql/data \\" >&2
    echo "    --restart unless-stopped postgres:16-alpine" >&2
    exit 1
  fi
fi
for i in $(seq 1 20); do
  docker exec airflow-postgres pg_isready -U "$AIRFLOW_DB_USER" -d airflow >/dev/null 2>&1 && break
  sleep 2
done

start() {
  local name="$1"; shift
  local pidfile="$RUN/$name.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "  $name already running (pid $(cat "$pidfile"))"; return
  fi
  nohup "$@" > "$RUN/$name.log" 2>&1 &
  echo $! > "$pidfile"
  echo "  $name started (pid $!)"
}

start api-server   "$VENV/airflow" api-server --port "$AIRFLOW_API_PORT"
start scheduler    "$VENV/airflow" scheduler
start dag-processor "$VENV/airflow" dag-processor
start triggerer    "$VENV/airflow" triggerer
echo "UI: http://localhost:$AIRFLOW_API_PORT"
