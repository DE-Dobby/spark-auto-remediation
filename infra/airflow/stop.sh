#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
. infra/airflow/env.sh
RUN="$AIRFLOW_HOME/run"
for name in api-server scheduler dag-processor triggerer; do
  pidfile="$RUN/$name.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" && echo "  $name stopped (pid $pid)"; fi
    rm -f "$pidfile"
  fi
done
