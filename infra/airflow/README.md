# Airflow setup

Airflow is what runs the batch jobs and, when one fails, re-runs it after the judgment.
It runs **on the host**, not inside the cluster.

## Why on the host

Airflow does not execute Spark. It only creates the `SparkApplication` CR through the
Kubernetes API; the cluster's spark-operator does the execution. So it needs nothing but
API access, and works just as well from outside the cluster.

It is kept out of the cluster to save memory. Two agents of 3.5GiB is all there is, and
what this project tries to show is recovery with less memory. Giving 1-2GiB to an
orchestrator would take that away from the experiment. The limit is measured: an executor
pod whose container total exceeds the node's free memory stays `Pending` forever.

Moving it into the cluster later changes nothing in the DAG or task code - only the
endpoints and `in_cluster`.

## Prerequisites

- The k3d cluster is up and `make up` has finished.
- These three are reachable from the host; Airflow and the judgment code read all of them.

```bash
kubectl --context k3d-spark-ar get ns        # Kubernetes API (localhost:6550)
curl -s localhost:18080/api/v1/applications  # Spark History Server
nc -z localhost 15432                        # the ops database
```

- Docker, for the metadata DB container. The `postgres:16-alpine` image is reused.
- Python 3.10-3.14. This repository pins **3.13**.

## 1. Metadata database

Holds Airflow's own state: DAG run history and task states. It lives in a **container
independent of k3d** so the history survives deleting the cluster. Host port 5432 is
already in use, so this uses **5433**.

`.env` must carry the credentials (see `.env.example`):

```
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=...
AIRFLOW_DB_PORT=5433
```

```bash
set -a && . ./.env && set +a
docker run -d --name airflow-postgres \
  -e POSTGRES_USER="$AIRFLOW_DB_USER" \
  -e POSTGRES_PASSWORD="$AIRFLOW_DB_PASSWORD" \
  -e POSTGRES_DB=airflow \
  -p 127.0.0.1:${AIRFLOW_DB_PORT}:5432 \
  -v airflow-pg-data:/var/lib/postgresql/data \
  --restart unless-stopped \
  postgres:16-alpine
```

Only needed once. After that `make airflow-up` restarts a stopped container itself.

## 2. Install Airflow

**Always use the constraints file.** Airflow has many dependencies and installing without
it produces mismatched versions. The official docs support only `pip` and `uv`.

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install --upgrade pip

.venv/bin/pip install "apache-airflow[cncf-kubernetes,postgres]==3.3.2" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt"
```

It takes **427MB**. `psycopg2-binary` and `pyyaml` come with it, so **this is the only
venv in the repository**: the collector, classifier and `faults/submit.py` all run here.
They have to, because the judgment runs inside Airflow's `retry_policy`.

To change versions, first check that
`constraints-<airflow version>/constraints-<python version>.txt` exists. That file decides
the provider version (3.3.2 pins `cncf-kubernetes==10.22.0`).

## 3. Configuration

`infra/airflow/env.sh` holds all of it and the Makefile targets source it. Secrets are
read only from `.env`, so that file is committed.

| Variable | Value | Why |
|---|---|---|
| `AIRFLOW_HOME` | `./.airflow` | inside the repository, gitignored |
| `AIRFLOW__CORE__DAGS_FOLDER` | `./dags` | |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | `SequentialExecutor` was removed in 3.0 |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` | the example DAGs bury the list |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://...@localhost:5433/airflow` | |
| `AIRFLOW_API_PORT` | `8081` | **host 8080 is already in use** |

```bash
. infra/airflow/env.sh
.venv/bin/airflow db migrate
```

Not `airflow db init`, which was deprecated in 2.7.

## 4. Cluster connection

```bash
. infra/airflow/env.sh
.venv/bin/airflow connections add kubernetes_default \
  --conn-type kubernetes \
  --conn-extra "{\"in_cluster\": false, \"kube_config_path\": \"$HOME/.kube/config\",
                 \"cluster_context\": \"k3d-spark-ar\", \"namespace\": \"spark\"}"
```

`in_cluster: false` means it connects with a kubeconfig rather than from inside a pod.

Check it:

```bash
.venv/bin/python -c "
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
h = KubernetesHook(conn_id='kubernetes_default')
print([n.metadata.name for n in h.core_v1_client.list_namespace().items])
r = h.custom_object_client.list_namespaced_custom_object(
    group='sparkoperator.k8s.io', version='v1beta2',
    namespace='spark', plural='sparkapplications')
print(len(r.get('items', [])), 'SparkApplication')
"
```

## 5. Running it

3.x runs **four separate processes**. This differs from 2.x.

| Process | Role |
|---|---|
| `api-server` | UI and REST (not 2.x's `webserver`) |
| `scheduler` | LocalExecutor runs the tasks inside this process |
| `dag-processor` | parses the DAGs; **a separate process is required since 3.0** |
| `triggerer` | for deferrable tasks; unused here, but its absence warns |

```bash
make airflow-up       # check the metadata DB, then start the four processes
make airflow-status
make airflow-down
```

The UI is at http://localhost:8081. The default auth manager in 3.3 is SimpleAuthManager
and the password is generated into
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` (it is not always printed to
the terminal).

## Common problems

**`No airflow-postgres container`** - step 1 has not been done. `start.sh` prints the
command.

**Port conflicts** - 8080 and 5432 are both in use on this host, hence 8081 and 5433. To
change them, edit `AIRFLOW_API_PORT` in `env.sh` and `AIRFLOW_DB_PORT` in `.env`.

**A process dies immediately** - read `.airflow/run/<name>.log`. `make airflow-status`
reports which one is down.

**Callback errors are invisible** - Airflow writes exceptions raised in callback functions
to the **dag processor log, not the task log**. That is why the judgment does not live in
a callback here.

## Verified against the installation

Read out of the installed packages, not the documentation.

```
airflow.sdk -> RetryPolicy, RetryDecision, ExceptionRetryPolicy, RetryRule, RetryAction
RetryPolicy.evaluate(self, exception, try_number, max_tries, context=None) -> RetryDecision
RetryDecision.retry / .fail / .default
```

`retry_policy` (AIP-105) landed in Airflow **3.3**. The judgment sits there: it runs
synchronously in the worker process between catching the exception and deciding the task's
next state, and its decision is logged to the task log. `on_retry_callback` is not used
because the docs do not guarantee that ordering.

### SparkKubernetesOperator (10.22.0) defaults

Three of them **must** be changed when writing a DAG.

| Parameter | Default | Use | Why |
|---|---|---|---|
| `delete_on_termination` | `True` | **`False`** | the default deletes **the failed SparkApplication too**, losing exit 137/52, the failed stage shape and the plan - everything the collector reads after the fact |
| `reattach_on_restart` | `True` | **`False`** | attaching to a live driver would ignore the judged setting |
| `random_name_suffix` | `True` | `True` (keep) | a new random name per attempt keeps each attempt's evidence |
| `deferrable` | `False` | `False` (keep) | its retry behaviour is undocumented and it had a 422 bug |

`on_kill()` ignores `delete_on_termination` and **always deletes**. Killing a task by hand
destroys the evidence.

## Measured - things the documentation does not say

A probe DAG that fails on purpose (`retries=2` plus a custom `RetryPolicy`) was run four
times. The probe was deleted afterwards.

### 1. Changing the port makes tasks fail with no log

The Execution API's default address is **`http://localhost:8080`**. Airflow 3 runs tasks
through that API, so changing only `api-server --port 8081` leaves workers unable to pick
up tasks.

```
Task SDK: ServerResponseError: Not Found
scheduler: state=queued, executor_state=failed, run_start_date=None
-> no task log is created at all
```

`env.sh` exports `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` and `AIRFLOW__API__BASE_URL`
alongside the API port. Changing the port again means changing those two as well.

### 2. `evaluate()` runs synchronously and tolerates a multi-second collection

```
attempt 1   evaluate_start ts=...530.279  try_number=1 max_tries=2
            collect() -> error_class='CAST_INVALID_INPUT' exceptions=2 executors=2
            evaluate_end   ts=...531.472  elapsed=1.2s   decision=RETRY
attempt 2   evaluate_start ts=...561.032  (the next attempt started after it)
```

A real `collector.collect()` took 1.2s and the next attempt started afterwards. The log
goes to the **task log**, with `ti_id` and `try_number` attached.

`retries=2` means **three attempts**: `max_tries` is printed as the retry count and
`try_number` comes through as 1, 2, 3.

### 3. Airflow's labels do not reach the driver pod

Every label on the driver pod:

```
job-name=ansi_error  run-id=probe-ansi          <- what we put in the spec
spark-app-name=probe-ansi-qddps7lq
sparkoperator.k8s.io/app-name=probe-ansi-qddps7lq
already_checked=True   spark-role=driver   ...
```

There is no `dag_id`, `task_id` or `run_id`. spark-operator does not propagate the
KubernetesPodOperator labels to the driver pod, so the job identity has to be put into the
SparkApplication spec by the DAG.

Note that a `run-id` label in the spec is the same value on every attempt, so attempts are
told apart by the CR name prefix instead.

### 4. `delete_on_termination=False` preserves the evidence

Each attempt creates a new randomly named CR (`probe-ansi-mktca81s`,
`probe-ansi-qddps7lq`, ...) and **none is deleted**. The pods survive too - one driver and
two executors per attempt, which is what makes collection possible. Four runs of two
attempts left all eight CRs in place.

### 5. When the worker dies the policy does not run at that moment

The scheduler was `SIGKILL`ed mid-task (LocalExecutor runs tasks inside it).

```
right after the kill   attempt=1.log -> 0 PROBE lines    the policy did not run
scheduler restarted    attempt=1.log -> 8 PROBE lines    the attempt ran again, policy with it
```

The judgment is not lost. But **the SparkApplication that the dead attempt submitted stays
in the cluster** with nobody having judged it at the time. Whether the re-run picks that CR
up depends on how it chooses its target, which is why the task start reconciles any
uncollected CR.

## The operating DAG

`dags/spark_jobs.py` - one DAG per job (`spark_<job name>`), reflecting all of the above.

```
task fails
  -> RetryPolicy.evaluate()   collect, classify, suggest, write one job_overrides row
  -> Airflow runs the next attempt
  -> the task merges the override and submits (with a per-attempt CR name)
```

### Measured - recovery with no human involvement

```
t1  arrow=1g    FAILED 67s  python_mem/container_sigkill  -> override arrow=256m
t2  arrow=256m  FAILED 62s  python_mem/container_sigkill  -> override arrow=64m
t3  arrow=64m   COMPLETED 24s                             -> waiting to be folded into the DAG
```

`non_resource` **stops after one attempt** (`ansi_error`, `retryable=false`).

### Stop conditions

| Condition | Note |
|---|---|
| `MAX_TRIES` attempts reached | |
| `retryable=false` (`non_resource`, `unknown`) | configuration cannot fix it |
| no suggestion | lever at its ceiling, rule conditions unmet |
| node free memory exceeded | the pod would stay Pending forever |
| no progress | the same cause returns **the same suggestion** |

The last one matters. "Stop when the cause repeats" cuts off a recovery that walks a lever
one step at a time - in the run above, t2 had the same cause. What must stop it is the
suggestion staying the same.
