"""Judge a failed attempt and leave one value for the next one.

Collection and classification are reused from collector and classifier. This module
adds three things:
    job_overrides   the one value the next attempt reads
    runs            per-attempt outcome, linking suggestion to applied value to result
    stop conditions a lever at its ceiling, no progress, or config cannot help
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import classifier  # noqa: E402
import collector  # noqa: E402

MAX_TRIES = 3
# An executor pod stays Pending forever if the container total exceeds the node's free
# memory. This much of allocatable is left to the system.
NODE_HEADROOM_MB = 600


def app_prefix(job: str, dag_run_id: str, try_number: int) -> str:
    """Build the SparkApplication name prefix for one attempt.

    The operator appends eight random characters to it. Airflow's own labels do not
    reach the driver pod, so attempts are told apart by this
    prefix instead.

    Args:
        job: job name.
        dag_run_id: Airflow DAG run id.
        try_number: attempt number, starting at 1.
    """
    token = hashlib.sha1(dag_run_id.encode()).hexdigest()[:8]
    return f"{job.replace('_', '-')}-{token}-t{try_number}"


def find_app(prefix: str) -> str | None:
    """Return the most recent SparkApplication whose name starts with prefix."""
    data = collector.kubectl_json("-n", collector.NAMESPACE, "get", "sparkapplications")
    cands = [i for i in data.get("items", [])
             if i["metadata"]["name"].startswith(prefix)]
    if not cands:
        return None
    cands.sort(key=lambda i: i["metadata"].get("creationTimestamp", ""))
    return cands[-1]["metadata"]["name"]


# ---------------------------------------------------------------- overrides

def read_override(conn, job: str) -> dict | None:
    """Return the job's pending override, or None if it has none."""
    with conn.cursor() as cur:
        cur.execute("""SELECT target_path, value, from_failure_id, succeeded_at
                       FROM job_overrides WHERE job_name = %s""", (job,))
        row = cur.fetchone()
    if not row:
        return None
    return {"target_path": row[0], "value": row[1],
            "from_failure_id": row[2], "succeeded_at": row[3]}


def write_override(conn, job: str, path: str, value: str, failure_id: int | None) -> None:
    """Record the one value the job's next attempt should use.

    Replaces any previous override for that job and clears its succeeded_at.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO job_overrides (job_name, target_path, value, from_failure_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (job_name) DO UPDATE
              SET target_path = EXCLUDED.target_path, value = EXCLUDED.value,
                  from_failure_id = EXCLUDED.from_failure_id,
                  created_at = now(), succeeded_at = NULL""",
                    (job, path, value, failure_id))
    conn.commit()


def mark_override_succeeded(conn, job: str) -> None:
    """Mark the job's override as the one that succeeded.

    The row is kept, not deleted: removing it would make the next scheduled run fail
    the same way. It stays visible until a human folds the value into the DAG.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE job_overrides SET succeeded_at = now() "
                    "WHERE job_name = %s AND succeeded_at IS NULL", (job,))
    conn.commit()


# ---------------------------------------------------------------- runs

def start_run(conn, run_id: str, job: str, applied: dict | None) -> None:
    """Record at submit time what this run ran with.

    The outcome is filled in later by record_run. The value decided by a judgment
    belongs to the *next* run, so writing it here would be wrong.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO runs (run_id, job_name, app_name, applied_overrides,
                              state, submitted_by)
            VALUES (%s,%s,%s,%s,'SUBMITTED','airflow')
            ON CONFLICT (run_id) DO UPDATE
              SET applied_overrides = EXCLUDED.applied_overrides""",
                    (run_id, job, run_id,
                     json.dumps(applied, ensure_ascii=False) if applied else None))
    conn.commit()


def record_run(conn, run_id: str, job: str, app: str | None, parent_failure_id: int | None,
               applied: dict | None, state: str, ev: dict | None) -> None:
    """Fill in a run's outcome: state, duration and core-hours.

    core_hours is executor cores times instances times the run's duration.
    """
    duration = core_hours = None
    if ev:
        cfg = ev["config"]
        cores = (cfg.get("executor_cores") or 0) * (cfg.get("executor_instances") or 0)
        duration = ev.get("duration_s")
        if duration and cores:
            core_hours = round(cores * duration / 3600, 6)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO runs (run_id, job_name, app_name, parent_failure_id,
                              applied_overrides, state, duration_s, core_hours, submitted_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'airflow')
            ON CONFLICT (run_id) DO UPDATE
              SET state = EXCLUDED.state, app_name = EXCLUDED.app_name,
                  parent_failure_id = EXCLUDED.parent_failure_id,
                  duration_s = EXCLUDED.duration_s, core_hours = EXCLUDED.core_hours""",
                    (run_id, job, app, parent_failure_id,
                     json.dumps(applied, ensure_ascii=False) if applied else None,
                     state, duration, core_hours))
    conn.commit()


# ---------------------------------------------------------------- stop conditions

_K8S_UNIT = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
             "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}


def _k8s_mb(v: str | None) -> int:
    """k8s quantity ('3584Mi', '1Gi') → MiB.

    classifier._num must not be used here: '1m' is 1MiB to Spark but a milli-unit to k8s.
    """
    if not v:
        return 0
    t = str(v).strip()
    for suf, mul in _K8S_UNIT.items():
        if t.endswith(suf):
            try:
                return int(float(t[: -len(suf)]) * mul / 2**20)
            except ValueError:
                return 0
    try:
        return int(float(t) / 2**20)
    except ValueError:
        return 0


def node_free_mb() -> int:
    """Return the free memory in MiB of the most spacious schedulable node.

    allocatable alone is not enough: the requests of running pods have to be subtracted
    to get the memory an executor could actually be given.
    """
    nodes = collector.kubectl_json("get", "nodes").get("items", [])
    free = {}
    for n in nodes:
        if any(t.get("effect") == "NoSchedule" for t in (n["spec"].get("taints") or [])):
            continue
        free[n["metadata"]["name"]] = _k8s_mb(n["status"]["allocatable"].get("memory"))

    pods = collector.kubectl_json(
        "get", "pods", "-A",
        "--field-selector=status.phase!=Succeeded,status.phase!=Failed").get("items", [])
    for p in pods:
        node = p["spec"].get("nodeName")
        if node not in free:
            continue
        for c in p["spec"].get("containers", []):
            req = ((c.get("resources") or {}).get("requests") or {}).get("memory")
            free[node] -= _k8s_mb(req)

    best = max(free.values(), default=0)
    return max(0, best - NODE_HEADROOM_MB)


def exceeds_node(cfg: dict, path: str, value: str) -> bool:
    """Return True if applying this value would push the container total past the node.

    Args:
        cfg: the failed run's config snapshot.
        path: the lever's target_path.
        value: the value the suggestion would set.
    """
    parts = {
        "executor_memory": cfg.get("executor_memory"),
        "executor_overhead": cfg.get("executor_overhead"),
        "pyspark": cfg["conf"].get("spark.executor.pyspark.memory"),
    }
    key = {"executor.memory": "executor_memory",
           "executor.memoryOverhead": "executor_overhead",
           "conf.spark.executor.pyspark.memory": "pyspark"}.get(path)
    if key:
        parts[key] = value
    total_mb = 0
    for v in parts.values():
        n = classifier._num(v) if v else None
        if n:
            total_mb += int(n / 2**20)
    free = node_free_mb()
    return bool(free and total_mb > free)


def stop_reason(conn, job: str, try_number: int, cls: dict, sug: dict,
                cfg: dict, prev: dict | None) -> str | None:
    """Return the reason not to retry, or None to retry."""
    if try_number >= MAX_TRIES:
        return f"reached the maximum of {MAX_TRIES} attempts"
    if "suggest_path" not in sug:
        return sug.get("no_suggest_reason", "no suggestion")
    if exceeds_node(cfg, sug["suggest_path"], sug["suggest_to"]):
        return (f"{sug['suggest_path']}={sug['suggest_to']} exceeds the node's free memory "
                f"({node_free_mb()}MiB): the pod would stay Pending forever")
    # A repeated cause is normal: the lever moves one step at a time. Stop only when
    # there is no progress, i.e. the same suggestion comes back.
    if (prev and prev["error_code"] == cls["error_code"]
            and prev["suggest_path"] == sug["suggest_path"]
            and prev["suggest_to"] == sug["suggest_to"]):
        return (f"same cause ({cls['error_code']}) with the same suggestion "
                f"({sug['suggest_path']}={sug['suggest_to']}): no progress")
    return None


def previous_attempt(conn, job: str, dag_run_id: str, try_number: int) -> dict | None:
    """Return the cause and suggestion of the previous attempt in this DAG run.

    Scoped to this run on purpose. Looking at the job's whole history would stop the
    very first attempt as a "repeat" whenever an older run had the same cause.
    """
    if try_number <= 1:
        return None
    prev = app_prefix(job, dag_run_id, try_number - 1)
    with conn.cursor() as cur:
        cur.execute("""SELECT error_code, suggest_path, suggest_to FROM failures
                       WHERE app_name LIKE %s ORDER BY collected_at DESC LIMIT 1""",
                    (prev + "%",))
        row = cur.fetchone()
    if not row:
        return None
    return {"error_code": row[0], "suggest_path": row[1], "suggest_to": row[2]}


# ---------------------------------------------------------------- safety net

def reconcile(conn, job: str, dag_run_id: str, try_number: int, log=print) -> int:
    """Collect any SparkApplication from an earlier attempt that was never judged.

    When the worker dies the retry policy does not run at that moment. The
    CR that attempt submitted survives with nobody having classified it.

    Returns:
        How many failures were picked up.
    """
    picked = 0
    for t in range(1, try_number):
        app = find_app(app_prefix(job, dag_run_id, t))
        if not app:
            continue
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM failures WHERE app_name = %s", (app,))
            if cur.fetchone():
                continue
        try:
            ev = collector.collect(app)
            if ev["state"] != "FAILED":
                continue
            cls = classifier.classify(ev)
            sug = classifier.suggest(conn, cls["error_code"], ev["config"],
                                     cls["failure_mode"])
            classifier.store(conn, ev, cls, sug)
            log(f"safety net: picked up attempt {t}'s unjudged failure - {app} -> {cls['error_code']}")
            picked += 1
        except SystemExit as exc:
            log(f"safety net: could not collect {app} - {exc}")
    return picked


# ---------------------------------------------------------------- entry point

def judge(job: str, dag_run_id: str, try_number: int, log=print) -> dict:
    """Judge one failed attempt: collect, classify, suggest, store, decide.

    Args:
        job: job name.
        dag_run_id: Airflow DAG run id.
        try_number: attempt number, starting at 1.
        log: where to write progress lines.

    Returns:
        retry (bool), reason, and the error_code, failure_id and applied override.
    """
    prefix = app_prefix(job, dag_run_id, try_number)
    app = find_app(prefix)
    if not app:
        return {"retry": False, "reason": f"no SparkApplication found with prefix {prefix}"}

    ev = collector.collect(app)
    cls = classifier.classify(ev)
    conn = classifier.connect()
    try:
        sug = classifier.suggest(conn, cls["error_code"], ev["config"], cls["failure_mode"])
        fid = classifier.store(conn, ev, cls, sug)
        log(f"judged {app} -> {cls['error_code']}"
            f"{'/' + cls['failure_mode'] if cls.get('failure_mode') else ''}"
            f" (failure_id={fid})")

        prev = previous_attempt(conn, job, dag_run_id, try_number)
        stop = stop_reason(conn, job, try_number, cls, sug, ev["config"], prev)
        applied = None
        if not stop:
            write_override(conn, job, sug["suggest_path"], sug["suggest_to"], fid)
            applied = {sug["suggest_path"]: sug["suggest_to"]}
            log(f"override {sug['suggest_path']} "
                f"{sug['suggest_from']} → {sug['suggest_to']}")
        else:
            log(f"stop: {stop}")

        record_run(conn, app, job, app, fid, None, ev["state"], ev)
        return {"retry": not stop, "reason": stop or "retrying with the adjustment",
                "error_code": cls["error_code"], "failure_id": fid, "applied": applied}
    finally:
        conn.close()


def main() -> int:
    """CLI entry point: remediate.py <job> <dag_run_id> <try_number>."""
    if len(sys.argv) != 4:
        raise SystemExit("usage: remediate.py <job> <dag_run_id> <try_number>")
    r = judge(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    print(json.dumps(r, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
