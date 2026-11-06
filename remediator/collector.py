"""Collect failure signals for a SparkApplication from outside the job.

Four sources:
    SparkApplication CR   final state, errorMessage, config snapshot
    executor/driver pods  terminated.reason / exitCode (137 vs 52)
    driver log            exceptions and their messages
    History Server REST   failed stage shape, executor peaks, failed SQL plan

Usage: collector.py <app-name>
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.error
import urllib.request

CONTEXT = "k3d-spark-ar"
NAMESPACE = "spark"
HISTORY = "http://localhost:18080/api/v1"
LOG_TAIL = 4000          # max driver log lines to read
EXCERPT_MAX = 40000      # max stored log excerpt length

# Spark error classes are logged as [DIVIDE_BY_ZERO]. Matched by pattern, not a list.
ERROR_CLASS_RE = re.compile(r"\[([A-Z][A-Z0-9_.]{3,})\]")
JSON_CLASS_RE = re.compile(r'"errorClass":\s*"([^"]+)"')
# Every JVM failure is logged in this form
EXC_RE = re.compile(r"((?:[a-z][\w]*\.)+[A-Z]\w*(?:Exception|Error|Throwable)): ([^\n\\\"]{1,400})")
# A PythonException message stops at 'Traceback ...'. The real exception is the last
# traceback line, `<module.path.>Name: message` (e.g. numpy ..._ArrayMemoryError).
PY_EXC_RE = re.compile(r"^\s{0,4}((?:[\w.]+\.)?_?\w*(?:Error|Exception))(?::\s*(.*))?$")
# Logged as consequences, not causes
ERROR_CLASS_IGNORE = ("INTERNAL_ERROR",)
GENERIC_MSGS = ("cancelled because SparkContext was shut down",
                "cancelled as part of cancellation of all jobs",
                "Job aborted due to stage failure")
NON_RESOURCE_EXC = ("AnalysisException", "ParseException", "ArithmeticException",
                    "NumberFormatException", "FileNotFoundException")
# Their presence in the plan means a Python process held the memory
PYTHON_OPS = ("MapInPandas", "ArrowEvalPython", "BatchEvalPython", "FlatMapGroupsInPandas",
              "AggregateInPandas", "MapInArrow", "WindowInPandas")


def kubectl(*args: str) -> str:
    """Run kubectl against this project's context and return stdout."""
    r = subprocess.run(["kubectl", "--context", CONTEXT, *args], capture_output=True, text=True)
    return r.stdout


def kubectl_json(*args: str) -> dict:
    """Run kubectl with -o json and return the parsed object, or {} if empty."""
    out = kubectl(*args, "-o", "json")
    return json.loads(out) if out.strip() else {}


def history(path: str):
    """GET a Spark History Server REST path, or None if it is unreachable."""
    try:
        with urllib.request.urlopen(f"{HISTORY}/{path}", timeout=10) as r:
            return json.load(r)
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return None


def pod_signal(pod: dict) -> dict:
    """Reduce a pod object to its role, exit code and termination reason.

    exitCode is what separates 137 (kernel OOM kill) from 52 (JVM OutOfMemoryError).
    """
    cs = (pod.get("status", {}).get("containerStatuses") or [{}])[0]
    term = cs.get("state", {}).get("terminated") or cs.get("lastState", {}).get("terminated") or {}
    labels = pod.get("metadata", {}).get("labels", {})
    return {
        "pod_name": pod["metadata"]["name"],
        "executor_id": labels.get("spark-exec-id") or labels.get("spark-role", "driver"),
        "role": labels.get("spark-role", ""),
        "exit_code": term.get("exitCode"),
        "reason": term.get("reason"),
    }


def python_exception(lines: list[str]) -> dict:
    """Return the real Python exception from the last line of a traceback.

    A PythonException message is truncated at 'Traceback (most recent call last):',
    leaving only the wrapper name, so the traceback body has to be read.

    Args:
        lines: driver log lines.

    Returns:
        {"class": ..., "message": ...}, or {} if no traceback was found.
    """
    out: dict = {}
    in_tb = False
    for line in lines:
        if "Traceback (most recent call last)" in line:
            in_tb, out = True, {}
            continue
        if not in_tb:
            continue
        stripped = line.strip()
        # The body is 'File "..."' lines and their source lines. Skip them.
        if stripped.startswith('File "') or not stripped:
            continue
        m = PY_EXC_RE.match(line)
        if m:
            # The first traceback is the cause; a later py4j one is its consequence.
            return {"class": m.group(1), "message": (m.group(2) or "").strip()[:300]}
        if line.startswith(("\tat ", "        at ", "26/", "25/")):
            in_tb = False
    return out


def log_signals(driver_pod: str) -> dict:
    """Extract the error content from a driver log.

    JVM failures are matched with EXC_RE, Spark 4 query errors come from the JSON
    log's errorClass. Every flag in the result is derived from that content.

    Args:
        driver_pod: name of the driver pod to read logs from.

    Returns:
        Exception list, error class and text, and the derived boolean signals.
    """
    log = kubectl("-n", NAMESPACE, "logs", driver_pod, f"--tail={LOG_TAIL}")
    lines = log.splitlines()

    # 1) exception classes and messages
    seen, exceptions = set(), []
    for cls, msg in EXC_RE.findall(log):
        key = (cls, msg[:80])
        if key in seen:
            continue
        seen.add(key)
        exceptions.append({"class": cls, "message": msg.strip()[:300]})
        if len(exceptions) >= 10:
            break

    # 2) error classes (JSON errorClass first, then [UPPER_SNAKE])
    classes = [c for c in JSON_CLASS_RE.findall(log) + ERROR_CLASS_RE.findall(log)
               if not c.startswith(ERROR_CLASS_IGNORE)]
    error_classes = list(dict.fromkeys(classes))

    def in_msg(*needles: str) -> bool:
        return any(n in e["message"] for n in needles for e in exceptions)

    def in_class(*needles: str) -> bool:
        return any(n in e["class"] for n in needles for e in exceptions)

    # The first exception may be a consequence ('SparkContext was shut down' is
    # logged before the executor OOM that caused it).
    primary = next((e for e in exceptions
                    if not any(g in e["message"] for g in GENERIC_MSGS)),
                   exceptions[0] if exceptions else {})
    py_exc = python_exception(lines)
    return {
        "exceptions": exceptions,
        "error_classes": error_classes,
        # The PythonException wrapper carries no content; use the traceback's exception.
        "error_class": (error_classes[0] if error_classes
                        else py_exc.get("class") if py_exc and primary.get("class", "")
                        .endswith("PythonException")
                        else primary.get("class")),
        "error_text": (py_exc.get("message") if py_exc and primary.get("class", "")
                       .endswith("PythonException")
                       else primary.get("message")),
        # --- derived from the content above ---
        "python_exc": py_exc,
        # An exception name ending in MemoryError means Python ran out of memory
        # (numpy raises _ArrayMemoryError, pyarrow MemoryError; both are subclasses).
        "python_memory_error": py_exc.get("class", "").endswith("MemoryError"),
        "jvm_oom": in_class("OutOfMemoryError"),
        # Off-heap, not heap. The exit code is 52 either way, so only content tells them apart.
        "offheap_alloc": in_class("OutOfDirectMemoryError",
                                  "arrow.memory.OutOfMemoryException")
        or in_msg("Failure allocating buffer", "of direct memory"),
        "gc_overhead": in_msg("GC overhead limit exceeded"),
        "broadcast_build": in_msg("Not enough memory to build and broadcast",
                                  "Could not execute broadcast", "Cannot broadcast"),
        "max_result_size": in_msg("maxResultSize"),
        "no_space": in_msg("No space left on device"),
        "s3_pool": in_msg("Timeout waiting for connection from pool"),
        "kryo_buffer": in_msg("Kryo serialization failed"),
        "fetch_failed": in_class("FetchFailed") or in_msg("FetchFailed"),
        "heartbeat_timeout": in_msg("heartbeat timed out"),
        "broadcast_timeout": bool(re.search(r"Could not execute broadcast in \d+ secs", log)),
        # Kernel and JVM exits leave no exception; only the driver's notice line.
        "exit137": "exit code 137" in log,
        "exit52": "exit code 52" in log,
        # For the non_resource branch: an error class with no memory signal
        "non_resource_class": error_classes[0] if error_classes else None,
        # A bare JVM exception name needs a whitelist: calling an unrecognised one
        # "not a resource problem" would drop it from the queue a human should review.
        "non_resource_exception": next(
            (e for e in NON_RESOURCE_EXC if primary.get("class", "").endswith(e)), None),
        "first_error": (f"{primary.get('class')}: {primary.get('message')}"
                        if primary else "")[:500],
        # Kept so stored failures can be re-classified after detection changes
        "excerpt": "\n".join(l[:1000] for l in lines
                             if "Exception" in l or "Error" in l or "WARN" in l)[:EXCERPT_MAX],
    }


def history_app_name(app_id: str) -> str | None:
    """Return the name the job declared for itself (SparkSession.appName).

    A job submitted by someone else carries no job-name label, so the name is read
    from the event log rather than guessed.

    Args:
        app_id: Spark application id.

    Returns:
        The declared app name, or None if the History Server has no record.
    """
    app = history(f"applications/{app_id}") or {}
    return app.get("name") or None


def stable_job_name(candidate: str | None, app_name: str) -> str | None:
    """Reject a candidate job name that changes from run to run.

    job_name groups the runs of one job. A per-run unique string would split every
    failure into its own group, which is worse than NULL: NULL is visibly missing,
    a unique string breaks aggregation silently.

    Args:
        candidate: name read from a label or the event log.
        app_name: this run's application name.

    Returns:
        The candidate if it looks stable, otherwise None.
    """
    if not candidate:
        return None
    if candidate == app_name:
        return None
    # Names carrying a date, time or hash (spark-etl-20261102-174523, run-a19a2d26af)
    if re.search(r"\d{6,}", candidate) or re.search(r"[0-9a-f]{8,}", candidate.lower()):
        return None
    return candidate


def history_signals(app_id: str) -> dict:
    """Read failed stage shapes, executor peaks and the failed SQL plan.

    Args:
        app_id: Spark application id.

    Returns:
        failed_stages, executor_peaks, sql_ops and whether the plan has Python operators.
    """
    stages, peaks, sql_ops = [], [], []
    for s in (history(f"applications/{app_id}/stages?status=FAILED") or []):
        # Task times are absent from the stage list, so each stage needs a detail
        # call. They are what the skew check reads.
        detail = history(f"applications/{app_id}/stages/{s.get('stageId')}/"
                         f"{s.get('attemptId')}?details=true") or {}
        raw = detail.get("tasks") or {}
        tasks = sorted((t.get("duration") or 0) for t in raw.values()) if isinstance(raw, dict) else []
        stages.append({
            "stage_id": s.get("stageId"),
            "attempt_id": s.get("attemptId"),
            "num_tasks": s.get("numTasks"),
            "shuffle_read": s.get("shuffleReadBytes"),
            "spill": (s.get("memoryBytesSpilled") or 0) + (s.get("diskBytesSpilled") or 0),
            "task_max_ms": tasks[-1] if tasks else None,
            "task_median_ms": tasks[len(tasks) // 2] if tasks else None,
            "failure_reason": (s.get("failureReason") or "")[:500] or None,
        })
    for e in (history(f"applications/{app_id}/executors") or []):
        pm = e.get("peakMemoryMetrics") or {}
        peaks.append({
            "executor_id": e.get("id"),
            # An OOMKilled executor dies before reporting peaks, so these stay None
            "jvm_heap_peak": pm.get("JVMHeapMemory"),
            "jvm_rss_peak": pm.get("ProcessTreeJVMRSS"),
            "python_rss_peak": pm.get("ProcessTreePythonRSS"),
        })
    for x in (history(f"applications/{app_id}/sql") or []):
        if x.get("status") == "FAILED":
            sql_ops += [n.get("nodeName", "") for n in (x.get("nodes") or [])]
    return {"failed_stages": stages, "executor_peaks": peaks, "sql_ops": sql_ops,
            "sql_has_python": any(any(p in op for p in PYTHON_OPS) for op in sql_ops)}


def _elapsed(start: str | None, end: str | None) -> float | None:
    """Return elapsed seconds between two CR timestamps, or None if either is missing."""
    if not start or not end:
        return None
    import datetime as _dt
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    try:
        a = _dt.datetime.strptime(start, fmt)
        b = _dt.datetime.strptime(end, fmt)
    except ValueError:
        return None
    return round((b - a).total_seconds(), 1)


def collect(app_name: str) -> dict:
    """Collect every signal for one failed SparkApplication.

    Args:
        app_name: SparkApplication name.

    Returns:
        Identity, state, config snapshot, executor and driver pod signals, log signals
        and History Server signals.

    Raises:
        SystemExit: if the SparkApplication does not exist.
    """
    cr = kubectl_json("-n", NAMESPACE, "get", "sparkapplication", app_name)
    if not cr:
        raise SystemExit(f"SparkApplication '{app_name}' not found")

    status = cr.get("status", {})
    spec = cr.get("spec", {})
    conf = spec.get("sparkConf", {}) or {}
    app_id = status.get("sparkApplicationId", "")
    driver_pod = status.get("driverInfo", {}).get("podName") or f"{app_name}-driver"
    env = {e["name"]: e.get("value") for e in ((spec.get("driver") or {}).get("env") or [])
           if e.get("value") is not None}

    # Selected by the operator's own label, not our run-id convention, so this works
    # whoever submitted the job.
    pods = [pod_signal(p) for p in kubectl_json(
        "-n", NAMESPACE, "get", "pods",
        "-l", f"sparkoperator.k8s.io/app-name={app_name}").get("items", [])]
    executors = [p for p in pods if p["role"] == "executor"]

    hist = history_signals(app_id) if app_id else {
        "failed_stages": [], "executor_peaks": [], "sql_ops": [], "sql_has_python": False}
    # Merge peaks into the executor list; History Server and pod ids match
    peaks = {p["executor_id"]: p for p in hist["executor_peaks"]}
    for e in executors:
        e.update({k: v for k, v in (peaks.get(e["executor_id"]) or {}).items()
                  if k != "executor_id"})

    # Our own label first, then the name the job declared. NULL if neither is usable.
    labels = cr["metadata"].get("labels", {})
    job_name = labels.get("job-name") or stable_job_name(
        history_app_name(app_id) if app_id else None, app_name)
    return {
        "app_name": app_name,
        "job_name": job_name,
        # run_id equals the app name by convention; for external jobs it is the only id.
        "run_id": labels.get("run-id") or app_name,
        "spark_app_id": app_id,
        "state": status.get("applicationState", {}).get("state"),
        "error_message": status.get("applicationState", {}).get("errorMessage"),
        "failed_at": status.get("terminationTime"),
        "started_at": status.get("lastSubmissionAttemptTime"),
        "duration_s": _elapsed(status.get("lastSubmissionAttemptTime"),
                               status.get("terminationTime")),
        "config": {
            "driver_memory": (spec.get("driver") or {}).get("memory"),
            "driver_cores": (spec.get("driver") or {}).get("cores"),
            "executor_memory": (spec.get("executor") or {}).get("memory"),
            "executor_cores": (spec.get("executor") or {}).get("cores"),
            "executor_instances": (spec.get("executor") or {}).get("instances"),
            "executor_overhead": (spec.get("executor") or {}).get("memoryOverhead"),
            "conf": conf,
            "env": env,
        },
        "executors": executors,
        "driver": next((p for p in pods if p["role"] == "driver"), None),
        "oom_killed": [e for e in executors if e["reason"] == "OOMKilled" or e["exit_code"] == 137],
        "jvm_oom_exit": [e for e in executors if e["exit_code"] == 52],
        "log": log_signals(driver_pod),
        "history": hist,
    }


def main() -> int:
    """CLI entry point: print the collected signals for one application."""
    if len(sys.argv) != 2:
        raise SystemExit("usage: collector.py <app-name>")
    ev = collect(sys.argv[1])
    print(json.dumps(ev, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
