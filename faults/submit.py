"""Submit one failure-reproduction job by hand. A test tool, not the operating path.

Airflow does the submitting (see dags/). This is for running a single job quickly,
without a DAG, to check its classification. It is also the only reader of
faults/jobs.yaml.

Usage:
  .venv/bin/python faults/submit.py udf_oom_killed
  .venv/bin/python faults/submit.py udf_oom_killed \
      --set conf.spark.sql.execution.arrow.maxBytesPerBatch=64m
"""
from __future__ import annotations

import argparse
import copy
import json
import pathlib
import subprocess
import sys
import time

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONTEXT = "k3d-spark-ar"
NAMESPACE = "spark"
JARS_HOSTPATH = "/var/lib/rancher/k3s/storage/jars"
SPARK_VERSION = "4.1.4"


def kubectl(*args: str, check: bool = True, stdin: str | None = None) -> str:
    """Run kubectl against this project's context and return stdout."""
    r = subprocess.run(["kubectl", "--context", CONTEXT, *args],
                       input=stdin, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"kubectl {' '.join(args)} failed:\n{r.stderr.strip()}")
    return r.stdout


def load_env() -> dict[str, str]:
    """Read .env into a dict."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        raise SystemExit(".env is missing. cp .env.example .env")
    env = {}
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def deep_merge(base: dict, over: dict) -> dict:
    """Merge over into base recursively, returning a new dict."""
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) \
            else copy.deepcopy(v)
    return out


def apply_set(spec: dict, assignments: list[str]) -> dict:
    """--set executor.memory=1g / conf.spark.x=1 / env.FOO=bar"""
    spec = copy.deepcopy(spec)
    for a in assignments:
        if "=" not in a:
            raise SystemExit(f"--set must be path=value, got: {a}")
        path, value = a.split("=", 1)
        if path.startswith("conf."):
            spec.setdefault("conf", {})[path[len("conf."):]] = value
        elif path.startswith("env."):
            spec.setdefault("env", {})[path[len("env."):]] = value
        else:
            section, key = path.split(".", 1)
            # The CRD rejects a string where it expects an integer
            spec.setdefault(section, {})[key] = int(value) if key in ("cores", "instances") else value
        print(f"  --set {path}={value}")
    return spec


def resolve(cfg: dict, job: str, assignments: list[str]) -> dict:
    """Build a job's spec from defaults, its own settings and any --set overrides."""
    if job not in cfg["jobs"]:
        raise SystemExit(f"'{job}' is not in faults/jobs.yaml. Available: {', '.join(cfg['jobs'])}")
    spec = deep_merge(cfg["defaults"], cfg["jobs"][job])
    return apply_set(spec, assignments) if assignments else spec


def build_manifest(cfg: dict, job: str, spec: dict, name: str, env: dict) -> dict:
    """Render a SparkApplication manifest for one submission."""
    conf = {**cfg["common_conf"], **(spec.get("conf") or {})}
    conf = {k: str(v) for k, v in conf.items()}

    pod_env = [
        {"name": "AWS_ACCESS_KEY_ID",
         "valueFrom": {"secretKeyRef": {"name": "minio-credentials", "key": "MINIO_ROOT_USER"}}},
        {"name": "AWS_SECRET_ACCESS_KEY",
         "valueFrom": {"secretKeyRef": {"name": "minio-credentials", "key": "MINIO_ROOT_PASSWORD"}}},
        {"name": "AWS_REGION", "value": "us-east-1"},
        {"name": "PG_USER",
         "valueFrom": {"secretKeyRef": {"name": "postgres-credentials", "key": "POSTGRES_USER"}}},
        {"name": "PG_PASSWORD",
         "valueFrom": {"secretKeyRef": {"name": "postgres-credentials", "key": "POSTGRES_PASSWORD"}}},
        # run_id equals the app name so pods and logs share one key
    ]
    for k, v in (spec.get("env") or {}).items():
        pod_env.append({"name": k, "value": str(v)})
    mounts = [
        {"name": "extra-jars", "mountPath": "/opt/extra-jars", "readOnly": True},
        {"name": "jobs", "mountPath": "/opt/jobs", "readOnly": True},
    ]

    def side(kind: str) -> dict:
        s = spec[kind]
        out = {"cores": s["cores"], "memory": s["memory"], "env": pod_env,
               "volumeMounts": mounts, "labels": {"job-name": job, "run-id": name}}
        if kind == "driver":
            out["serviceAccount"] = "spark"
        else:
            out["instances"] = s["instances"]
        if s.get("memoryOverhead"):
            out["memoryOverhead"] = s["memoryOverhead"]
        return out

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": NAMESPACE,
                     "labels": {"job-name": job, "run-id": name}},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": cfg["image"],
            "imagePullPolicy": "Never",
            "mainApplicationFile": f"local:///opt/jobs/{spec['main']}",
            "sparkVersion": SPARK_VERSION,
            # The operator must not retry: a failure has to stay failed to be collected
            "restartPolicy": {"type": "Never"},
            "sparkConf": conf,
            "volumes": [
                {"name": "extra-jars", "hostPath": {"path": JARS_HOSTPATH, "type": "Directory"}},
                {"name": "jobs", "configMap": {"name": "spark-jobs"}},
            ],
            "driver": side("driver"),
            "executor": side("executor"),
        },
    }


def sync_jobs_configmap() -> None:
    """Recreate the spark-jobs ConfigMap from faults/jobs/."""
    rendered = kubectl("-n", NAMESPACE, "create", "configmap", "spark-jobs",
                       f"--from-file={ROOT / 'faults' / 'jobs'}", "--dry-run=client", "-o", "yaml")
    kubectl("apply", "-f", "-", stdin=rendered)


def wait(name: str, timeout_s: int) -> tuple[str, dict]:
    """Poll a SparkApplication until it finishes, returning its state and status."""
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        out = kubectl("-n", NAMESPACE, "get", "sparkapplication", name, "-o", "json", check=False)
        status = json.loads(out).get("status", {}) if out.strip() else {}
        state = status.get("applicationState", {}).get("state", "PENDING")
        if state != last:
            print(f"  state={state}", flush=True)
            last = state
        if state in ("COMPLETED", "FAILED", "FAILING", "SUBMISSION_FAILED", "INVALIDATING"):
            return state, status
        time.sleep(5)
    return "TIMEOUT", {}


def main() -> int:
    """CLI entry point: render, submit and wait for one job."""
    ap = argparse.ArgumentParser()
    ap.add_argument("job")
    ap.add_argument("--set", dest="sets", action="append", default=[],
                    metavar="PATH=VALUE", help="override a setting (executor.memory / conf.<key> / env.<key>)")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    cfg = yaml.safe_load((ROOT / "faults" / "jobs.yaml").read_text())
    env = load_env()

    # Each submission needs a new app name. Reusing one collides with the retained
    # driver pod, so the operator cannot create it and the app stalls in SUBMITTED.
    name = f"{args.job.replace('_', '-')}-{time.strftime('%m%d-%H%M%S')}"
    spec = resolve(cfg, args.job, args.sets)
    manifest = build_manifest(cfg, args.job, spec, name, env)

    dr, ex = spec["driver"], spec["executor"]
    print(f"[{args.job}] {name}")
    print(f"  driver   {dr['cores']}core / {dr['memory']}"
          + (f" / overhead {dr['memoryOverhead']}" if dr.get("memoryOverhead") else ""))
    print(f"  executor {ex['instances']} × {ex['cores']}core / {ex['memory']}"
          + (f" / overhead {ex['memoryOverhead']}" if ex.get("memoryOverhead") else ""))
    for k, v in (spec.get("conf") or {}).items():
        print(f"  conf     {k}={v}")

    sync_jobs_configmap()
    kubectl("apply", "-f", "-", stdin=yaml.safe_dump(manifest))

    t0 = time.time()
    state, _ = wait(name, args.timeout)
    print(f"[{args.job}] state={state}  {time.time() - t0:.0f}s")

    # With --set the expectation in jobs.yaml no longer applies
    expect = None if args.sets else (spec.get("expect") or {}).get("state")
    if expect:
        print(f"[{args.job}] expected={expect} -> {'match' if state == expect else 'mismatch'}")
    if state == "TIMEOUT":
        print(f"[{args.job}] TIMEOUT - to free the node, "
              f"kubectl -n {NAMESPACE} delete sparkapplication {name}")
    return 0 if (not expect or state == expect) else 1


if __name__ == "__main__":
    sys.exit(main())
