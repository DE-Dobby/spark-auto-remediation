"""config/jobs.yaml 로 SparkApplication 을 렌더링해 제출하고 끝날 때까지 기다린다.

재제출은 하지 않는다. 실패 원인 파악은 수집·분류가, 재작업은 사람이 --set 으로 한다.

사용:
  .venv/bin/python remediator/submit.py udf_oom_killed
  .venv/bin/python remediator/submit.py udf_oom_killed \
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
    r = subprocess.run(["kubectl", "--context", CONTEXT, *args],
                       input=stdin, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"kubectl {' '.join(args)} 실패:\n{r.stderr.strip()}")
    return r.stdout


def load_env() -> dict[str, str]:
    env_file = ROOT / ".env"
    if not env_file.exists():
        raise SystemExit(".env 가 없다. cp .env.example .env")
    env = {}
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def deep_merge(base: dict, over: dict) -> dict:
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
            raise SystemExit(f"--set 형식이 아니다: {a} (path=value)")
        path, value = a.split("=", 1)
        if path.startswith("conf."):
            spec.setdefault("conf", {})[path[len("conf."):]] = value
        else:
            section, key = path.split(".", 1)
            spec.setdefault(section, {})[key] = value
        print(f"  --set {path}={value}")
    return spec


def resolve(cfg: dict, job: str, assignments: list[str]) -> dict:
    if job not in cfg["jobs"]:
        raise SystemExit(f"'{job}' 가 config/jobs.yaml 에 없다. 있는 잡: {', '.join(cfg['jobs'])}")
    spec = deep_merge(cfg["defaults"], cfg["jobs"][job])
    return apply_set(spec, assignments) if assignments else spec


def build_manifest(cfg: dict, job: str, spec: dict, name: str, env: dict) -> dict:
    conf = {**cfg["common_conf"], **(spec.get("conf") or {})}
    conf["spark.sql.catalog.lake.jdbc.user"] = env["POSTGRES_USER"]
    conf["spark.sql.catalog.lake.jdbc.password"] = env["POSTGRES_PASSWORD"]
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
        # run_id 는 앱 이름과 같게 둔다 — Pod·로그·Iceberg 스냅샷을 한 키로 잇는다
        {"name": "RUN_ID", "value": name},
        {"name": "JOB_NAME", "value": job},
    ]
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
            # 재시도는 operator 가 하지 않는다. 실패는 실패로 남겨야 수집·분류가 된다.
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
    rendered = kubectl("-n", NAMESPACE, "create", "configmap", "spark-jobs",
                       f"--from-file={ROOT / 'jobs'}", "--dry-run=client", "-o", "yaml")
    kubectl("apply", "-f", "-", stdin=rendered)


def wait(name: str, timeout_s: int) -> tuple[str, dict]:
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
    ap = argparse.ArgumentParser()
    ap.add_argument("job")
    ap.add_argument("--set", dest="sets", action="append", default=[],
                    metavar="PATH=VALUE", help="설정 덮어쓰기 (executor.memory / conf.<키>)")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    cfg = yaml.safe_load((ROOT / "config" / "jobs.yaml").read_text())
    env = load_env()

    # 앱 이름은 제출마다 달라야 한다. 같은 이름을 재사용하면 보존해 둔 driver Pod 과
    # 부딛쳐 operator 가 Pod 을 만들지 못하고 SUBMITTED 에서 멈춘다.
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

    # --set 으로 설정을 바꿨으면 jobs.yaml 의 기대 상태는 기준이 아니다
    expect = None if args.sets else (spec.get("expect") or {}).get("state")
    if expect:
        print(f"[{args.job}] 기대={expect} → {'일치' if state == expect else '불일치'}")
    if state == "TIMEOUT":
        print(f"[{args.job}] TIMEOUT — 노드를 비우려면 "
              f"kubectl -n {NAMESPACE} delete sparkapplication {name}")
    return 0 if (not expect or state == expect) else 1


if __name__ == "__main__":
    sys.exit(main())
