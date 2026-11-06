"""Turn collected signals into an error_code and failure_mode, attach a remediation
suggestion, and store the result in Postgres.

error_code is the lever unit, failure_mode is for aggregation.
Classification lives in code; remediation rules live in Postgres.

Usage:
    classifier.py <app-name> [--no-store]
    classifier.py --all                    # every FAILED application
    classifier.py --reclassify [--apply]   # re-classify stored raw, print differences
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import collector  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
DSN_HOST, DSN_PORT, DSN_DB = "localhost", 15432, "ops"
SKEW_RATIO = 5          # task max / median at or above this counts as skew


def connect():
    """Open a connection to the ops database using credentials from .env."""
    env = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return psycopg2.connect(host=DSN_HOST, port=DSN_PORT, dbname=DSN_DB,
                            user=env["POSTGRES_USER"], password=env["POSTGRES_PASSWORD"])


_UNIT = {"k": 2**10, "m": 2**20, "g": 2**30, "t": 2**40, "s": 1, "ms": 0.001}


def _num(v) -> float | None:
    """Parse a Spark config value into a number, or None if it is not comparable.

    Handles '512m', '120s', '200' and '-1'.
    """
    if v is None:
        return None
    t = str(v).strip().lower()
    try:
        return float(t)
    except ValueError:
        pass
    for suffix, mult in sorted(_UNIT.items(), key=lambda kv: -len(kv[0])):
        if t.endswith(suffix):
            try:
                return float(t[: -len(suffix)]) * mult
            except ValueError:
                return None
    return None


def _beyond(steps: list[str], cur, direction: str) -> str | None:
    """Pick the first step past the current value, in the rule's direction.

    Used when the current value is not one of the configured steps. Falls back to the
    first step when the values cannot be compared.
    """
    c = _num(cur)
    if c is None:
        return steps[0]
    for st in steps:
        n = _num(st)
        if n is None:
            continue
        if (direction == "up" and n > c) or (direction == "down" and n < c):
            return st
    return None


def _plan_has(ops: list[str], needle: str) -> bool:
    """Return True if any plan operator contains the needle."""
    return any(needle in op for op in ops)


def _heap_mode(stages: list[dict], ops: list[str], log: dict) -> str:
    """Return the failure_mode within error_code 'heap'.

    The lever is the same for every mode here; this is for aggregation only.
    """
    if log.get("gc_overhead"):
        return "gc_overhead"
    spill = max((s["spill"] or 0) for s in stages) if stages else 0
    if _plan_has(ops, "BroadcastExchange") and spill == 0:
        return "broadcast_exchange"
    if spill > 0:
        for s in stages:
            mx, md = s.get("task_max_ms") or 0, s.get("task_median_ms") or 0
            if md and mx >= SKEW_RATIO * md:
                return "skew_hot_key"
        return "shuffle_spill"
    return "generic_heap"


def classify(ev: dict) -> dict:
    """Classify collected signals. The first matching branch wins.

    Args:
        ev: the dict returned by collector.collect().

    Returns:
        error_code, failure_mode, evidence lines, error class and text.
    """
    log, hist, cfg = ev["log"], ev["history"], ev["config"]
    stages, ops = hist["failed_stages"], hist["sql_ops"]
    oom_killed = bool(ev["oom_killed"])
    exec_jvm_oom = bool(ev["jvm_oom_exit"]) or log["exit52"]
    jvm_oom = exec_jvm_oom or log["jvm_oom"]
    # The error content is the first evidence line, so it survives a wrong branch.
    why: list[str] = []
    if log.get("error_class"):
        why.append(f"{log['error_class']}: {(log.get('error_text') or '')[:200]}")

    def out(code: str, mode: str | None) -> dict:
        return {"error_code": code, "failure_mode": mode, "evidence": why,
                "error_class": log.get("error_class"), "error_text": log.get("error_text")}

    # 1) RLIMIT_AS hit first. The process survives and only raises.
    if log["python_memory_error"]:
        why.append("MemoryError in the driver log (RLIMIT_AS)")
        if cfg["conf"].get("spark.executor.pyspark.memory"):
            why.append(f"pyspark.memory={cfg['conf']['spark.executor.pyspark.memory']}")
        return out("python_mem", "rlimit_memoryerror")

    # 2) The kernel killed the container (137), so the memory was outside the JVM heap.
    #    An OOMKilled executor reports no peaks, so the plan decides Python or not.
    if oom_killed:
        why.append(f"{len(ev['oom_killed'])} executor pods OOMKilled (exit 137)")
        if hist["sql_has_python"]:
            why.append("Python operator in the failed SQL plan")
            return out("python_mem", "container_sigkill")
        why.append("no Python operator in the failed SQL plan")
        if cfg.get("executor_overhead"):
            why.append(f"memoryOverhead={cfg['executor_overhead']}")
        return out("jvm_overhead", "container_sigkill")

    # 3) Driver-side failure: the lever is driver.memory, not executor.memory.
    if log["broadcast_build"]:
        why.append("driver log: broadcast build failed")
        why.append(f"driver memory={cfg.get('driver_memory')}")
        return out("driver", "broadcast_build")
    if log["max_result_size"]:
        why.append("driver log: spark.driver.maxResultSize exceeded")
        return out("driver", "max_result_size")

    # 4) An OOM with no executor exit 52 means the driver's heap.
    if jvm_oom and not exec_jvm_oom:
        why.append("OutOfMemoryError with no executor exit 52: the driver heap")
        why.append(f"driver memory={cfg.get('driver_memory')}")
        return out("driver", "driver_heap")

    # 5) Executor JVM OOM (52). Exit 52 covers every OutOfMemoryError
    #   , so heap and off-heap are told apart by content.
    if exec_jvm_oom:
        why.append("executor exit 52 (JVM OutOfMemoryError)")
        if log.get("offheap_alloc"):
            why.append("off-heap allocation failed, not the heap (Arrow/netty direct memory)")
            # With a batch cap above the heap, raising memory just fails on the next batch.
            if hist["sql_has_python"]:
                why.append("Python operator in the failed SQL plan")
                why.append("arrow.maxBytesPerBatch="
                           f"{cfg['conf'].get('spark.sql.execution.arrow.maxBytesPerBatch')}"
                           f" · executor memory={cfg.get('executor_memory')}")
                return out("python_mem", "arrow_alloc")
            why.append("no Python operator: no verified off-heap lever")
            return out("heap", "offheap_alloc")
        for s in stages:
            if s["num_tasks"] == 1 and not s["shuffle_read"]:
                why.append(f"failed stage {s['stage_id']}.{s['attempt_id']} has one task "
                           f"and no shuffle read")
                # An unsplittable file looks the same, so confirm JDBC from the plan.
                jdbc = next((op for op in ops if "JDBCRelation" in op), None)
                if jdbc:
                    why.append(f"plan: {jdbc}")
                    return out("jdbc_partition", "jdbc_unpartitioned")
                why.append("no JDBC scan in the plan: no verified lever")
                return out("unknown", "unsplit_source")
        if stages:
            s = max(stages, key=lambda x: x["shuffle_read"] or 0)
            why.append(f"failed stage {s['stage_id']} shuffle read "
                       f"{(s['shuffle_read'] or 0) / 2**20:.0f}MB · spill "
                       f"{(s['spill'] or 0) / 2**20:.0f}MB")
        why.append(f"executor memory={cfg.get('executor_memory')}")
        return out("heap", _heap_mode(stages, ops, log))

    # 6) Non-memory failures. FetchFailed and heartbeat timeouts are also logged as
    #    consequences of an OOM, so this must stay after the memory branches.
    for flag, code, mode in (
        ("kryo_buffer", "serializer_buffer", "kryo_buffer_overflow"),
        ("broadcast_timeout", "broadcast_timeout", "broadcast_wait"),
        ("no_space", "disk_space", "local_disk_full"),
        ("s3_pool", "s3_throttle", "connection_pool_exhausted"),
        ("fetch_failed", "shuffle_fetch", "fetch_failed"),
        ("heartbeat_timeout", "executor_lost", "heartbeat_timeout"),
    ):
        if log.get(flag):
            why.append(f"driver log signal: {flag}")
            return out(code, mode)

    # 7) No resource signal but an error class or known exception: config cannot fix it.
    #    Every exception is extracted, so this must stay last or it catches broadcast OOM.
    if log.get("non_resource_class") or log.get("non_resource_exception"):
        mode = log.get("non_resource_class") or log.get("non_resource_exception")
        why.append("no resource signal: not OOMKilled, exit 52, MemoryError, disk or broadcast")
        return out("non_resource", mode)

    # 8) The driver failed with no executors at all.
    if (ev.get("driver") or {}).get("exit_code") not in (0, None) and not ev["executors"]:
        why.append("the driver failed with no executors")
        return out("driver", "driver_only")

    # No branch matched. Keep the error content: this row is a human's work queue.
    why.append("no signal matched any rule")
    for e in (log.get("exceptions") or [])[:5]:
        why.append(f"exception {e['class']}: {e['message'][:160]}")
    return out("unknown", None)


def current_value(path: str, cfg: dict):
    """Return the failed run's current value for a rule's target_path.

    Args:
        path: 'executor.memory', 'conf.<key>' or 'env.<key>'.
        cfg: the config snapshot from the SparkApplication CR.
    """
    if path.startswith("conf."):
        return cfg["conf"].get(path[len("conf."):])
    if path.startswith("env."):
        return cfg["env"].get(path[len("env."):])
    section, key = path.split(".", 1)
    return cfg.get(f"{section}_{'overhead' if key == 'memoryOverhead' else key}")


def condition_ok(cond: str | None, cfg: dict, mode: str | None) -> bool:
    """Evaluate a rule condition. Only four forms are supported.

    conf_set:<key> / env_set:<key> / unset:<path> / mode:<failure_mode>

    conf_set and env_set check whether the submitted spec declares that knob.
    Any other form is treated as not matching.
    """
    if not cond:
        return True
    kind, _, val = cond.partition(":")
    if kind == "conf_set":
        return val in cfg["conf"]
    if kind == "env_set":
        return val in cfg["env"]
    if kind == "unset":
        return current_value(val, cfg) is None
    if kind == "mode":
        return mode == val
    return False        # an unrecognised form never matches


def suggest(conn, error_code: str, cfg: dict, mode: str | None = None) -> dict:
    """Pick one lever from remediation_rules and move it one step.

    Args:
        conn: Postgres connection to the ops database.
        error_code: the classification to look rules up by.
        cfg: the failed run's config snapshot.
        mode: failure_mode, for rules whose condition is 'mode:<value>'.

    Returns:
        The chosen rule and the from/to values, or no_suggest_reason explaining why not.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT retryable FROM error_types WHERE error_code = %s", (error_code,))
        row = cur.fetchone()
        if row and not row["retryable"]:
            return {"no_suggest_reason": f"{error_code} cannot be fixed by configuration (retryable=false)"}
        cur.execute("""SELECT rule_id, condition, target_path, direction, steps, step_default
                       FROM remediation_rules WHERE error_code = %s ORDER BY priority""",
                    (error_code,))
        rules = cur.fetchall()
    if not rules:
        return {"no_suggest_reason": f"no remediation rule is registered for {error_code}"}

    for r in rules:
        if not condition_ok(r["condition"], cfg, mode):
            continue
        steps = [str(s) for s in r["steps"]]
        cur_val = current_value(r["target_path"], cfg)
        cur_val = str(cur_val) if cur_val is not None else r["step_default"]
        if cur_val in steps:
            i = steps.index(cur_val)
            if i + 1 >= len(steps):
                return {"no_suggest_reason": f"{r['target_path']} has reached its ceiling of {cur_val}"}
            nxt = steps[i + 1]
        else:
            nxt = _beyond(steps, cur_val, r["direction"])
            if nxt is None:
                return {"no_suggest_reason":
                        f"{r['target_path']} current value {cur_val} is outside the configured steps"}
            cur_val = cur_val or "unset"
        return {"rule_id": r["rule_id"], "suggest_path": r["target_path"],
                "suggest_from": cur_val, "suggest_to": nxt, "direction": r["direction"]}
    # Unmet conditions mean the job never declared that knob: its code needs the fix.
    need = ", ".join(r["condition"] for r in rules if r["condition"]) or "(none)"
    return {"no_suggest_reason":
            f"{error_code} rule conditions unmet (needs: {need})"}


def store(conn, ev: dict, cls: dict, sug: dict) -> int | None:
    """Store a failure and its child rows.

    Args:
        conn: Postgres connection to the ops database.
        ev: collector output.
        cls: classify() output.
        sug: suggest() output.

    Returns:
        The new failure_id, or None if this app_name was already stored.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO failures (app_name, run_id, job_name, failed_at, state, error_code,
                                  failure_mode, error_class, error_text, evidence, config,
                                  rule_id, suggest_path, suggest_from, suggest_to,
                                  no_suggest_reason, raw)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (app_name) DO NOTHING RETURNING failure_id""",
            (ev["app_name"], ev["run_id"], ev["job_name"], ev["failed_at"], ev["state"],
             cls["error_code"], cls["failure_mode"], cls.get("error_class"),
             cls.get("error_text"), cls["evidence"],
             json.dumps(ev["config"], ensure_ascii=False),
             sug.get("rule_id"), sug.get("suggest_path"), sug.get("suggest_from"),
             sug.get("suggest_to"), sug.get("no_suggest_reason"),
             json.dumps(ev, ensure_ascii=False)))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None
        fid = row[0]
        cur.executemany("""INSERT INTO failure_executors
            (failure_id, executor_id, pod_name, exit_code, reason,
             jvm_heap_peak, jvm_rss_peak, python_rss_peak)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            [(fid, e["executor_id"], e["pod_name"], e["exit_code"], e["reason"],
              e.get("jvm_heap_peak"), e.get("jvm_rss_peak"), e.get("python_rss_peak"))
             for e in ev["executors"]])
        cur.executemany("""INSERT INTO failure_stages
            (failure_id, stage_id, attempt_id, num_tasks, shuffle_read, spill,
             task_max_ms, task_median_ms, failure_reason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            [(fid, s["stage_id"], s["attempt_id"], s["num_tasks"], s["shuffle_read"],
              s["spill"], s["task_max_ms"], s["task_median_ms"], s["failure_reason"])
             for s in ev["history"]["failed_stages"]])
    conn.commit()
    return fid


def _print_result(tag: str, cls: dict, sug: dict) -> None:
    """Print one classification and its suggestion."""
    mode = f" / {cls['failure_mode']}" if cls.get("failure_mode") else ""
    print(f"{tag} → {cls['error_code']}{mode}")
    if cls.get("error_class"):
        print(f"  error {cls['error_class']}: {(cls.get('error_text') or '')[:120]}")
    for line in cls["evidence"]:
        print(f"  - {line}")
    if "suggest_path" in sug:
        arrow = "↑" if sug["direction"] == "up" else "↓"
        print(f"  suggest {sug['suggest_path']}  {sug['suggest_from']} -> {sug['suggest_to']} {arrow}")
    else:
        print(f"  no suggestion: {sug['no_suggest_reason']}")


def handle(app_name: str, store_row: bool = True) -> dict:
    """Collect, classify, suggest and optionally store one failure.

    Args:
        app_name: SparkApplication name.
        store_row: write the result to the failures table when True.
    """
    ev = collector.collect(app_name)
    cls = classify(ev)
    conn = connect()
    try:
        sug = suggest(conn, cls["error_code"], ev["config"], cls["failure_mode"])
        _print_result(f"[{app_name}] {ev['state']}", cls, sug)
        if store_row:
            fid = store(conn, ev, cls, sug)
            print(f"  {'stored failure_id=' + str(fid) if fid else 'already stored'}")
    finally:
        conn.close()
    return {**cls, **sug}


def reclassify(apply: bool) -> int:
    """Re-classify every stored failure from its raw signals and print the differences.

    This is the regression check for changes to the classification branches. Pods may be
    gone by then, so the stored raw is the only way to replay a past failure.

    Args:
        apply: write the new classification back when True.
    """
    conn = connect()
    changed = 0
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""SELECT failure_id, app_name, error_code, failure_mode,
                                  suggest_to, evidence, no_suggest_reason, raw
                           FROM failures ORDER BY failure_id""")
            rows = cur.fetchall()
        for r in rows:
            ev = r["raw"]
            cls = classify(ev)
            sug = suggest(conn, cls["error_code"], ev["config"], cls["failure_mode"])
            # Evidence and the no-suggestion reason are compared too: they are what a
            # human reads, so a change in their wording has to reach the stored rows.
            same = (cls["error_code"] == r["error_code"]
                    and cls["failure_mode"] == r["failure_mode"]
                    and sug.get("suggest_to") == r["suggest_to"]
                    and cls["evidence"] == list(r["evidence"] or [])
                    and sug.get("no_suggest_reason") == r["no_suggest_reason"])
            if same:
                continue
            changed += 1
            print(f"[{r['failure_id']}] {r['app_name']}")
            print(f"  before {r['error_code']} / {r['failure_mode']} -> {r['suggest_to']}")
            print(f"  now    {cls['error_code']} / {cls['failure_mode']} -> {sug.get('suggest_to')}")
            if apply:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE failures SET error_code=%s, failure_mode=%s,
                                   error_class=%s, error_text=%s,
                                   evidence=%s, rule_id=%s, suggest_path=%s, suggest_from=%s,
                                   suggest_to=%s, no_suggest_reason=%s WHERE failure_id=%s""",
                                (cls["error_code"], cls["failure_mode"],
                                 cls.get("error_class"), cls.get("error_text"), cls["evidence"],
                                 sug.get("rule_id"), sug.get("suggest_path"),
                                 sug.get("suggest_from"), sug.get("suggest_to"),
                                 sug.get("no_suggest_reason"), r["failure_id"]))
                conn.commit()
        print(f"\n{changed} of {len(rows)} changed{' (applied)' if apply and changed else ''}")
    finally:
        conn.close()
    return 0


def failed_apps() -> list[str]:
    """Return the names of every SparkApplication currently in state FAILED."""
    data = collector.kubectl_json("-n", collector.NAMESPACE, "get", "sparkapplications")
    return [i["metadata"]["name"] for i in data.get("items", [])
            if (i.get("status", {}).get("applicationState", {}) or {}).get("state") == "FAILED"]


def main() -> int:
    """CLI entry point. See the module docstring for the usage forms."""
    ap = argparse.ArgumentParser()
    ap.add_argument("app", nargs="?")
    ap.add_argument("--all", action="store_true", help="every application in state FAILED")
    ap.add_argument("--no-store", action="store_true", help="do not store the result")
    ap.add_argument("--reclassify", action="store_true", help="re-classify stored raw (regression check)")
    ap.add_argument("--apply", action="store_true", help="write the --reclassify result back")
    args = ap.parse_args()

    if args.reclassify:
        return reclassify(args.apply)
    apps = failed_apps() if args.all else ([args.app] if args.app else [])
    if not apps:
        raise SystemExit("usage: classifier.py <app-name> | --all | --reclassify [--apply]")
    for a in apps:
        try:
            handle(a, store_row=not args.no_store)
        except SystemExit as exc:
            print(f"[{a}] skipped: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
