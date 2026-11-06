"""One DAG per job: on failure it judges the cause and re-runs with a changed setting.

When a task fails, the RetryPolicy collects, classifies, suggests and writes one
override; the next attempt reads that value and submits with it.

Three operator defaults are overridden: the default deletes the failed CR, which the
collector still needs; reattaching to a live driver would ignore the judged setting; and
a per-attempt name keeps each attempt's evidence.
"""

from __future__ import annotations

import pathlib
import sys

import pendulum
import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import DAG, RetryDecision, RetryPolicy

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "remediator"))
sys.path.insert(0, str(ROOT / "faults"))

import classifier  # noqa: E402
import remediate  # noqa: E402
import submit  # noqa: E402

NAMESPACE = "spark"


class RemediatedSparkJob(SparkKubernetesOperator):
    """Merge the override just before submitting, with a per-attempt CR name.

    The override has to be read from the database at submit time, so the manifest is
    rendered inside execute() rather than through Jinja.
    """

    def __init__(self, job: str, **kwargs) -> None:
        self.job = job
        super().__init__(application_file="placeholder", **kwargs)

    def execute(self, context):
        """Pick up unjudged attempts, merge the override, then submit."""
        try_number = context["ti"].try_number
        dag_run_id = context["run_id"]
        conn = classifier.connect()
        try:
            # An attempt whose worker died was never judged. Pick it up here.
            if try_number > 1:
                remediate.reconcile(conn, self.job, dag_run_id, try_number,
                                    log=self.log.info)
            override = remediate.read_override(conn, self.job)
        finally:
            conn.close()

        sets = []
        if override:
            sets.append(f"{override['target_path']}={override['value']}")
            self.log.info("applying override %s=%s (failure_id=%s)",
                          override["target_path"], override["value"],
                          override["from_failure_id"])

        self.name = remediate.app_prefix(self.job, dag_run_id, try_number)
        cfg = yaml.safe_load((ROOT / "faults" / "jobs.yaml").read_text())
        spec = submit.resolve(cfg, self.job, sets)
        self.application_file = yaml.safe_dump(
            submit.build_manifest(cfg, self.job, spec, self.name, submit.load_env()),
            sort_keys=False)
        self.log.info("submitting %s", self.name)

        applied = ({override["target_path"]: override["value"]} if override else None)
        try:
            result = super().execute(context)
        except BaseException:
            # On failure the RetryPolicy fills in runs while collecting
            self._record_submitted(applied)
            raise
        self._record_submitted(applied, state="COMPLETED")
        if override:
            # This override worked. It stays until a human folds it into the DAG.
            conn = classifier.connect()
            try:
                remediate.mark_override_succeeded(conn, self.job)
            finally:
                conn.close()
            self.log.info("succeeded with the override; waiting to be folded into the DAG")
        return result

    def _record_submitted(self, applied, state: str | None = None) -> None:
        """Record what this run ran with, keyed by the CR name as run_id."""
        app = remediate.find_app(self.name) or self.name
        conn = classifier.connect()
        try:
            remediate.start_run(conn, app, self.job, applied)
            if state:
                import collector
                remediate.record_run(conn, app, self.job, app, None, applied,
                                     state, collector.collect(app))
        finally:
            conn.close()


class Remediate(RetryPolicy):
    """Runs synchronously in the worker process before the next state is decided."""

    def __init__(self, job: str) -> None:
        self.job = job

    def evaluate(self, exception, try_number, max_tries, context=None):
        """Judge the failure and decide whether Airflow should retry."""
        dag_run_id = (context or {}).get("run_id") or "unknown"
        import logging
        log = logging.getLogger(__name__)
        log.info("judging try_number=%s max_tries=%s", try_number, max_tries)
        r = remediate.judge(self.job, dag_run_id, try_number, log=log.info)
        # Airflow's retries enforces the cap; a policy can only stop earlier.
        return RetryDecision.retry() if r["retry"] else RetryDecision.fail(reason=r["reason"])


def build(job: str) -> DAG:
    """Build the DAG for one job."""
    with DAG(
        dag_id=f"spark_{job}",
        schedule=None,
        start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["spark", "remediation"],
    ) as dag:
        RemediatedSparkJob(
            task_id="run",
            job=job,
            namespace=NAMESPACE,
            kubernetes_conn_id="kubernetes_default",
            delete_on_termination=False,
            reattach_on_restart=False,
            random_name_suffix=True,
            get_logs=False,
            retries=remediate.MAX_TRIES - 1,
            retry_delay=pendulum.duration(seconds=15),
            retry_policy=Remediate(job),
        )
    return dag


for _job in yaml.safe_load((ROOT / "faults" / "jobs.yaml").read_text())["jobs"]:
    globals()[f"dag_{_job}"] = build(_job)
