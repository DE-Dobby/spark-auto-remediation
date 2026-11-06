-- Failure ledger schema (Postgres `ops` database).
-- Kept in Postgres rather than Iceberg: the row count is small, the rules have to be
-- edited in place, and the queries are single lookups and small aggregates.

-- Error types. The label the classifier emits is the primary key.
CREATE TABLE error_types (
  error_code      text    PRIMARY KEY,
  name            text    NOT NULL,
  detect_summary  text    NOT NULL,   -- how it is detected, for a human to read
  retryable       boolean NOT NULL    -- false means no setting can fix it
);

-- Remediation per error type: one lever, discrete steps, one step at a time.
CREATE TABLE remediation_rules (
  rule_id       serial  PRIMARY KEY,
  error_code    text    NOT NULL REFERENCES error_types(error_code),
  priority      int     NOT NULL DEFAULT 1,  -- conditions are evaluated lowest first
  condition     text,                        -- 'conf_set:<key>'; NULL is the default rule
  target_path   text    NOT NULL,            -- executor.memory | conf.<key> | env.<key>
  direction     text    NOT NULL CHECK (direction IN ('up','down')),
  steps         jsonb   NOT NULL,            -- discrete steps; the last one is the ceiling
  step_default  text,                        -- effective value when the knob is unset
  UNIQUE (error_code, priority)
);

-- One row per failure. app_name is unique, so storing twice is impossible.
CREATE TABLE failures (
  failure_id        bigserial   PRIMARY KEY,
  app_name          text        NOT NULL UNIQUE,
  run_id            text,
  job_name          text,
  failed_at         timestamptz,
  state             text,
  error_code        text        REFERENCES error_types(error_code),
  -- Mode within one error_code, for aggregation. Not used to pick a lever, except
  -- indirectly through a rule condition 'mode:<value>'.
  failure_mode      text,
  evidence          text[],                  -- the signals that matched
  -- The error content itself. This is what a human reads when the classification is
  -- wrong or unknown.
  error_class       text,                    -- Spark error class or exception class
  error_text        text,                    -- that exception's message
  config            jsonb,                   -- config snapshot of the failed run
  rule_id           int         REFERENCES remediation_rules(rule_id),
  suggest_path      text,
  suggest_from      text,
  suggest_to        text,
  no_suggest_reason text,
  raw               jsonb,                   -- raw collector output, for re-classification
  collected_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX failures_job_time_idx ON failures (job_name, failed_at DESC);
CREATE INDEX failures_error_idx    ON failures (error_code);

-- Per-executor exit reason and peaks. An OOMKilled executor dies before reporting
-- peaks, so a missing value is itself a signal; aggregating these would hide it.
CREATE TABLE failure_executors (
  failure_id      bigint NOT NULL REFERENCES failures(failure_id) ON DELETE CASCADE,
  executor_id     text   NOT NULL,
  pod_name        text,
  exit_code       int,
  reason          text,                      -- OOMKilled / Error / Completed
  jvm_heap_peak   bigint,
  jvm_rss_peak    bigint,
  python_rss_peak bigint,
  PRIMARY KEY (failure_id, executor_id)
);

-- Shape of each failed stage. This is what separates jdbc_partition from heap.
CREATE TABLE failure_stages (
  failure_id     bigint NOT NULL REFERENCES failures(failure_id) ON DELETE CASCADE,
  stage_id       int    NOT NULL,
  attempt_id     int    NOT NULL,
  num_tasks      int,
  shuffle_read   bigint,
  spill          bigint,
  task_max_ms    bigint,
  task_median_ms bigint,
  failure_reason text,
  PRIMARY KEY (failure_id, stage_id, attempt_id)
);

-- Runs and their outcomes. parent_failure_id links a run to the failure it responds
-- to; without it, which adjustment actually worked is not in the data.
CREATE TABLE runs (
  run_id            text        PRIMARY KEY,
  job_name          text        NOT NULL,
  app_name          text,
  parent_failure_id bigint      REFERENCES failures(failure_id),
  applied_overrides jsonb,                   -- the values this run actually ran with
  state             text,
  duration_s        numeric(10,1),
  core_hours        numeric(12,6),
  submitted_by      text,
  submitted_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX runs_job_time_idx ON runs (job_name, submitted_at DESC);

-- The one value to override on that job's next attempt. One row per job, normally
-- empty. The DAG owns the configuration; this table only records "this failure made us
-- change this one value". A filled succeeded_at means that value worked and
-- is waiting to be folded into the DAG.
CREATE TABLE job_overrides (
  job_name        text        PRIMARY KEY,
  target_path     text        NOT NULL,      -- executor.memory | conf.<key> | env.<key>
  value           text        NOT NULL,
  from_failure_id bigint      REFERENCES failures(failure_id),
  created_at      timestamptz NOT NULL DEFAULT now(),
  succeeded_at    timestamptz
);
