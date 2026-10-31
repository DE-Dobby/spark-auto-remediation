-- 실패 관리 스키마 (Postgres `ops` DB).
-- 운영 장부는 Iceberg 가 아니라 Postgres 에 둔다 — 행 수가 작고, 규칙은 고쳐야 하고,
-- 조회는 단건·소규모 집계뿐이다. Iceberg 는 잡이 적재하는 대상으로만 쓴다.

-- 대표 오류 유형. classifier 가 내는 라벨이 그대로 PK 다.
CREATE TABLE error_types (
  error_code      text    PRIMARY KEY,
  name_ko         text    NOT NULL,
  detect_summary  text    NOT NULL,   -- 사람이 읽는 판정 근거
  retryable       boolean NOT NULL    -- false 면 설정을 고쳐도 의미가 없다
);

-- 오류별 대처. 레버 하나, 이산 단계, 한 번에 한 칸.
CREATE TABLE remediation_rules (
  rule_id       serial  PRIMARY KEY,
  error_code    text    NOT NULL REFERENCES error_types(error_code),
  priority      int     NOT NULL DEFAULT 1,  -- 작은 값부터 조건을 본다
  condition     text,                        -- 'conf_set:<키>' / NULL 이면 기본 규칙
  target_path   text    NOT NULL,            -- executor.memory | conf.<키> | env.<키>
  direction     text    NOT NULL CHECK (direction IN ('up','down')),
  steps         jsonb   NOT NULL,            -- 이산 단계. 마지막 값이 곧 상한
  step_default  text,                        -- 설정하지 않았을 때의 유효값
  UNIQUE (error_code, priority)
);

-- 실패 1건. app_name 이 유일하므로 중복 적재가 막힌다.
CREATE TABLE failures (
  failure_id        bigserial   PRIMARY KEY,
  app_name          text        NOT NULL UNIQUE,
  run_id            text,
  job_name          text,
  failed_at         timestamptz,
  state             text,
  error_code        text        REFERENCES error_types(error_code),
  evidence          text[],                  -- 매칭된 신호
  config            jsonb,                   -- 실패한 실행의 설정 스냅샷
  rule_id           int         REFERENCES remediation_rules(rule_id),
  suggest_path      text,
  suggest_from      text,
  suggest_to        text,
  no_suggest_reason text,
  committed         boolean     NOT NULL DEFAULT false,  -- 커밋 후 실패
  raw               jsonb,                   -- collector 원본. 재분류용
  collected_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX failures_job_time_idx ON failures (job_name, failed_at DESC);
CREATE INDEX failures_error_idx    ON failures (error_code);

-- executor 별 종료 사유·피크. OOMKilled 된 executor 는 피크를 보고하지 못하고 죽으므로
-- 값이 0 으로 남는 것 자체가 신호다. 집계로 누르면 그 구분이 사라진다.
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

-- 실패 stage 별 모양. jdbc_partition 과 heap 을 가르는 근거가 여기 있다.
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

-- 돌린 실행과 결과. parent_failure_id 가 "어떤 실패에 대한 대응인지" 를 잇는다.
-- 이 연결이 없으면 "무슨 조정으로 통했다" 가 데이터에 남지 않는다.
CREATE TABLE runs (
  run_id            text        PRIMARY KEY,
  job_name          text        NOT NULL,
  app_name          text,
  parent_failure_id bigint      REFERENCES failures(failure_id),
  applied_overrides jsonb,                   -- 실제로 넣은 값
  state             text,
  duration_s        numeric(10,1),
  core_hours        numeric(12,6),
  submitted_by      text,
  submitted_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX runs_job_time_idx ON runs (job_name, submitted_at DESC);
