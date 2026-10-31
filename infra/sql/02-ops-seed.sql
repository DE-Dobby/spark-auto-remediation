-- 사전 데이터. detect_summary 는 classifier 의 판정 조건을, steps 는 Phase 4·6 에서
-- 실제로 통한 값을 그대로 옮긴 것이다.

INSERT INTO error_types (error_code, name_ko, detect_summary, retryable) VALUES
  ('heap',           'JVM heap 부족',
   'OutOfMemoryError: Java heap space (executor exit 52). 셔플 입력이 있는 stage 에서 발생',
   true),
  ('jvm_overhead',   'JVM 외 컨테이너 메모리 초과',
   'executor Pod OOMKilled (exit 137) 이고 실패한 SQL 실행 계획에 Python 연산자가 없음',
   true),
  ('python_mem',     'Python 프로세스 메모리 초과',
   'driver 로그에 PythonException ... MemoryError, 또는 OOMKilled + SQL 계획에 MapInPandas/ArrowEvalPython',
   true),
  ('skew_partition', '파티션 편중',
   '태스크 시간 max/median 편차가 크고 stage spill 이 큼',
   true),
  ('jdbc_partition', 'JDBC 단일 파티션 읽기',
   '실패 stage 가 태스크 1개이고 shuffle read 가 없음 — 소스를 쪼개지 않고 읽은 것',
   true),
  ('driver',         'driver 메모리 부족',
   'driver 로그에 broadcast hash relation 생성 실패 또는 spark.driver.maxResultSize 초과',
   true),
  ('non_resource',   '리소스와 무관한 실패',
   'ANSI 에러 클래스(DIVIDE_BY_ZERO, CAST_INVALID_INPUT 등) 가 있고 메모리 신호가 없음',
   false),
  ('unknown',        '분류 불가',
   '매칭된 신호가 없음',
   false);

-- 레버 하나당 규칙 하나. priority 가 작은 것부터 조건을 본다.
-- steps 의 마지막 값이 곧 상한이다 (노드 용량을 넘기면 Pod 이 영구 Pending 이 된다).
INSERT INTO remediation_rules
  (error_code, priority, condition, target_path, direction, steps, step_default) VALUES
  -- executor 컨테이너 896 / 1024 / 1433 / 1792 / 2150MiB
  ('heap', 1, NULL,
   'executor.memory', 'up', '["512m","640m","1g","1280m","1536m"]', NULL),

  -- driver 컨테이너 1433 / 2150 / 2867MiB (실측: 2816Mi 스케줄됨, 3584Mi Pending)
  ('driver', 1, NULL,
   'driver.memory', 'up', '["1g","1536m","2g"]', NULL),

  -- pyspark.memory 가 명시된 잡은 그 한도(RLIMIT_AS)가 먼저 걸린 것이므로 그걸 올린다
  ('python_mem', 1, 'conf_set:spark.executor.pyspark.memory',
   'conf.spark.executor.pyspark.memory', 'up', '["256m","512m","1g"]', NULL),

  -- 그 외 Python 메모리는 Arrow 배치 크기를 줄인다.
  -- maxRecordsPerBatch 만 내려도 효과가 없다. Spark 4.1 은 바이트 상한이 배치 크기를 결정한다.
  ('python_mem', 2, NULL,
   'conf.spark.sql.execution.arrow.maxBytesPerBatch', 'down',
   '["1g","256m","64m","16m"]', '64m'),

  -- 설정하지 않으면 파티션 1개로 읽는다 (= 단계 0 이 아니라 '1')
  ('jdbc_partition', 1, NULL,
   'env.JDBC_NUM_PARTITIONS', 'up', '["1","4","8","16"]', '1'),

  ('jvm_overhead', 1, NULL,
   'executor.memoryOverhead', 'up', '["384m","1g","2g"]', NULL),

  ('skew_partition', 1, NULL,
   'conf.spark.sql.shuffle.partitions', 'up', '["200","400","800"]', '200');
