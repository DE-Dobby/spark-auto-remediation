-- Seed data. detect_summary mirrors the classifier's branch conditions; steps are the
-- values that actually recovered the reproduction jobs.

INSERT INTO error_types (error_code, name, detect_summary, retryable) VALUES
  ('heap',           'JVM heap exhausted',
   'OutOfMemoryError: Java heap space (executor exit 52), in a stage with shuffle input',
   true),
  ('jvm_overhead',   'container memory outside the JVM exhausted',
   'executor pod OOMKilled (exit 137) with no Python operator in the failed SQL plan',
   true),
  ('python_mem',     'Python process memory exhausted',
   'PythonException ... MemoryError in the driver log, or OOMKilled with MapInPandas / ArrowEvalPython in the plan',
   true),
  ('skew_partition', 'skewed partition',
   'large task max/median spread with heavy stage spill',
   true),
  ('jdbc_partition', 'unpartitioned JDBC read',
   'failed stage has one task and no shuffle read: the source was read without splitting',
   true),
  ('driver',         'driver memory exhausted',
   'driver log shows a failed broadcast hash relation build, or spark.driver.maxResultSize exceeded',
   true),
  ('non_resource',   'failure unrelated to resources',
   'an ANSI error class (DIVIDE_BY_ZERO, CAST_INVALID_INPUT, ...) with no memory signal',
   false),
  ('unknown',        'not classified',
   'no signal matched',
   false)
ON CONFLICT (error_code) DO NOTHING;

-- One rule per lever. Conditions are evaluated from the lowest priority up.
-- The last value in steps is the ceiling; past the node's capacity a pod would stay
-- Pending forever.
INSERT INTO remediation_rules
  (error_code, priority, condition, target_path, direction, steps, step_default) VALUES
  -- executor container totals: 896 / 1024 / 1433 / 1792 / 2150MiB
  ('heap', 1, NULL,
   'executor.memory', 'up', '["512m","640m","1g","1280m","1536m"]', NULL),

  -- driver container totals: 1433 / 2150 / 2867MiB. 3584Mi exceeds the node and stays Pending.
  ('driver', 1, NULL,
   'driver.memory', 'up', '["1g","1536m","2g"]', NULL),

  -- mode arrow_alloc is the JVM off-heap running out, so raising pyspark.memory (a
  -- limit on the Python process) does nothing. The batch has to shrink.
  ('python_mem', 0, 'mode:arrow_alloc',
   'conf.spark.sql.execution.arrow.maxBytesPerBatch', 'down',
   '["1g","256m","64m","16m"]', '64m'),

  -- When a job declares pyspark.memory, its RLIMIT_AS bound first, so raise that
  ('python_mem', 1, 'conf_set:spark.executor.pyspark.memory',
   'conf.spark.executor.pyspark.memory', 'up', '["256m","512m","1g"]', NULL),

  -- Otherwise shrink the Arrow batch. Lowering maxRecordsPerBatch alone has no effect:
  -- in Spark 4.1 the byte cap decides the batch size.
  ('python_mem', 2, NULL,
   'conf.spark.sql.execution.arrow.maxBytesPerBatch', 'down',
   '["1g","256m","64m","16m"]', '64m'),

  -- Splitting a JDBC read is a DataFrameReader option inside the job's code, not a
  -- Spark setting, so this lever only applies when the job declares it reads
  -- JDBC_NUM_PARTITIONS. Setting it on a job that does not would do nothing.
  ('jdbc_partition', 1, 'env_set:JDBC_NUM_PARTITIONS',
   'env.JDBC_NUM_PARTITIONS', 'up', '["1","4","8","16"]', '1'),

  ('jvm_overhead', 1, NULL,
   'executor.memoryOverhead', 'up', '["384m","1g","2g"]', NULL),

  ('skew_partition', 1, NULL,
   'conf.spark.sql.shuffle.partitions', 'up', '["200","400","800"]', '200')
ON CONFLICT (error_code, priority) DO NOTHING;


-- ------------------------------------------------------------------
-- Failures commonly seen in practice. The defaults below are from the official docs.
--   Spark configuration.html      : network.timeout 120s, shuffle.io.maxRetries 3,
--                                   kryoserializer.buffer.max 64m
--   Spark sql-performance-tuning  : autoBroadcastJoinThreshold 10485760(10MB),
--                                   broadcastTimeout 300, shuffle.partitions 200
--   Hadoop 3.4.1 hadoop-aws       : fs.s3a.connection.maximum 500
-- ------------------------------------------------------------------
INSERT INTO error_types (error_code, name, detect_summary, retryable) VALUES
  ('serializer_buffer', 'Kryo serialization buffer exceeded',
   'Kryo serialization failed: Buffer overflow in the driver log', true),
  ('broadcast_timeout', 'broadcast wait timed out',
   'Could not execute broadcast in N secs in the driver log', true),
  ('disk_space',        'local disk full',
   'No space left on device in the driver log: shuffle and spill filled the local disk', true),
  ('s3_throttle',       'S3 connection pool exhausted',
   'Timeout waiting for connection from pool in the driver log', true),
  ('shuffle_fetch',     'shuffle fetch failed',
   'FetchFailedException, and only when there is no memory signal', true),
  ('executor_lost',     'executor stopped responding',
   'Executor heartbeat timed out: a GC pause or network delay', true)
ON CONFLICT (error_code) DO NOTHING;

INSERT INTO remediation_rules
  (error_code, priority, condition, target_path, direction, steps, step_default) VALUES
  -- The Kryo buffer must stay within int range (2GB), so this stops at 512m
  ('serializer_buffer', 1, NULL,
   'conf.spark.kryoserializer.buffer.max', 'up', '["64m","128m","256m","512m"]', '64m'),

  -- A broadcast that does not finish in time usually means the table is too large, so
  -- disabling the broadcast comes before waiting longer (-1 disables it).
  ('broadcast_timeout', 1, NULL,
   'conf.spark.sql.autoBroadcastJoinThreshold', 'down',
   '["10485760","2097152","-1"]', '10485760'),
  ('broadcast_timeout', 2, NULL,
   'conf.spark.sql.broadcastTimeout', 'up', '["300","600","1200"]', '300'),

  -- Smaller shuffle and spill per task lowers local disk usage
  ('disk_space', 1, NULL,
   'conf.spark.sql.shuffle.partitions', 'up', '["200","400","800"]', '200'),

  ('s3_throttle', 1, NULL,
   'conf.spark.hadoop.fs.s3a.connection.maximum', 'up', '["500","1000","2000"]', '500'),

  ('shuffle_fetch', 1, NULL,
   'conf.spark.shuffle.io.maxRetries', 'up', '["3","6","10"]', '3'),

  ('executor_lost', 1, NULL,
   'conf.spark.network.timeout', 'up', '["120s","300s","600s"]', '120s')
ON CONFLICT (error_code, priority) DO NOTHING;
