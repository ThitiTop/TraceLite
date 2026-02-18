CREATE DATABASE IF NOT EXISTS trace_lite;

CREATE TABLE IF NOT EXISTS trace_lite.raw_logs (
  ts               DateTime64(3, 'UTC'),
  ingest_ts        DateTime64(3, 'UTC') DEFAULT now64(3),
  service          LowCardinality(String),
  env              LowCardinality(String),
  host             LowCardinality(String),
  version          LowCardinality(String),
  level            LowCardinality(String),
  message          String,
  trace_id         String,
  span_id          String,
  parent_span_id   String,
  event            LowCardinality(String),
  route            String,
  method           LowCardinality(String),
  status_code      UInt16,
  duration_ms      UInt32,
  attrs            Map(String, String),
  raw_json         String,
  INDEX idx_trace trace_id TYPE bloom_filter GRANULARITY 2,
  INDEX idx_span span_id TYPE bloom_filter GRANULARITY 2
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (env, service, ts, trace_id, span_id, host)
TTL toDateTime(ts) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS trace_lite.spans (
  trace_id          String,
  span_id           String,
  parent_span_id    String,
  service           LowCardinality(String),
  env               LowCardinality(String),
  host              LowCardinality(String),
  version           LowCardinality(String),
  operation         String,
  start_ts          DateTime64(3, 'UTC'),
  end_ts            DateTime64(3, 'UTC'),
  duration_ms       UInt32,
  self_time_ms      UInt32,
  status_code       UInt16,
  is_error          UInt8,
  source            LowCardinality(String),
  updated_at        DateTime64(3, 'UTC') DEFAULT now64(3),
  INDEX idx_span_t trace_id TYPE bloom_filter GRANULARITY 2
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toDate(start_ts)
ORDER BY (env, service, start_ts, trace_id, span_id)
TTL toDateTime(start_ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS trace_lite.traces (
  trace_id            String,
  env                 LowCardinality(String),
  root_service        LowCardinality(String),
  start_ts            DateTime64(3, 'UTC'),
  end_ts              DateTime64(3, 'UTC'),
  duration_ms         UInt32,
  span_count          UInt16,
  service_count       UInt16,
  error_count         UInt16,
  critical_path_ms    UInt32,
  versions            Array(LowCardinality(String)),
  updated_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toDate(start_ts)
ORDER BY (env, start_ts, trace_id)
TTL toDateTime(start_ts) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS trace_lite.dependency_edges_minute (
  bucket_ts         DateTime('UTC'),
  env               LowCardinality(String),
  caller_service    LowCardinality(String),
  callee_service    LowCardinality(String),
  caller_version    LowCardinality(String),
  callee_version    LowCardinality(String),
  calls             UInt64,
  error_calls       UInt64,
  p50_ms            Float32,
  p95_ms            Float32,
  max_ms            UInt32
)
ENGINE = MergeTree
PARTITION BY toDate(bucket_ts)
ORDER BY (env, bucket_ts, caller_service, callee_service, caller_version, callee_version)
TTL bucket_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS trace_lite.host_stats_minute (
  bucket_ts          DateTime('UTC'),
  env                LowCardinality(String),
  host               LowCardinality(String),
  logs               UInt64,
  errors             UInt64,
  distinct_services  UInt32,
  last_seen_ts       DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(bucket_ts)
ORDER BY (env, bucket_ts, host)
TTL bucket_ts + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS trace_lite.mv_host_stats_minute
TO trace_lite.host_stats_minute
AS
SELECT
  toStartOfMinute(ts) AS bucket_ts,
  env,
  host,
  count() AS logs,
  countIf(level = 'ERROR' OR status_code >= 500) AS errors,
  uniqExact(service) AS distinct_services,
  max(ts) AS last_seen_ts
FROM trace_lite.raw_logs
GROUP BY bucket_ts, env, host;
