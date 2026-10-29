-- M8 控制面：全部新表/索引一次性建齐（S0 单写；后续切片只使用、不改本文件）。
-- jobs/lifecycle_rules 的新列走 db.rs::ensure_columns（老库需 ALTER 补列）。

CREATE TABLE IF NOT EXISTS job_tasks (
  job_id INTEGER NOT NULL,
  task_no INTEGER NOT NULL,
  action TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'queued',
  attempt INTEGER NOT NULL DEFAULT 0,
  next_at INTEGER NOT NULL DEFAULT 0,
  dedupe_key TEXT,
  deadline_at INTEGER,
  result_code TEXT,
  result_string TEXT,
  started_at INTEGER,
  finished_at INTEGER,
  PRIMARY KEY (job_id, task_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_job_tasks_dedupe
  ON job_tasks(dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_tasks_claim ON job_tasks(state, next_at);
CREATE INDEX IF NOT EXISTS idx_job_tasks_job_state ON job_tasks(job_id, state);

CREATE TABLE IF NOT EXISTS access_points (
  name TEXT PRIMARY KEY,
  bucket TEXT NOT NULL,
  owner_id INTEGER NOT NULL,
  policy_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_access_points_bucket ON access_points(bucket);

CREATE TABLE IF NOT EXISTS olap_configs (
  access_point TEXT PRIMARY KEY REFERENCES access_points(name) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('http', 'wasm')),
  endpoint TEXT,
  module BLOB,
  module_sha256 TEXT,
  timeout_ms INTEGER NOT NULL DEFAULT 5000,
  max_response_bytes INTEGER NOT NULL DEFAULT 67108864,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_configs (
  bucket TEXT NOT NULL,
  id TEXT NOT NULL,
  owner_id INTEGER NOT NULL,
  config_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, id)
);
CREATE INDEX IF NOT EXISTS idx_inventory_configs_bucket ON inventory_configs(bucket);

CREATE TABLE IF NOT EXISTS inventory_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bucket TEXT NOT NULL,
  config_id TEXT NOT NULL,
  run_key TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  state TEXT NOT NULL,
  job_id INTEGER,
  manifest_key TEXT,
  error TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE (bucket, config_id, run_key)
);
CREATE INDEX IF NOT EXISTS idx_inventory_runs_cfg ON inventory_runs(bucket, config_id);

CREATE TABLE IF NOT EXISTS tier_restores (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  version_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL DEFAULT 0,
  job_id INTEGER,
  state TEXT NOT NULL DEFAULT 'pending',
  created_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, version_id)
);
CREATE INDEX IF NOT EXISTS idx_tier_restores_expiry ON tier_restores(state, expires_at);

-- 4.10：生命周期扫描游标（v1.23.0 及更早为节点本地表；feature 5 起经 Raft 复制）
CREATE TABLE IF NOT EXISTS lifecycle_cursors (
  bucket TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  cursor TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, rule_id, stage)
);

-- 3.6：Inventory 增量扫描水位（本地表；绑定 config_updated_at 防配置变更后误用旧游标）
CREATE TABLE IF NOT EXISTS inventory_cursors (
  bucket TEXT NOT NULL,
  config_id TEXT NOT NULL,
  cursor TEXT NOT NULL,
  config_updated_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, config_id)
);

-- 3.6：Inventory 增量模式开关（默认全量；本地表，配置整行替换时复位）
CREATE TABLE IF NOT EXISTS inventory_incremental (
  bucket TEXT NOT NULL,
  config_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, config_id)
);

-- 4.11：无任务 running 作业回收按 (state, created_at) 局部索引，避免每 tick 全表扫
CREATE INDEX IF NOT EXISTS idx_jobs_running_created ON jobs(created_at) WHERE state = 'running';

-- v3（阻断 1）：生命周期规则 id 桶内唯一（rule_id 列由 db.rs::ensure_columns 补出）
CREATE UNIQUE INDEX IF NOT EXISTS idx_lifecycle_rule_id ON lifecycle_rules(bucket, rule_id)
  WHERE rule_id IS NOT NULL;
