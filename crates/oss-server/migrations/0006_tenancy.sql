-- AC-2：账户用量（软字节配额采样结果；节点本地表，不进 Raft 快照）。
-- 每次启动重放（IF NOT EXISTS）；accounts 由 0005 建表，故须在其后重放。
-- `complete=0` 表示最近一次采样失败/超时：保留上次 used_bytes，只降级可信度。
CREATE TABLE IF NOT EXISTS account_usage (
  account_id   TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  used_bytes   INTEGER NOT NULL DEFAULT 0,
  object_count INTEGER NOT NULL DEFAULT 0,
  complete     INTEGER NOT NULL DEFAULT 0,
  computed_at  INTEGER NOT NULL DEFAULT 0
);
