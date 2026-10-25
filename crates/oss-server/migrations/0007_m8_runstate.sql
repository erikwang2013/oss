-- M8 多节点控制面：调度租约与运行态序列（level-3）。
-- 0006 已被 tenancy（Wave 0 Track C）占用，故本文件顺延为 0007。
-- 运行态七表由 0001/0003 建出；这里只补随 `m8.runstate` 扩展复制的两张表。
-- 不进 11 表 `ControlData`，不加 raft guard 触发器（level-3 前仍由本地 worker 直写）。

-- 单行租约：term/node/expires_at 由 Raft apply 校验，expiry 不是 renewal。
CREATE TABLE IF NOT EXISTS schedule_lease (
  singleton   INTEGER PRIMARY KEY CHECK (singleton = 1),
  term        INTEGER NOT NULL DEFAULT 0,
  node        INTEGER NOT NULL DEFAULT 0,
  expires_at  INTEGER NOT NULL DEFAULT 0,
  acquired_at INTEGER NOT NULL DEFAULT 0
);

-- 运行态序列：`next_job_id` / `next_run_id`（`RunStateInit` 播种；缺行 = 未初始化）。
CREATE TABLE IF NOT EXISTS runstate_meta (
  key   TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
