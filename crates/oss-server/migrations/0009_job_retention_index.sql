-- 作业保留清理索引（2026-10-31）：`terminal_jobs_before` 的
-- `state IN (...) AND updated_at < ?` 过滤（覆盖索引，免全表扫）。
-- IF NOT EXISTS：启动重放幂等。
CREATE INDEX IF NOT EXISTS idx_jobs_state_updated_at ON jobs(state, updated_at);
