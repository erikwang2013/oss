-- 热路径查询索引（2026-11-01 审计）：复制积压版本链、站点目标清理、审计过滤。
-- 全部 IF NOT EXISTS；启动重放幂等。target_kind 由 ensure_columns 补列后本文件才执行。
CREATE INDEX IF NOT EXISTS idx_repl_outbox_version
    ON repl_outbox(bucket, key, version_id, seq);
CREATE INDEX IF NOT EXISTS idx_repl_outbox_site_target
    ON repl_outbox(target_kind, remote_id, state);
CREATE INDEX IF NOT EXISTS idx_audit_log_bucket_ts ON audit_log(bucket, ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_decision_ts ON audit_log(decision, ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_op_ts ON audit_log(op, ts);
