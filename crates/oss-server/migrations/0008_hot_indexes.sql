-- 热路径索引（2026-10-30 审计）：审计查询/保留清理、上传历史、秒传过期清扫。
-- 全部 IF NOT EXISTS；启动重放幂等。
CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_uid_ts ON audit_log(uid, ts);
CREATE INDEX IF NOT EXISTS idx_uploads_owner_created ON uploads(owner_id, created_at);
CREATE INDEX IF NOT EXISTS idx_instant_index_expires ON instant_index(expires_at);
