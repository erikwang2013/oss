-- AC-0：多租户账户实体（MVP）。账户 ID 仍是 12 位十进制，既有
-- `arn:aws:iam::<12位>:root` 主体的兼容性由启动期回填保证：每个存量用户 →
-- 其 uid 派生账户，role='owner'（账户 = 用户的旧语义在回填后继续成立）。
-- 本表**不进 Raft 快照/控制命令**：账户写接口只在非 Raft 模式开放（见 db.rs / accounts_api.rs）。
-- 回填只跑一次（schema_meta.accounts_backfill_v1）：之后被显式迁移到其它账户的用户
-- 不会再被「拉回」派生账户；无成员行的新用户继续按派生账户（legacy fallback）解析。

CREATE TABLE IF NOT EXISTS accounts (
    id           TEXT PRIMARY KEY,              -- 12 位十进制；LEGACY_ACCOUNT = 系统账户
    name         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active',-- active | disabled
    admin_uid    INTEGER REFERENCES users(id) ON DELETE SET NULL,
    bucket_quota INTEGER,                       -- NULL = 不限；>=0 为硬上限（建桶时 fail-closed）
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

-- 一个用户最多属于一个账户（UNIQUE(uid)）；role: owner（回填的派生账户）/ admin / member。
CREATE TABLE IF NOT EXISTS account_members (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    uid        INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    role       TEXT NOT NULL DEFAULT 'member',
    created_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, uid)
);
CREATE INDEX IF NOT EXISTS idx_account_members_account ON account_members(account_id);
