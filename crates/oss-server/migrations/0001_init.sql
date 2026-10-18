-- M1 五张表。对象数据永远不进 SQL —— 这里只有控制面状态。
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0,  -- M1 只写 0/1（创建时定）；不加 CHECK，M4 引角色时再演进
    -- 'active' / 'disabled'：停用即全链路失效（登录 401、S3 的 AK 403、控制台与上传面 401）。
    -- 旧库由 db.rs 的 ensure_columns 探测补齐（ALTER 不幂等，不能放这整份重放里）。
    status        TEXT NOT NULL DEFAULT 'active',
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS access_keys (
    access_key  TEXT PRIMARY KEY,
    secret_key  TEXT NOT NULL,           -- M1 明文存；M5 随 SSE 一起改为加密存
    user_id     INTEGER NOT NULL REFERENCES users(id),
    status      TEXT NOT NULL DEFAULT 'active',  -- 只有 'active' 能通过 xl-s3/src/auth.rs 的 AK→SK 查询
    expires_at  INTEGER,                 -- NULL = 永不过期；M5 的 STS 会话用同一列
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name          TEXT PRIMARY KEY,
    owner_id      INTEGER REFERENCES users(id),
    creation_date INTEGER NOT NULL,
    versioning    TEXT NOT NULL DEFAULT 'Off',  -- M4 用
    object_lock   INTEGER NOT NULL DEFAULT 0,   -- M1 只写 0；M5 用。不加 CHECK
    set_id        INTEGER NOT NULL DEFAULT 0    -- M4 切片 8：桶→set 归属（ensure_columns 给老库补列）
);

CREATE TABLE IF NOT EXISTS uploads (
    saved_path    TEXT PRIMARY KEY,      -- aetherupload 的三段式寻址
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    group_name    TEXT NOT NULL,
    size          INTEGER NOT NULL,
    etag          TEXT NOT NULL,
    original_name TEXT,                  -- 前端 register 时填
    owner_id      INTEGER REFERENCES users(id),  -- 同上
    created_at    INTEGER NOT NULL
);

-- ponytail: 过期行只增不删、expires_at 无索引；升级：set 时顺手清过期行，或加索引 + 周期清扫。
CREATE TABLE IF NOT EXISTS instant_index (
    k          TEXT PRIMARY KEY,
    v          TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);

-- 子账户 × 桶 的三档权限（读/写/删）。求值规则见 crates/xl-s3/src/access.rs：
-- 管理员全允许；桶主（buckets.owner_id）全允许；否则查本表，无行 = 无权限（fail-closed）。
-- 用桶名而非 FK：删桶时由 delete_bucket 顺手清行（防重建同名桶继承旧授权）。
CREATE TABLE IF NOT EXISTS permissions (
    user_id    INTEGER NOT NULL REFERENCES users(id),
    bucket     TEXT NOT NULL,
    can_read   INTEGER NOT NULL DEFAULT 0,
    can_write  INTEGER NOT NULL DEFAULT 0,
    can_delete INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, bucket)
);

-- M4 切片 6：生命周期规则与作业记录（0001 整份重放，增量表对新老库都幂等）
CREATE TABLE IF NOT EXISTS lifecycle_rules (
    bucket      TEXT NOT NULL,
    prefix      TEXT NOT NULL DEFAULT '',   -- 对象键前缀（'' = 全桶）
    expire_days INTEGER NOT NULL,           -- 0 = 立即到期（测试/激进清理）；负值拒绝（应用层校验）
    enabled     INTEGER NOT NULL DEFAULT 1,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (bucket, prefix)
);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,              -- 目前仅 'lifecycle'
    bucket      TEXT NOT NULL DEFAULT '',
    prefix      TEXT NOT NULL DEFAULT '',
    state       TEXT NOT NULL DEFAULT 'running', -- running | done | error
    scanned     INTEGER NOT NULL DEFAULT 0,
    deleted     INTEGER NOT NULL DEFAULT 0,
    skipped     INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- M4 切片 7：桶策略与审计
CREATE TABLE IF NOT EXISTS bucket_policies (
    bucket      TEXT PRIMARY KEY,
    policy_json TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         INTEGER NOT NULL,
    uid        INTEGER NOT NULL,
    op         TEXT NOT NULL,            -- S3 op 名（如 GetObject）
    bucket     TEXT,
    object_key TEXT,
    decision   TEXT NOT NULL,            -- allow | deny
    detail     TEXT
);

CREATE TABLE IF NOT EXISTS event_configs (
    bucket TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS event_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL,
    target_id TEXT NOT NULL,
    event_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',   -- pending | sending | dead（M6 R2 两态租约）
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_at INTEGER,
    next_retry_at INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_outbox_claim ON event_outbox (state, next_retry_at);

-- M6 切片 4：桶复制规则与出站积压（R2 300s 租约；is_delete_marker 为计划外必要列——切片 3 定稿记档）
CREATE TABLE IF NOT EXISTS replication_rules (
    bucket TEXT PRIMARY KEY,
    rules_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS repl_outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    etag TEXT NOT NULL,
    size INTEGER NOT NULL,
    remote_id TEXT NOT NULL,
    dest_bucket TEXT NOT NULL,
    is_delete_marker INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',   -- pending | sending | dead
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_at INTEGER,
    next_retry_at INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_repl_outbox_claim ON repl_outbox (state, next_retry_at);
