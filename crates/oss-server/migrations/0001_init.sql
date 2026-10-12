-- M1 五张表。对象数据永远不进 SQL —— 这里只有控制面状态。
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0,  -- M1 只写 0/1（创建时定）；不加 CHECK，M4 引角色时再演进
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
    object_lock   INTEGER NOT NULL DEFAULT 0    -- M1 只写 0；M5 用。不加 CHECK
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
