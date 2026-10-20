-- M9 S0：控制面 schema 契约（表/守卫；无 Raft 运行时）。
-- 本文件每次启动重放：表 IF NOT EXISTS、触发器 DROP IF EXISTS 后 CREATE、ctx INSERT OR IGNORE。
-- 列迁移（buckets.replication_mode/deleting、sites.raft_node_id）不在这里：
-- ALTER 不幂等，走 db.rs ensure_columns / ensure_columns_m9。

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- 已应用 LogId（完整 JSON，不拆 index/term）+ 成员表（快照/重启恢复同源）。
CREATE TABLE IF NOT EXISTS raft_applied (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    applied_log_id  TEXT,
    membership_json TEXT
);

-- apply 事务内为 1：守卫据此放行；apply 外一律 ABORT。
CREATE TABLE IF NOT EXISTS raft_apply_ctx (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    active INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO raft_apply_ctx (id, active) VALUES (1, 0);

-- sync 写意图：staging 完成后、远端确认前落盘（恢复/补偿用）。
CREATE TABLE IF NOT EXISTS sync_write_intents (
    id                TEXT PRIMARY KEY,
    bucket            TEXT NOT NULL,
    key               TEXT NOT NULL,
    local_version_id  TEXT NOT NULL,
    remote_version_id TEXT NOT NULL,
    tee_path          TEXT NOT NULL,
    commit_json       TEXT NOT NULL,
    state             TEXT NOT NULL DEFAULT 'pending',
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

-- sync 删意图：op = delete / delete_version / marker；版本列允许 NULL，不做空串 UNIQUE。
CREATE TABLE IF NOT EXISTS sync_delete_intents (
    id                TEXT PRIMARY KEY,
    bucket            TEXT NOT NULL,
    key               TEXT NOT NULL,
    op                TEXT NOT NULL,
    local_version_id  TEXT,
    remote_version_id TEXT,
    marker_version_id TEXT,
    state             TEXT NOT NULL DEFAULT 'pending',
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

-- ——— guard：11 张复制表 × INSERT/UPDATE/DELETE ———
-- 放行条件：raft_apply_ctx.active=1；否则仅下列本地写例外，其余 ABORT。
-- 例外：users 管理员行；access_keys 的 session AK 或管理员属主；permissions 的管理员目标。

DROP TRIGGER IF EXISTS raft_guard_users_ins;
CREATE TRIGGER raft_guard_users_ins
BEFORE INSERT ON users
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND NEW.is_admin <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_users_upd;
CREATE TRIGGER raft_guard_users_upd
BEFORE UPDATE ON users
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND OLD.is_admin <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_users_del;
CREATE TRIGGER raft_guard_users_del
BEFORE DELETE ON users
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND OLD.is_admin <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_keys_ins;
CREATE TRIGGER raft_guard_access_keys_ins
BEFORE INSERT ON access_keys
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND NEW.kind <> 'session'
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = NEW.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_keys_upd;
CREATE TRIGGER raft_guard_access_keys_upd
BEFORE UPDATE ON access_keys
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND OLD.kind <> 'session'
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = OLD.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_keys_del;
CREATE TRIGGER raft_guard_access_keys_del
BEFORE DELETE ON access_keys
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND OLD.kind <> 'session'
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = OLD.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_permissions_ins;
CREATE TRIGGER raft_guard_permissions_ins
BEFORE INSERT ON permissions
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = NEW.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_permissions_upd;
CREATE TRIGGER raft_guard_permissions_upd
BEFORE UPDATE ON permissions
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = OLD.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_permissions_del;
CREATE TRIGGER raft_guard_permissions_del
BEFORE DELETE ON permissions
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
     AND NOT EXISTS (SELECT 1 FROM users WHERE id = OLD.user_id AND is_admin = 1)
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_buckets_ins;
CREATE TRIGGER raft_guard_buckets_ins
BEFORE INSERT ON buckets
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_buckets_upd;
CREATE TRIGGER raft_guard_buckets_upd
BEFORE UPDATE ON buckets
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_buckets_del;
CREATE TRIGGER raft_guard_buckets_del
BEFORE DELETE ON buckets
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_bucket_policies_ins;
CREATE TRIGGER raft_guard_bucket_policies_ins
BEFORE INSERT ON bucket_policies
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_bucket_policies_upd;
CREATE TRIGGER raft_guard_bucket_policies_upd
BEFORE UPDATE ON bucket_policies
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_bucket_policies_del;
CREATE TRIGGER raft_guard_bucket_policies_del
BEFORE DELETE ON bucket_policies
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_ins;
CREATE TRIGGER raft_guard_lifecycle_rules_ins
BEFORE INSERT ON lifecycle_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_upd;
CREATE TRIGGER raft_guard_lifecycle_rules_upd
BEFORE UPDATE ON lifecycle_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_del;
CREATE TRIGGER raft_guard_lifecycle_rules_del
BEFORE DELETE ON lifecycle_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_event_configs_ins;
CREATE TRIGGER raft_guard_event_configs_ins
BEFORE INSERT ON event_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_event_configs_upd;
CREATE TRIGGER raft_guard_event_configs_upd
BEFORE UPDATE ON event_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_event_configs_del;
CREATE TRIGGER raft_guard_event_configs_del
BEFORE DELETE ON event_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_replication_rules_ins;
CREATE TRIGGER raft_guard_replication_rules_ins
BEFORE INSERT ON replication_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_replication_rules_upd;
CREATE TRIGGER raft_guard_replication_rules_upd
BEFORE UPDATE ON replication_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_replication_rules_del;
CREATE TRIGGER raft_guard_replication_rules_del
BEFORE DELETE ON replication_rules
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_points_ins;
CREATE TRIGGER raft_guard_access_points_ins
BEFORE INSERT ON access_points
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_points_upd;
CREATE TRIGGER raft_guard_access_points_upd
BEFORE UPDATE ON access_points
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_access_points_del;
CREATE TRIGGER raft_guard_access_points_del
BEFORE DELETE ON access_points
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_olap_configs_ins;
CREATE TRIGGER raft_guard_olap_configs_ins
BEFORE INSERT ON olap_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_olap_configs_upd;
CREATE TRIGGER raft_guard_olap_configs_upd
BEFORE UPDATE ON olap_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_olap_configs_del;
CREATE TRIGGER raft_guard_olap_configs_del
BEFORE DELETE ON olap_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_inventory_configs_ins;
CREATE TRIGGER raft_guard_inventory_configs_ins
BEFORE INSERT ON inventory_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_inventory_configs_upd;
CREATE TRIGGER raft_guard_inventory_configs_upd
BEFORE UPDATE ON inventory_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;

DROP TRIGGER IF EXISTS raft_guard_inventory_configs_del;
CREATE TRIGGER raft_guard_inventory_configs_del
BEFORE DELETE ON inventory_configs
WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1
BEGIN
    SELECT RAISE(ABORT, 'raft: control write outside apply');
END;
