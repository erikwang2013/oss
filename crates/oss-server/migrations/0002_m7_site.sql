-- M7 多站点异步复制：站点身份/关系、控制面 changelog、游标与死信。
-- 本文件每次启动重放（IF NOT EXISTS 幂等）；触发器在切片 1a 以 DROP+CREATE 追加于此。
-- 注意：repl_outbox.target_kind 是 ALTER 增量列，由 db.rs::ensure_columns 探测补齐（不在本文件）。

CREATE TABLE IF NOT EXISTS site_config (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    region     TEXT NOT NULL,
    priority   INTEGER NOT NULL DEFAULT 0,
    site_token TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sites (
    peer_id         TEXT PRIMARY KEY,       -- 管理员起的别名（出站行 remote_id 用它）
    remote_site_id  TEXT NOT NULL DEFAULT '', -- 对端 site_config.id（dead/日志归属）
    name            TEXT NOT NULL,
    endpoint        TEXT NOT NULL,          -- http://host:port（无 path/query）
    region          TEXT NOT NULL DEFAULT 'us-east-1',
    priority        INTEGER NOT NULL DEFAULT 0,
    s3_access_key   TEXT NOT NULL,          -- 对端铸造的 AK（对象复制 SigV4）
    s3_secret_key   TEXT NOT NULL,
    peer_site_token TEXT NOT NULL,          -- 出站控制面请求呈现给对端的 token
    created_at      INTEGER NOT NULL DEFAULT 0,
    last_seen       INTEGER,
    last_error      TEXT
);

-- apply 期间抑制触发器回环（事务内置 1，回滚即复位）
CREATE TABLE IF NOT EXISTS apply_ctx (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    active INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO apply_ctx (id, active) VALUES (1, 0);

-- 控制面变更日志（触发器写入；配对快照也直接写入）
CREATE TABLE IF NOT EXISTS site_changelog (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_site_id TEXT NOT NULL,
    entity         TEXT NOT NULL,           -- bucket | bucket_policy | permission | user | access_key
    entity_id      TEXT NOT NULL,
    op             TEXT NOT NULL,           -- upsert | delete
    payload        TEXT NOT NULL,
    priority       INTEGER NOT NULL,
    ts             INTEGER NOT NULL,
    created_at     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_site_changelog_seq ON site_changelog(seq);

-- LWW 时钟：每个实体胜出元组的当前值（本地写与 apply 共用）
CREATE TABLE IF NOT EXISTS site_entity_clock (
    entity         TEXT NOT NULL,
    entity_id      TEXT NOT NULL,
    ts             INTEGER NOT NULL,
    priority       INTEGER NOT NULL,
    origin_site_id TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    PRIMARY KEY (entity, entity_id)
);

-- 每个 peer 的出货游标
CREATE TABLE IF NOT EXISTS site_cursors (
    peer_id    TEXT PRIMARY KEY,
    last_seq   INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

-- 对端条目的永久失败（越过毒丸；cursor 继续推进）
CREATE TABLE IF NOT EXISTS site_apply_dead (
    peer_id    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    entity     TEXT NOT NULL,
    entity_id  TEXT NOT NULL,
    error      TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (peer_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_site_apply_dead_peer ON site_apply_dead(peer_id, created_at);

-- ===================== M7 切片 1a：控制面 changelog 触发器 =====================
-- 统一激活条件：不在 apply 事务中、已配对（sites 非空）、site_config 已种子。
-- BEFORE 门控：候选 LWW 元组 (now, priority, origin) < clock 时拒绝本地写（RAISE ABORT）。
-- AFTER 记录：写 changelog（seq=last_insert_rowid）并条件更新 clock。

-- buckets：只捕获 versioning/owner_id/object_lock 的实值变更（set_id/启动回填不触发）
DROP TRIGGER IF EXISTS site_gate_bucket_ins;
CREATE TRIGGER site_gate_bucket_ins BEFORE INSERT ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket' AND entity_id=NEW.name)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_bucket_upd;
CREATE TRIGGER site_gate_bucket_upd BEFORE UPDATE OF versioning, owner_id, object_lock ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (OLD.versioning IS NOT NEW.versioning OR OLD.owner_id IS NOT NEW.owner_id OR OLD.object_lock IS NOT NEW.object_lock)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket' AND entity_id=NEW.name)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_bucket_del;
CREATE TRIGGER site_gate_bucket_del BEFORE DELETE ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket' AND entity_id=OLD.name)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_log_bucket_ins;
CREATE TRIGGER site_log_bucket_ins AFTER INSERT ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket', NEW.name, 'upsert', json_object('name', NEW.name, 'owner_username', (SELECT username FROM users WHERE id=NEW.owner_id), 'creation_date', NEW.creation_date, 'versioning', NEW.versioning, 'object_lock', NEW.object_lock),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket', NEW.name, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket' AND entity_id = NEW.name
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_bucket_upd;
CREATE TRIGGER site_log_bucket_upd AFTER UPDATE OF versioning, owner_id, object_lock ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (OLD.versioning IS NOT NEW.versioning OR OLD.owner_id IS NOT NEW.owner_id OR OLD.object_lock IS NOT NEW.object_lock)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket', NEW.name, 'upsert', json_object('name', NEW.name, 'owner_username', (SELECT username FROM users WHERE id=NEW.owner_id), 'creation_date', NEW.creation_date, 'versioning', NEW.versioning, 'object_lock', NEW.object_lock),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket', NEW.name, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket' AND entity_id = NEW.name
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_bucket_del;
CREATE TRIGGER site_log_bucket_del AFTER DELETE ON buckets
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket', OLD.name, 'delete', json_object('name', OLD.name),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket', OLD.name, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket' AND entity_id = OLD.name
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;


-- bucket_policies
DROP TRIGGER IF EXISTS site_gate_policy_ins;
CREATE TRIGGER site_gate_policy_ins BEFORE INSERT ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket_policy' AND entity_id=NEW.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_policy_upd;
CREATE TRIGGER site_gate_policy_upd BEFORE UPDATE ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (OLD.policy_json IS NOT NEW.policy_json OR OLD.updated_at IS NOT NEW.updated_at)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket_policy' AND entity_id=NEW.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_policy_del;
CREATE TRIGGER site_gate_policy_del BEFORE DELETE ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='bucket_policy' AND entity_id=OLD.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_log_policy_ins;
CREATE TRIGGER site_log_policy_ins AFTER INSERT ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket_policy', NEW.bucket, 'upsert', json_object('bucket', NEW.bucket, 'policy_json', NEW.policy_json, 'updated_at', NEW.updated_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket_policy', NEW.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket_policy' AND entity_id = NEW.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_policy_upd;
CREATE TRIGGER site_log_policy_upd AFTER UPDATE ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (OLD.policy_json IS NOT NEW.policy_json OR OLD.updated_at IS NOT NEW.updated_at)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket_policy', NEW.bucket, 'upsert', json_object('bucket', NEW.bucket, 'policy_json', NEW.policy_json, 'updated_at', NEW.updated_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket_policy', NEW.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket_policy' AND entity_id = NEW.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_policy_del;
CREATE TRIGGER site_log_policy_del AFTER DELETE ON bucket_policies
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'bucket_policy', OLD.bucket, 'delete', json_object('bucket', OLD.bucket),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'bucket_policy', OLD.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'bucket_policy' AND entity_id = OLD.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;


-- permissions：仅非管理员用户的授权行；entity_id = username || US(0x1f) || bucket
DROP TRIGGER IF EXISTS site_gate_perm_ins;
CREATE TRIGGER site_gate_perm_ins BEFORE INSERT ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='permission' AND entity_id=COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_perm_upd;
CREATE TRIGGER site_gate_perm_upd BEFORE UPDATE ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='permission' AND entity_id=COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_perm_del;
CREATE TRIGGER site_gate_perm_del BEFORE DELETE ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=OLD.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='permission' AND entity_id=COALESCE((SELECT username FROM users WHERE id=OLD.user_id), '') || char(31) || OLD.bucket)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_log_perm_ins;
CREATE TRIGGER site_log_perm_ins AFTER INSERT ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'permission', COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket, 'upsert', json_object('username', (SELECT username FROM users WHERE id=NEW.user_id), 'bucket', NEW.bucket, 'can_read', NEW.can_read, 'can_write', NEW.can_write, 'can_delete', NEW.can_delete),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'permission', COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'permission' AND entity_id = COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_perm_upd;
CREATE TRIGGER site_log_perm_upd AFTER UPDATE ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'permission', COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket, 'upsert', json_object('username', (SELECT username FROM users WHERE id=NEW.user_id), 'bucket', NEW.bucket, 'can_read', NEW.can_read, 'can_write', NEW.can_write, 'can_delete', NEW.can_delete),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'permission', COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'permission' AND entity_id = COALESCE((SELECT username FROM users WHERE id=NEW.user_id), '') || char(31) || NEW.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_perm_del;
CREATE TRIGGER site_log_perm_del AFTER DELETE ON permissions
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND (SELECT is_admin FROM users WHERE id=OLD.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'permission', COALESCE((SELECT username FROM users WHERE id=OLD.user_id), '') || char(31) || OLD.bucket, 'delete', json_object('username', (SELECT username FROM users WHERE id=OLD.user_id), 'bucket', OLD.bucket, 'can_read', OLD.can_read, 'can_write', OLD.can_write, 'can_delete', OLD.can_delete),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'permission', COALESCE((SELECT username FROM users WHERE id=OLD.user_id), '') || char(31) || OLD.bucket, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'permission' AND entity_id = COALESCE((SELECT username FROM users WHERE id=OLD.user_id), '') || char(31) || OLD.bucket
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;


-- users：管理员行不复制（两端各自管理）
DROP TRIGGER IF EXISTS site_gate_user_ins;
CREATE TRIGGER site_gate_user_ins BEFORE INSERT ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.is_admin = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='user' AND entity_id=NEW.username)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_user_upd;
CREATE TRIGGER site_gate_user_upd BEFORE UPDATE ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.is_admin = 0 AND OLD.is_admin = 0
      AND (OLD.username IS NOT NEW.username OR OLD.password_hash IS NOT NEW.password_hash
           OR OLD.status IS NOT NEW.status OR OLD.created_at IS NOT NEW.created_at)
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='user' AND entity_id=NEW.username)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_user_del;
CREATE TRIGGER site_gate_user_del BEFORE DELETE ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.is_admin = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='user' AND entity_id=OLD.username)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_log_user_ins;
CREATE TRIGGER site_log_user_ins AFTER INSERT ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.is_admin = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'user', NEW.username, 'upsert', json_object('username', NEW.username, 'password_hash', NEW.password_hash, 'status', NEW.status, 'created_at', NEW.created_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'user', NEW.username, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'user' AND entity_id = NEW.username
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_user_upd;
CREATE TRIGGER site_log_user_upd AFTER UPDATE ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.is_admin = 0 AND OLD.is_admin = 0
      AND (OLD.username IS NOT NEW.username OR OLD.password_hash IS NOT NEW.password_hash
           OR OLD.status IS NOT NEW.status OR OLD.created_at IS NOT NEW.created_at)
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'user', NEW.username, 'upsert', json_object('username', NEW.username, 'password_hash', NEW.password_hash, 'status', NEW.status, 'created_at', NEW.created_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'user', NEW.username, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'user' AND entity_id = NEW.username
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_user_del;
CREATE TRIGGER site_log_user_del AFTER DELETE ON users
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.is_admin = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'user', OLD.username, 'delete', json_object('username', OLD.username),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'user', OLD.username, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'user' AND entity_id = OLD.username
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;


-- access_keys：仅长时键且属主非管理员（STS 会话键不复制）
DROP TRIGGER IF EXISTS site_gate_ak_ins;
CREATE TRIGGER site_gate_ak_ins BEFORE INSERT ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='access_key' AND entity_id=NEW.access_key)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_ak_upd;
CREATE TRIGGER site_gate_ak_upd BEFORE UPDATE ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.kind = 'long-term' AND NEW.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='access_key' AND entity_id=NEW.access_key)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_gate_ak_del;
CREATE TRIGGER site_gate_ak_del BEFORE DELETE ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=OLD.user_id) = 0
      AND (CAST(strftime('%s','now') AS INTEGER), (SELECT priority FROM site_config LIMIT 1), (SELECT id FROM site_config LIMIT 1))
        < (SELECT ts, priority, origin_site_id FROM site_entity_clock
       WHERE entity='access_key' AND entity_id=OLD.access_key)
BEGIN SELECT RAISE(ABORT,'site replication conflict'); END;

DROP TRIGGER IF EXISTS site_log_ak_ins;
CREATE TRIGGER site_log_ak_ins AFTER INSERT ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND NEW.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'access_key', NEW.access_key, 'upsert', json_object('access_key', NEW.access_key, 'secret_key', NEW.secret_key, 'username', (SELECT username FROM users WHERE id=NEW.user_id), 'status', NEW.status, 'expires_at', NEW.expires_at, 'created_at', NEW.created_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'access_key', NEW.access_key, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'access_key' AND entity_id = NEW.access_key
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_ak_upd;
CREATE TRIGGER site_log_ak_upd AFTER UPDATE ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.kind = 'long-term' AND NEW.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=NEW.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'access_key', NEW.access_key, 'upsert', json_object('access_key', NEW.access_key, 'secret_key', NEW.secret_key, 'username', (SELECT username FROM users WHERE id=NEW.user_id), 'status', NEW.status, 'expires_at', NEW.expires_at, 'created_at', NEW.created_at),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'access_key', NEW.access_key, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'access_key' AND entity_id = NEW.access_key
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;

DROP TRIGGER IF EXISTS site_log_ak_del;
CREATE TRIGGER site_log_ak_del AFTER DELETE ON access_keys
    WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)
      AND EXISTS (SELECT 1 FROM sites)
      AND EXISTS (SELECT 1 FROM site_config)
      AND OLD.kind = 'long-term' AND (SELECT is_admin FROM users WHERE id=OLD.user_id) = 0
BEGIN
  INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts, created_at)
  VALUES ((SELECT id FROM site_config LIMIT 1), 'access_key', OLD.access_key, 'delete', json_object('access_key', OLD.access_key, 'username', (SELECT username FROM users WHERE id=OLD.user_id)),
          (SELECT priority FROM site_config LIMIT 1),
          CAST(strftime('%s','now') AS INTEGER), CAST(strftime('%s','now') AS INTEGER));
  INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq)
  SELECT 'access_key', OLD.access_key, ts, priority, origin_site_id, seq
    FROM site_changelog WHERE seq = last_insert_rowid();
UPDATE site_entity_clock SET
  ts = (SELECT ts FROM site_changelog WHERE seq = last_insert_rowid()),
  priority = (SELECT priority FROM site_changelog WHERE seq = last_insert_rowid()),
  origin_site_id = (SELECT origin_site_id FROM site_changelog WHERE seq = last_insert_rowid()),
  seq = (SELECT seq FROM site_changelog WHERE seq = last_insert_rowid())
 WHERE entity = 'access_key' AND entity_id = OLD.access_key
   AND (SELECT ts, priority, origin_site_id, seq FROM site_changelog WHERE seq = last_insert_rowid())
     > (ts, priority, origin_site_id, seq);
END;
