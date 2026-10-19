//! M7：站点控制面 changelog 应用（对端条目 → 本端控制面）与出货 worker（切片 1b）。
//!
//! 冲突模型：LWW 元组 `(ts, priority, origin_site_id, seq)`（ts 优先，priority 同秒裁决）。
//! 毒丸：永久坏条目写 `site_apply_dead` 并越过（cursor 可推进）；DB/IO 错误整批 500 让对端重试。

use serde::Deserialize;
use sqlx::{Row, SqlitePool};
use xl_store::store::Store;

use crate::state::AppState;

const MAX_ENTRIES: usize = 500;
/// 单条 payload 上限（策略 64KiB 经一层 JSON 转义后仍可容纳——L5）。
const MAX_PAYLOAD: usize = 256 * 1024;
const MAX_DEAD_PAYLOAD: usize = 4096;
/// 单批线上体积预算（与 30s 控制面超时匹配：1MiB 在 ~300kbps 链路也可完成，
/// 避免超大策略批在慢链路上被超时永久卡住——L5/低-5）。
const MAX_BATCH_BYTES: usize = 1024 * 1024;
const CLOCK_SKEW_SECS: i64 = 300;

#[derive(Debug, Clone, Deserialize)]
pub struct Entry {
    pub seq: i64,
    pub origin_site_id: String,
    pub entity: String,
    pub entity_id: String,
    pub op: String,
    pub payload: serde_json::Value,
    pub priority: i64,
    pub ts: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ApplyStats {
    pub applied: usize,
    pub skipped: usize,
    pub dead: usize,
}

fn bad(msg: impl Into<String>) -> String {
    msg.into()
}

fn wins(e: &Entry, clock: Option<(i64, i64, String, i64)>) -> bool {
    match clock {
        None => true,
        Some(c) => (e.ts, e.priority, e.origin_site_id.clone(), e.seq) > c,
    }
}

async fn get_clock(
    tx: &mut sqlx::SqliteConnection,
    entity: &str,
    entity_id: &str,
) -> Result<Option<(i64, i64, String, i64)>, String> {
    let row = sqlx::query(
        "SELECT ts, priority, origin_site_id, seq FROM site_entity_clock \
         WHERE entity = ? AND entity_id = ?",
    )
    .bind(entity)
    .bind(entity_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| bad(format!("clock 查询失败：{e}")))?;
    Ok(row.map(|r| {
        (
            r.get("ts"),
            r.get("priority"),
            r.get("origin_site_id"),
            r.get("seq"),
        )
    }))
}

async fn put_clock(
    tx: &mut sqlx::SqliteConnection,
    entity: &str,
    entity_id: &str,
    e: &Entry,
) -> Result<(), String> {
    sqlx::query(
        "INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, \
         origin_site_id, seq) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(entity)
    .bind(entity_id)
    .bind(e.ts)
    .bind(e.priority)
    .bind(&e.origin_site_id)
    .bind(e.seq)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(format!("clock 写入失败：{e}")))?;
    sqlx::query(
        "UPDATE site_entity_clock SET ts=?, priority=?, origin_site_id=?, seq=? \
         WHERE entity=? AND entity_id=? AND (?,?,?,?) > (ts, priority, origin_site_id, seq)",
    )
    .bind(e.ts)
    .bind(e.priority)
    .bind(&e.origin_site_id)
    .bind(e.seq)
    .bind(entity)
    .bind(entity_id)
    .bind(e.ts)
    .bind(e.priority)
    .bind(&e.origin_site_id)
    .bind(e.seq)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(format!("clock 更新失败：{e}")))?;
    Ok(())
}

/// 写死信（同一 (peer,seq) 重投幂等——INSERT OR REPLACE）。
async fn bury(
    tx: &mut sqlx::SqliteConnection,
    peer: &str,
    e: &Entry,
    why: &str,
) -> Result<(), String> {
    let payload = e.payload.to_string();
    // 按 UTF-8 字符边界截断（多字节 CJK 命中 MAX 中间会 panic——M4）。
    let mut cut = payload.len().min(MAX_DEAD_PAYLOAD);
    while cut > 0 && !payload.is_char_boundary(cut) {
        cut -= 1;
    }
    let payload = &payload[..cut];
    sqlx::query(
        "INSERT OR REPLACE INTO site_apply_dead (peer_id, seq, entity, entity_id, error, \
         payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(peer)
    .bind(e.seq)
    .bind(&e.entity)
    .bind(&e.entity_id)
    .bind(why)
    .bind(payload)
    .bind(crate::db::now())
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(format!("死信写入失败：{e}")))?;
    // L3：死信原因同步到 status 的 last_error（按对端站点 id 归属）。
    sqlx::query("UPDATE sites SET last_error = ? WHERE remote_site_id = ?")
        .bind(why)
        .bind(peer)
        .execute(&mut *tx)
        .await
        .map_err(|e| bad(format!("死信 last_error 写入失败：{e}")))?;
    Ok(())
}

fn pstr(v: &serde_json::Value, k: &str) -> Option<String> {
    v.get(k).and_then(|x| x.as_str()).map(str::to_string)
}

fn pi64(v: &serde_json::Value, k: &str) -> Option<i64> {
    v.get(k).and_then(|x| x.as_i64())
}

/// 应用一批来自 peer 的 changelog 条目。错误 = 整批 500（重试幂等）。
pub async fn apply_changelog(state: &AppState, entries: Vec<Entry>) -> Result<ApplyStats, String> {
    if entries.len() > MAX_ENTRIES {
        return Err(bad(format!("批次超过 {MAX_ENTRIES} 条")));
    }
    let pool = state.db.pool().clone();
    let mut tx = pool.begin().await.map_err(|e| bad(e.to_string()))?;
    sqlx::query("UPDATE apply_ctx SET active = 1 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| bad(e.to_string()))?;

    let mut stats = ApplyStats::default();
    let mut bucket_entities: std::collections::BTreeSet<String> = Default::default();
    // B10：已应用的桶删除 → 提交成功后失效 AP 缓存（提交失败不失效）。
    let mut deleted_buckets: Vec<String> = Vec::new();
    let now = crate::db::now();

    for e in &entries {
        let payload_len = e.payload.to_string().len();
        if payload_len > MAX_PAYLOAD {
            bury(&mut tx, &e.origin_site_id, e, "payload 超限").await?;
            stats.dead += 1;
            continue;
        }
        if e.origin_site_id == state.site.id {
            stats.skipped += 1;
            continue;
        }
        if e.ts > now + CLOCK_SKEW_SECS {
            bury(&mut tx, &e.origin_site_id, e, "ts 超时钟偏移上限").await?;
            stats.dead += 1;
            continue;
        }
        if !matches!(e.op.as_str(), "upsert" | "delete") {
            bury(&mut tx, &e.origin_site_id, e, "未知 op").await?;
            stats.dead += 1;
            continue;
        }
        let clock = get_clock(&mut tx, &e.entity, &e.entity_id).await?;
        // R9：对账按**批次涉及 + 最终行**执行——clock 跳过的 bucket 条目也要在提交后
        // 确保物理桶与行一致（否则对账 500 后的重投全被跳过，物理桶永久缺失）。
        if e.entity == "bucket" {
            bucket_entities.insert(e.entity_id.clone());
        }
        if !wins(e, clock) {
            stats.skipped += 1;
            continue;
        }
        match e.entity.as_str() {
            "bucket" => match apply_bucket(&mut tx, &state.store, e).await? {
                Ok(()) => {
                    if e.op == "delete" {
                        deleted_buckets.push(e.entity_id.clone());
                    }
                    stats.applied += 1
                }
                Err(why) => {
                    bury(&mut tx, &e.origin_site_id, e, &why).await?;
                    stats.dead += 1;
                    continue;
                }
            },
            "bucket_policy" => match apply_policy(&mut tx, e).await? {
                Ok(()) => stats.applied += 1,
                Err(why) => {
                    bury(&mut tx, &e.origin_site_id, e, &why).await?;
                    stats.dead += 1;
                    continue;
                }
            },
            "permission" => match apply_permission(&mut tx, e).await? {
                Ok(()) => stats.applied += 1,
                Err(why) => {
                    bury(&mut tx, &e.origin_site_id, e, &why).await?;
                    stats.dead += 1;
                    continue;
                }
            },
            "user" => match apply_user(&mut tx, e).await? {
                Ok(()) => stats.applied += 1,
                Err(why) => {
                    bury(&mut tx, &e.origin_site_id, e, &why).await?;
                    stats.dead += 1;
                    continue;
                }
            },
            "access_key" => match apply_access_key(&mut tx, e).await? {
                Ok(()) => stats.applied += 1,
                Err(why) => {
                    bury(&mut tx, &e.origin_site_id, e, &why).await?;
                    stats.dead += 1;
                    continue;
                }
            },
            other => {
                bury(
                    &mut tx,
                    &e.origin_site_id,
                    e,
                    &format!("未知 entity：{other}"),
                )
                .await?;
                stats.dead += 1;
                continue;
            }
        }
        put_clock(&mut tx, &e.entity, &e.entity_id, e).await?;
    }

    sqlx::query("UPDATE apply_ctx SET active = 0 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| bad(e.to_string()))?;
    tx.commit().await.map_err(|e| bad(e.to_string()))?;

    // B10：只有提交成功才失效 AP 缓存。
    for name in deleted_buckets {
        state.bucket_cleanup.invalidate_ap_cache(&name);
    }

    // R9：提交后按**最终行**对账物理桶（clock 跳过也执行；重试幂等）
    for name in bucket_entities {
        // M5 残留：旧库可能有非法桶名行——永久跳过，不得让对账返回 500 卡死整条复制。
        if xl_store::layout::validate_bucket(&name).is_err() {
            continue;
        }
        let exists_row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
            .bind(&name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| bad(e.to_string()))?;
        if exists_row.is_none() {
            continue; // 物理删除已在条目应用时完成
        }
        match state.store.bucket_exists(&name).await {
            Ok(true) => {}
            Ok(false) => match state.store.create_bucket(&name).await {
                Ok(()) => {}
                Err(xl_store::error::Error::BucketAlreadyExists(_)) => {} // 并发窗口
                Err(e) => return Err(bad(format!("桶物理对账失败 {name}: {e}"))),
            },
            Err(e) => return Err(bad(format!("桶存在性查询失败 {name}: {e}"))),
        }
        // L6：建目录与本地删桶的 TOCTOU——若行在两步之间被删，清掉刚建的空目录。
        let still: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
            .bind(&name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| bad(e.to_string()))?;
        if still.is_none()
            && state.store.bucket_exists(&name).await.unwrap_or(false)
            && let Err(e) = state.store.delete_bucket(&name).await
        {
            tracing::warn!(error = %e, bucket = %name, "对账 TOCTOU 清理空桶失败（下次兜底）");
        }
    }
    Ok(stats)
}

/// 返回 Ok(Ok(())) = 应用成功；Ok(Err(why)) = 永久坏条目（dead）；Err(_) = 整批重试。
async fn apply_bucket(
    tx: &mut sqlx::SqliteConnection,
    store: &Store,
    e: &Entry,
) -> Result<Result<(), String>, String> {
    // M5：非法桶名是永久毒丸——必须在落行/物理操作前分类为 dead，否则对账每次 500。
    if let Err(err) = xl_store::layout::validate_bucket(&e.entity_id) {
        return Ok(Err(format!("非法桶名：{err}")));
    }
    if e.op == "delete" {
        match store.delete_bucket(&e.entity_id).await {
            Ok(()) | Err(xl_store::error::Error::NoSuchBucket(_)) => {
                for sql in [
                    "DELETE FROM buckets WHERE name = ?",
                    "DELETE FROM lifecycle_rules WHERE bucket = ?",
                    "DELETE FROM event_configs WHERE bucket = ?",
                    "DELETE FROM replication_rules WHERE bucket = ?",
                ] {
                    sqlx::query(sql)
                        .bind(&e.entity_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|err| bad(err.to_string()))?;
                }
                // L7：级联清策略/授权时尊重各自 clock——删除条目较旧时保留更新的远端行。
                sqlx::query(
                    "DELETE FROM bucket_policies WHERE bucket = ? AND NOT EXISTS (\
                       SELECT 1 FROM site_entity_clock c WHERE c.entity='bucket_policy' \
                         AND c.entity_id = bucket_policies.bucket \
                         AND (c.ts, c.priority, c.origin_site_id, c.seq) > (?, ?, ?, ?))",
                )
                .bind(&e.entity_id)
                .bind(e.ts)
                .bind(e.priority)
                .bind(&e.origin_site_id)
                .bind(e.seq)
                .execute(&mut *tx)
                .await
                .map_err(|err| bad(err.to_string()))?;
                sqlx::query(
                    "DELETE FROM permissions WHERE bucket = ? AND NOT EXISTS (\
                       SELECT 1 FROM site_entity_clock c WHERE c.entity='permission' \
                         AND c.entity_id = COALESCE((SELECT username FROM users WHERE id=permissions.user_id), '') \
                           || char(31) || permissions.bucket \
                         AND (c.ts, c.priority, c.origin_site_id, c.seq) > (?, ?, ?, ?))",
                )
                .bind(&e.entity_id)
                .bind(e.ts)
                .bind(e.priority)
                .bind(&e.origin_site_id)
                .bind(e.seq)
                .execute(&mut *tx)
                .await
                .map_err(|err| bad(err.to_string()))?;
                // B10：事务内清 M8 表（AP/Inventory/Tier/Lifecycle）；cache 失效由调用方
                // 在 commit 成功后执行——提交失败不得失效。
                crate::db::Db::clear_bucket_m8_tx(&mut *tx, &e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                Ok(Ok(()))
            }
            Err(xl_store::error::Error::BucketNotEmpty(_)) => Ok(Err("桶非空，删除未应用".into())),
            Err(err) => Err(bad(format!("删桶失败（可重试）：{err}"))),
        }
    } else {
        // owner：管理员/未知 → NULL（管理员行不复制；未知用户在快照前）
        let owner_username = pstr(&e.payload, "owner_username");
        let owner_id: Option<i64> = if let Some(u) = owner_username {
            let r = sqlx::query("SELECT id, is_admin FROM users WHERE username = ?")
                .bind(&u)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|err| bad(err.to_string()))?;
            match r {
                Some(row) if row.get::<i64, _>("is_admin") == 0 => Some(row.get("id")),
                _ => None,
            }
        } else {
            None
        };
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, set_id) \
             VALUES (?, ?, ?, ?, ?, 0) \
             ON CONFLICT(name) DO UPDATE SET owner_id=excluded.owner_id, \
               creation_date=excluded.creation_date, versioning=excluded.versioning, \
               object_lock=excluded.object_lock",
        )
        .bind(&e.entity_id)
        .bind(owner_id)
        .bind(pi64(&e.payload, "creation_date").unwrap_or(0))
        .bind(pstr(&e.payload, "versioning").unwrap_or_else(|| "Off".into()))
        .bind(pi64(&e.payload, "object_lock").unwrap_or(0))
        .execute(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
        Ok(Ok(()))
    }
}

async fn apply_policy(
    tx: &mut sqlx::SqliteConnection,
    e: &Entry,
) -> Result<Result<(), String>, String> {
    if e.op == "delete" {
        sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
            .bind(&e.entity_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| bad(err.to_string()))?;
    } else {
        let Some(policy) = pstr(&e.payload, "policy_json") else {
            return Ok(Err("policy_json 缺失".into()));
        };
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET policy_json=excluded.policy_json, \
               updated_at=excluded.updated_at",
        )
        .bind(&e.entity_id)
        .bind(policy)
        .bind(pi64(&e.payload, "updated_at").unwrap_or(0))
        .execute(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

async fn user_of(
    tx: &mut sqlx::SqliteConnection,
    username: &str,
) -> Result<Option<(i64, bool)>, String> {
    let r = sqlx::query("SELECT id, is_admin FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| bad(e.to_string()))?;
    Ok(r.map(|r| (r.get("id"), r.get::<i64, _>("is_admin") != 0)))
}

#[allow(clippy::explicit_auto_deref)]
async fn apply_permission(
    tx: &mut sqlx::SqliteConnection,
    e: &Entry,
) -> Result<Result<(), String>, String> {
    let Some(username) = pstr(&e.payload, "username") else {
        return Ok(Err("username 缺失".into()));
    };
    let Some((uid, admin)) = user_of(&mut *tx, &username).await? else {
        return Ok(Err("未知用户（授权跳过）".into()));
    };
    if admin {
        return Ok(Err("目标为本地管理员（授权跳过）".into()));
    }
    if e.op == "delete" {
        let bucket = pstr(&e.payload, "bucket")
            .unwrap_or_else(|| e.entity_id.split('\u{1f}').nth(1).unwrap_or("").to_string());
        sqlx::query("DELETE FROM permissions WHERE user_id = ? AND bucket = ?")
            .bind(uid)
            .bind(bucket)
            .execute(&mut *tx)
            .await
            .map_err(|err| bad(err.to_string()))?;
    } else {
        let bucket = pstr(&e.payload, "bucket").unwrap_or_default();
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id, bucket) DO UPDATE SET \
               can_read=excluded.can_read, can_write=excluded.can_write, \
               can_delete=excluded.can_delete",
        )
        .bind(uid)
        .bind(bucket)
        .bind(pi64(&e.payload, "can_read").unwrap_or(0))
        .bind(pi64(&e.payload, "can_write").unwrap_or(0))
        .bind(pi64(&e.payload, "can_delete").unwrap_or(0))
        .execute(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

#[allow(clippy::explicit_auto_deref)]
async fn apply_user(
    tx: &mut sqlx::SqliteConnection,
    e: &Entry,
) -> Result<Result<(), String>, String> {
    if e.op == "delete" {
        let Some((uid, admin)) = user_of(&mut *tx, &e.entity_id).await? else {
            return Ok(Ok(())); // 幂等：本端不存在
        };
        if admin {
            return Ok(Err("拒绝删除本地管理员".into()));
        }
        for sql in [
            "DELETE FROM access_keys WHERE user_id = ?",
            "DELETE FROM permissions WHERE user_id = ?",
            "UPDATE uploads SET owner_id = NULL WHERE owner_id = ?",
            "UPDATE buckets SET owner_id = NULL WHERE owner_id = ?",
            "DELETE FROM users WHERE id = ?",
        ] {
            sqlx::query(sql)
                .bind(uid)
                .execute(&mut *tx)
                .await
                .map_err(|err| bad(err.to_string()))?;
        }
        return Ok(Ok(()));
    }
    let Some(uid_admin) = user_of(&mut *tx, &e.entity_id).await? else {
        let Some(hash) = pstr(&e.payload, "password_hash") else {
            return Ok(Err("password_hash 缺失".into()));
        };
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES (?, ?, 0, ?, ?)",
        )
        .bind(&e.entity_id)
        .bind(hash)
        .bind(pstr(&e.payload, "status").unwrap_or_else(|| "active".into()))
        .bind(pi64(&e.payload, "created_at").unwrap_or(0))
        .execute(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
        return Ok(Ok(()));
    };
    if uid_admin.1 {
        return Ok(Err("目标是本地管理员（不覆盖）".into()));
    }
    if let Some(hash) = pstr(&e.payload, "password_hash") {
        sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(hash)
            .bind(uid_admin.0)
            .execute(&mut *tx)
            .await
            .map_err(|err| bad(err.to_string()))?;
    }
    if let Some(status) = pstr(&e.payload, "status") {
        sqlx::query("UPDATE users SET status = ? WHERE id = ?")
            .bind(status)
            .bind(uid_admin.0)
            .execute(&mut *tx)
            .await
            .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

#[allow(clippy::explicit_auto_deref)]
async fn apply_access_key(
    tx: &mut sqlx::SqliteConnection,
    e: &Entry,
) -> Result<Result<(), String>, String> {
    if e.op == "delete" {
        // L1：本地管理员的 AK 不得被远端删除（与 upsert 的 R11 守卫同口径）。
        let owner = sqlx::query(
            "SELECT u.is_admin FROM access_keys k JOIN users u ON u.id = k.user_id \
             WHERE k.access_key = ? AND k.kind = 'long-term'",
        )
        .bind(&e.entity_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
        if let Some(r) = owner
            && r.get::<i64, _>("is_admin") != 0
        {
            return Ok(Err("目标是本地管理员 AK（删除跳过）".into()));
        }
        sqlx::query("DELETE FROM access_keys WHERE access_key = ? AND kind = 'long-term'")
            .bind(&e.entity_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| bad(err.to_string()))?;
        return Ok(Ok(()));
    }
    let Some(username) = pstr(&e.payload, "username") else {
        return Ok(Err("username 缺失".into()));
    };
    let Some((uid, admin)) = user_of(&mut *tx, &username).await? else {
        return Ok(Err("未知用户（AK 跳过）".into()));
    };
    if admin {
        return Ok(Err("目标是本地管理员（AK 跳过）".into()));
    }
    let existing = sqlx::query("SELECT user_id, kind FROM access_keys WHERE access_key = ?")
        .bind(&e.entity_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| bad(err.to_string()))?;
    if let Some(r) = &existing
        && (r.get::<i64, _>("user_id") != uid || r.get::<String, _>("kind") != "long-term")
    {
        return Ok(Err("同 AK 属主/类型冲突（不静默改属）".into()));
    }
    let Some(secret) = pstr(&e.payload, "secret_key") else {
        return Ok(Err("secret_key 缺失".into()));
    };
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, \
         created_at, kind) VALUES (?, ?, ?, ?, ?, ?, 'long-term') \
         ON CONFLICT(access_key) DO UPDATE SET secret_key=excluded.secret_key, \
           user_id=excluded.user_id, status=excluded.status, expires_at=excluded.expires_at",
    )
    .bind(&e.entity_id)
    .bind(secret)
    .bind(uid)
    .bind(pstr(&e.payload, "status").unwrap_or_else(|| "active".into()))
    .bind(pi64(&e.payload, "expires_at"))
    .bind(pi64(&e.payload, "created_at").unwrap_or(0))
    .execute(&mut *tx)
    .await
    .map_err(|err| bad(err.to_string()))?;
    Ok(Ok(()))
}

/// 供测试与 status：未处理死信数。
pub async fn dead_count(pool: &SqlitePool, peer_id: &str) -> Result<i64, String> {
    sqlx::query_scalar("SELECT COUNT(*) FROM site_apply_dead WHERE peer_id = ?")
        .bind(peer_id)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())
}

// ---------------- 切片 1b：配对快照 / 出货 worker / status ----------------

async fn snapshot_entry(
    tx: &mut sqlx::SqliteConnection,
    origin: &str,
    priority: i64,
    ts: i64,
    entity: &str,
    entity_id: &str,
    payload: &serde_json::Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, \
         ts, created_at) VALUES (?, ?, ?, 'upsert', ?, ?, ?, ?)",
    )
    .bind(origin)
    .bind(entity)
    .bind(entity_id)
    .bind(payload.to_string())
    .bind(priority)
    .bind(ts)
    .bind(ts)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    let seq: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| bad(e.to_string()))?;
    sqlx::query(
        "INSERT OR IGNORE INTO site_entity_clock (entity, entity_id, ts, priority, \
         origin_site_id, seq) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(entity)
    .bind(entity_id)
    .bind(ts)
    .bind(priority)
    .bind(origin)
    .bind(seq)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    sqlx::query(
        "UPDATE site_entity_clock SET ts=?, priority=?, origin_site_id=?, seq=? \
         WHERE entity=? AND entity_id=? AND (?,?,?,?) > (ts, priority, origin_site_id, seq)",
    )
    .bind(ts)
    .bind(priority)
    .bind(origin)
    .bind(seq)
    .bind(entity)
    .bind(entity_id)
    .bind(ts)
    .bind(priority)
    .bind(origin)
    .bind(seq)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    Ok(())
}

/// `POST /sites` 的持久化：sites 行 + cursor + 控制面快照**同一事务**（P1）。
/// 快照只包含「本地 clock 无行或 origin=本站」的实体（不回显远端值）。
pub async fn insert_peer_with_snapshot(
    state: &AppState,
    peer: &crate::db::SitePeer,
) -> Result<(), String> {
    let pool = state.db.pool().clone();
    let mut tx = pool.begin().await.map_err(|e| bad(e.to_string()))?;
    // L9：单 peer 约束与插入原子化（并发 POST /sites 不得建出两个 peer）。
    let inserted = sqlx::query(
        "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
         s3_access_key, s3_secret_key, peer_site_token, created_at) \
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ? \
         WHERE NOT EXISTS (SELECT 1 FROM sites)",
    )
    .bind(&peer.peer_id)
    .bind(&peer.remote_site_id)
    .bind(&peer.name)
    .bind(&peer.endpoint)
    .bind(&peer.region)
    .bind(peer.priority)
    .bind(&peer.s3_access_key)
    .bind(&peer.s3_secret_key)
    .bind(&peer.peer_site_token)
    .bind(peer.created_at)
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?
    .rows_affected();
    if inserted != 1 {
        return Err("已存在站点对（M7 单 peer）".into());
    }
    sqlx::query(
        "INSERT OR IGNORE INTO site_cursors (peer_id, last_seq, updated_at) VALUES (?, 0, ?)",
    )
    .bind(&peer.peer_id)
    .bind(crate::db::now())
    .execute(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;

    let local = state.site.clone();
    let ts = crate::db::now();
    let missing = "c.entity_id IS NULL OR c.origin_site_id = ";

    // 用户
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT u.username, u.password_hash, u.status, u.created_at FROM users u \
             LEFT JOIN site_entity_clock c ON c.entity='user' AND c.entity_id=u.username \
             WHERE u.is_admin=0 AND ({missing}?)",
    )))
    .bind(&local.id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    for r in rows {
        let username: String = r.get("username");
        let payload = serde_json::json!({
            "username": username,
            "password_hash": r.get::<String, _>("password_hash"),
            "status": r.get::<String, _>("status"),
            "created_at": r.get::<i64, _>("created_at"),
        });
        snapshot_entry(
            &mut tx,
            &local.id,
            local.priority,
            ts,
            "user",
            &username,
            &payload,
        )
        .await?;
    }
    // 长时访问密钥
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT k.access_key, k.secret_key, k.status, k.expires_at, k.created_at, u.username \
             FROM access_keys k JOIN users u ON u.id=k.user_id \
             LEFT JOIN site_entity_clock c ON c.entity='access_key' AND c.entity_id=k.access_key \
             WHERE u.is_admin=0 AND k.kind='long-term' AND ({missing}?)",
    )))
    .bind(&local.id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    for r in rows {
        let ak: String = r.get("access_key");
        let payload = serde_json::json!({
            "access_key": ak,
            "secret_key": r.get::<String, _>("secret_key"),
            "username": r.get::<String, _>("username"),
            "status": r.get::<String, _>("status"),
            "expires_at": r.get::<Option<i64>, _>("expires_at"),
            "created_at": r.get::<i64, _>("created_at"),
        });
        snapshot_entry(
            &mut tx,
            &local.id,
            local.priority,
            ts,
            "access_key",
            &ak,
            &payload,
        )
        .await?;
    }
    // 权限
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT p.bucket, p.can_read, p.can_write, p.can_delete, u.username \
             FROM permissions p JOIN users u ON u.id=p.user_id \
             LEFT JOIN site_entity_clock c ON c.entity='permission' \
               AND c.entity_id=u.username || char(31) || p.bucket \
             WHERE u.is_admin=0 AND ({missing}?)",
    )))
    .bind(&local.id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    for r in rows {
        let username: String = r.get("username");
        let bucket: String = r.get("bucket");
        let eid = format!("{username}\u{1f}{bucket}");
        let payload = serde_json::json!({
            "username": username,
            "bucket": bucket,
            "can_read": r.get::<i64, _>("can_read"),
            "can_write": r.get::<i64, _>("can_write"),
            "can_delete": r.get::<i64, _>("can_delete"),
        });
        snapshot_entry(
            &mut tx,
            &local.id,
            local.priority,
            ts,
            "permission",
            &eid,
            &payload,
        )
        .await?;
    }
    // 桶（owner 以 username 表达）
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT b.name, b.creation_date, b.versioning, b.object_lock, \
             (SELECT username FROM users WHERE id=b.owner_id) AS owner_username \
             FROM buckets b LEFT JOIN site_entity_clock c ON c.entity='bucket' AND c.entity_id=b.name \
             WHERE ({missing}?)",
    )))
    .bind(&local.id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    for r in rows {
        let name: String = r.get("name");
        let payload = serde_json::json!({
            "name": name,
            "owner_username": r.get::<Option<String>, _>("owner_username"),
            "creation_date": r.get::<i64, _>("creation_date"),
            "versioning": r.get::<String, _>("versioning"),
            "object_lock": r.get::<i64, _>("object_lock"),
        });
        snapshot_entry(
            &mut tx,
            &local.id,
            local.priority,
            ts,
            "bucket",
            &name,
            &payload,
        )
        .await?;
    }
    // 桶策略
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT p.bucket, p.policy_json, p.updated_at FROM bucket_policies p \
             LEFT JOIN site_entity_clock c ON c.entity='bucket_policy' AND c.entity_id=p.bucket \
             WHERE ({missing}?)",
    )))
    .bind(&local.id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| bad(e.to_string()))?;
    for r in rows {
        let bucket: String = r.get("bucket");
        let payload = serde_json::json!({
            "bucket": bucket,
            "policy_json": r.get::<String, _>("policy_json"),
            "updated_at": r.get::<i64, _>("updated_at"),
        });
        snapshot_entry(
            &mut tx,
            &local.id,
            local.priority,
            ts,
            "bucket_policy",
            &bucket,
            &payload,
        )
        .await?;
    }
    tx.commit().await.map_err(|e| bad(e.to_string()))?;
    Ok(())
}

/// 一轮出货：逐 peer 推 cursor 之后的 changelog（≤100 条/批）。
pub async fn ship_once(pool: &SqlitePool) -> Result<(usize, usize), String> {
    let peers = sqlx::query("SELECT peer_id, endpoint, peer_site_token FROM sites")
        .fetch_all(pool)
        .await
        .map_err(|e| bad(e.to_string()))?;
    let (mut shipped_peers, mut shipped_entries) = (0usize, 0usize);
    for p in peers {
        let peer_id: String = p.get("peer_id");
        let endpoint: String = p.get("endpoint");
        let token: String = p.get("peer_site_token");
        let cursor: i64 = sqlx::query_scalar("SELECT last_seq FROM site_cursors WHERE peer_id = ?")
            .bind(&peer_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| bad(e.to_string()))?
            .unwrap_or(0);
        let rows = sqlx::query(
            "SELECT seq, origin_site_id, entity, entity_id, op, payload, priority, ts \
             FROM site_changelog WHERE seq > ? ORDER BY seq LIMIT 100",
        )
        .bind(cursor)
        .fetch_all(pool)
        .await
        .map_err(|e| bad(e.to_string()))?;
        if rows.is_empty() {
            continue;
        }
        // L4/L5：非 JSON 行不得静默跨过（否则该变更永久丢失）；批次按线上体积预算截断。
        let mut max_seq = cursor;
        let mut batch_bytes = 0usize;
        let mut entries = Vec::with_capacity(rows.len());
        for r in &rows {
            let payload_txt: String = r.get("payload");
            let payload = serde_json::from_str::<serde_json::Value>(&payload_txt).map_err(|e| {
                bad(format!(
                    "changelog seq {} payload 非 JSON（拒绝推进 cursor）：{e}",
                    r.get::<i64, _>("seq")
                ))
            })?;
            if !entries.is_empty() && batch_bytes + payload_txt.len() + 512 > MAX_BATCH_BYTES {
                break;
            }
            batch_bytes += payload_txt.len() + 512;
            max_seq = max_seq.max(r.get::<i64, _>("seq"));
            entries.push(serde_json::json!({
                "seq": r.get::<i64, _>("seq"),
                "origin_site_id": r.get::<String, _>("origin_site_id"),
                "entity": r.get::<String, _>("entity"),
                "entity_id": r.get::<String, _>("entity_id"),
                "op": r.get::<String, _>("op"),
                "payload": payload,
                "priority": r.get::<i64, _>("priority"),
                "ts": r.get::<i64, _>("ts"),
            }));
        }
        let body = serde_json::json!({ "entries": entries }).to_string();
        match crate::site_client::post_changelog(&endpoint, &token, bytes::Bytes::from(body)).await
        {
            Ok((200, _)) => {
                sqlx::query(
                    "UPDATE site_cursors SET last_seq = ?, updated_at = ? WHERE peer_id = ?",
                )
                .bind(max_seq)
                .bind(crate::db::now())
                .bind(&peer_id)
                .execute(pool)
                .await
                .map_err(|e| bad(e.to_string()))?;
                let _ = sqlx::query("UPDATE sites SET last_error = NULL WHERE peer_id = ?")
                    .bind(&peer_id)
                    .execute(pool)
                    .await;
                shipped_peers += 1;
                shipped_entries += entries.len();
            }
            Ok((status, body)) => {
                let msg = format!(
                    "changelog 出货 {status}：{}",
                    String::from_utf8_lossy(&body)
                        .chars()
                        .take(200)
                        .collect::<String>()
                );
                let _ = sqlx::query("UPDATE sites SET last_error = ? WHERE peer_id = ?")
                    .bind(&msg)
                    .bind(&peer_id)
                    .execute(pool)
                    .await;
                tracing::warn!(%msg, peer = %peer_id, "changelog 出货失败（下轮重试）");
            }
            Err(e) => {
                let _ = sqlx::query("UPDATE sites SET last_error = ? WHERE peer_id = ?")
                    .bind(&e)
                    .bind(&peer_id)
                    .execute(pool)
                    .await;
                tracing::warn!(error = %e, peer = %peer_id, "changelog 出货失败（下轮重试）");
            }
        }
    }
    Ok((shipped_peers, shipped_entries))
}

/// serve 挂载（`OSS_SITE_INTERVAL_SECS` 默认 1s；0=关）。
pub fn spawn_site_worker(pool: SqlitePool) {
    let secs: u64 = std::env::var("OSS_SITE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if secs == 0 {
        tracing::info!("站点 changelog worker 已关闭（OSS_SITE_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let _ = ship_once(&pool).await;
        }
    });
}
