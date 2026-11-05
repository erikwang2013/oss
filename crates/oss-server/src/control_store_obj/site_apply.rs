//! Application-level replacement for the 30 M7 site triggers (design §5.2).
//!
//! Domain writes call one hook per affected row, after staging the row but in
//! the same transaction. A hook is a no-op unless the origin is `Local`, a peer
//! is paired and `site_config` is seeded. A losing LWW write returns
//! `site replication conflict` and aborts the whole transaction.
//!
//! `SiteApply` bypasses the automatic gate/log: its caller records the winning
//! remote clock explicitly via [`ObjectControlTxn::site_put_clock`], exactly as
//! `site.rs` did with `put_clock`.

use xl_store::control_store::ControlError;

use super::jobs::{backend, decode, encode, scan_txn};
use super::site::{SiteChangelogRow, SiteEntityClockRow, changelog_key, clock_key, config_row_txn};
use super::{ObjectControlTxn, TableId, WriteOrigin};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketSiteRow {
    pub name: String,
    pub owner_uid: Option<i64>,
    pub owner_username: Option<String>,
    pub creation_date: i64,
    pub versioning: String,
    pub object_lock: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicySiteRow {
    pub bucket: String,
    pub policy_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionSiteRow {
    pub username: Option<String>,
    pub bucket: String,
    pub can_read: bool,
    pub can_write: bool,
    pub can_delete: bool,
    /// `None` = owner row missing; the SQLite subquery is NULL and does not fire.
    pub owner_is_admin: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserSiteRow {
    pub username: String,
    pub password_hash: String,
    pub status: String,
    pub created_at: i64,
    pub is_admin: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessKeySiteRow {
    pub access_key: String,
    pub secret_key: String,
    pub username: Option<String>,
    pub status: String,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub kind: String,
    pub owner_is_admin: Option<bool>,
}

struct HookCtx {
    site_id: String,
    priority: i64,
}

async fn hook_ctx(txn: &mut ObjectControlTxn) -> Result<Option<HookCtx>, ControlError> {
    if !matches!(txn.origin(), WriteOrigin::Local { .. }) {
        return Ok(None);
    }
    let Some(config) = config_row_txn(txn).await? else {
        return Ok(None);
    };
    if scan_txn(txn, TableId::Sites).await?.is_empty() {
        return Ok(None);
    }
    Ok(Some(HookCtx {
        site_id: config.id,
        priority: config.priority,
    }))
}

async fn emit(
    txn: &mut ObjectControlTxn,
    ctx: &HookCtx,
    entity: &str,
    entity_id: &str,
    op: &str,
    payload: String,
    now: i64,
) -> Result<(), ControlError> {
    let key = clock_key(entity, entity_id);
    let existing = match txn
        .get(TableId::SiteEntityClock, &key)
        .await
        .map_err(backend)?
    {
        Some(bytes) => Some(decode::<SiteEntityClockRow>(&bytes)?),
        None => None,
    };
    if let Some(clock) = &existing
        && (now, ctx.priority, ctx.site_id.as_str())
            < (clock.ts, clock.priority, clock.origin_site_id.as_str())
    {
        return Err(ControlError::Invalid("site replication conflict".into()));
    }
    let seq = txn.alloc_site_seq().map_err(backend)?;
    let row = SiteChangelogRow {
        seq,
        origin_site_id: ctx.site_id.clone(),
        entity: entity.into(),
        entity_id: entity_id.into(),
        op: op.into(),
        payload,
        priority: ctx.priority,
        ts: now,
        created_at: now,
    };
    txn.put(TableId::SiteChangelog, &changelog_key(seq), &encode(&row)?)
        .await
        .map_err(backend)?;
    let wins = existing.as_ref().is_none_or(|c| {
        (now, ctx.priority, ctx.site_id.as_str(), seq)
            > (c.ts, c.priority, c.origin_site_id.as_str(), c.seq)
    });
    if wins {
        let clock = SiteEntityClockRow {
            entity: entity.into(),
            entity_id: entity_id.into(),
            ts: now,
            priority: ctx.priority,
            origin_site_id: ctx.site_id.clone(),
            seq,
        };
        txn.put(TableId::SiteEntityClock, &key, &encode(&clock)?)
            .await
            .map_err(backend)?;
    }
    Ok(())
}

impl ObjectControlTxn {
    /// Explicit LWW clock write for `SiteApply` (today's `put_clock`).
    pub async fn site_put_clock(
        &mut self,
        entity: &str,
        entity_id: &str,
        ts: i64,
        priority: i64,
        origin_site_id: &str,
        seq: i64,
    ) -> Result<(), ControlError> {
        let key = clock_key(entity, entity_id);
        let existing = match self
            .get(TableId::SiteEntityClock, &key)
            .await
            .map_err(backend)?
        {
            Some(bytes) => Some(decode::<SiteEntityClockRow>(&bytes)?),
            None => None,
        };
        let wins = existing.as_ref().is_none_or(|c| {
            (ts, priority, origin_site_id, seq)
                > (c.ts, c.priority, c.origin_site_id.as_str(), c.seq)
        });
        if wins {
            let clock = SiteEntityClockRow {
                entity: entity.into(),
                entity_id: entity_id.into(),
                ts,
                priority,
                origin_site_id: origin_site_id.into(),
                seq,
            };
            self.put(TableId::SiteEntityClock, &key, &encode(&clock)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    pub async fn site_hook_bucket(
        &mut self,
        old: Option<&BucketSiteRow>,
        new: Option<&BucketSiteRow>,
        now: i64,
    ) -> Result<(), ControlError> {
        let (entity_id, op, payload) = match (old, new) {
            (None, Some(n)) => (n.name.clone(), "upsert", bucket_upsert(n)),
            (Some(o), None) => (o.name.clone(), "delete", bucket_delete(o)),
            (Some(o), Some(n)) => {
                if o.versioning == n.versioning
                    && o.owner_uid == n.owner_uid
                    && o.object_lock == n.object_lock
                {
                    return Ok(());
                }
                (n.name.clone(), "upsert", bucket_upsert(n))
            }
            (None, None) => return Ok(()),
        };
        self.site_hook("bucket", &entity_id, op, payload, now).await
    }

    pub async fn site_hook_policy(
        &mut self,
        old: Option<&PolicySiteRow>,
        new: Option<&PolicySiteRow>,
        now: i64,
    ) -> Result<(), ControlError> {
        let (entity_id, op, payload) = match (old, new) {
            (None, Some(n)) => (n.bucket.clone(), "upsert", policy_upsert(n)),
            (Some(o), None) => (o.bucket.clone(), "delete", policy_delete(o)),
            (Some(o), Some(n)) => {
                if o.policy_json == n.policy_json && o.updated_at == n.updated_at {
                    return Ok(());
                }
                (n.bucket.clone(), "upsert", policy_upsert(n))
            }
            (None, None) => return Ok(()),
        };
        self.site_hook("bucket_policy", &entity_id, op, payload, now)
            .await
    }

    pub async fn site_hook_permission(
        &mut self,
        old: Option<&PermissionSiteRow>,
        new: Option<&PermissionSiteRow>,
        now: i64,
    ) -> Result<(), ControlError> {
        let row = match (old, new) {
            (_, Some(n)) => n,
            (Some(o), None) => o,
            (None, None) => return Ok(()),
        };
        if row.owner_is_admin != Some(false) {
            return Ok(());
        }
        let op = if new.is_some() { "upsert" } else { "delete" };
        self.site_hook(
            "permission",
            &permission_id(row),
            op,
            permission_payload(row),
            now,
        )
        .await
    }

    pub async fn site_hook_user(
        &mut self,
        old: Option<&UserSiteRow>,
        new: Option<&UserSiteRow>,
        now: i64,
    ) -> Result<(), ControlError> {
        let (entity_id, op, payload) = match (old, new) {
            (None, Some(n)) if !n.is_admin => (n.username.clone(), "upsert", user_upsert(n)),
            (Some(o), None) if !o.is_admin => (o.username.clone(), "delete", user_delete(o)),
            (Some(o), Some(n)) if !o.is_admin && !n.is_admin => {
                if o.username == n.username
                    && o.password_hash == n.password_hash
                    && o.status == n.status
                    && o.created_at == n.created_at
                {
                    return Ok(());
                }
                (n.username.clone(), "upsert", user_upsert(n))
            }
            _ => return Ok(()),
        };
        self.site_hook("user", &entity_id, op, payload, now).await
    }

    pub async fn site_hook_access_key(
        &mut self,
        old: Option<&AccessKeySiteRow>,
        new: Option<&AccessKeySiteRow>,
        now: i64,
    ) -> Result<(), ControlError> {
        let (entity_id, op, payload) = match (old, new) {
            (None, Some(n)) if n.kind == "long-term" && n.owner_is_admin == Some(false) => {
                (n.access_key.clone(), "upsert", access_key_upsert(n))
            }
            (Some(o), None) if o.kind == "long-term" && o.owner_is_admin == Some(false) => {
                (o.access_key.clone(), "delete", access_key_delete(o))
            }
            (Some(o), Some(n))
                if o.kind == "long-term"
                    && n.kind == "long-term"
                    && n.owner_is_admin == Some(false) =>
            {
                (n.access_key.clone(), "upsert", access_key_upsert(n))
            }
            _ => return Ok(()),
        };
        self.site_hook("access_key", &entity_id, op, payload, now)
            .await
    }

    async fn site_hook(
        &mut self,
        entity: &str,
        entity_id: &str,
        op: &str,
        payload: String,
        now: i64,
    ) -> Result<(), ControlError> {
        let Some(ctx) = hook_ctx(self).await? else {
            return Ok(());
        };
        emit(self, &ctx, entity, entity_id, op, payload, now).await
    }
}

fn permission_id(row: &PermissionSiteRow) -> String {
    let username = row.username.as_deref().unwrap_or("");
    format!("{username}\u{1f}{}", row.bucket)
}

fn bucket_upsert(row: &BucketSiteRow) -> String {
    let mut out = String::from("{\"name\":");
    json_str(&mut out, &row.name);
    out.push_str(",\"owner_username\":");
    json_opt_str(&mut out, row.owner_username.as_deref());
    out.push_str(",\"creation_date\":");
    out.push_str(&row.creation_date.to_string());
    out.push_str(",\"versioning\":");
    json_str(&mut out, &row.versioning);
    out.push_str(",\"object_lock\":");
    out.push_str(if row.object_lock { "1" } else { "0" });
    out.push('}');
    out
}

fn bucket_delete(row: &BucketSiteRow) -> String {
    let mut out = String::from("{\"name\":");
    json_str(&mut out, &row.name);
    out.push('}');
    out
}

fn policy_upsert(row: &PolicySiteRow) -> String {
    let mut out = String::from("{\"bucket\":");
    json_str(&mut out, &row.bucket);
    out.push_str(",\"policy_json\":");
    json_str(&mut out, &row.policy_json);
    out.push_str(",\"updated_at\":");
    out.push_str(&row.updated_at.to_string());
    out.push('}');
    out
}

fn policy_delete(row: &PolicySiteRow) -> String {
    let mut out = String::from("{\"bucket\":");
    json_str(&mut out, &row.bucket);
    out.push('}');
    out
}

fn permission_payload(row: &PermissionSiteRow) -> String {
    let mut out = String::from("{\"username\":");
    json_opt_str(&mut out, row.username.as_deref());
    out.push_str(",\"bucket\":");
    json_str(&mut out, &row.bucket);
    out.push_str(",\"can_read\":");
    out.push_str(if row.can_read { "1" } else { "0" });
    out.push_str(",\"can_write\":");
    out.push_str(if row.can_write { "1" } else { "0" });
    out.push_str(",\"can_delete\":");
    out.push_str(if row.can_delete { "1" } else { "0" });
    out.push('}');
    out
}

fn user_upsert(row: &UserSiteRow) -> String {
    let mut out = String::from("{\"username\":");
    json_str(&mut out, &row.username);
    out.push_str(",\"password_hash\":");
    json_str(&mut out, &row.password_hash);
    out.push_str(",\"status\":");
    json_str(&mut out, &row.status);
    out.push_str(",\"created_at\":");
    out.push_str(&row.created_at.to_string());
    out.push('}');
    out
}

fn user_delete(row: &UserSiteRow) -> String {
    let mut out = String::from("{\"username\":");
    json_str(&mut out, &row.username);
    out.push('}');
    out
}

fn access_key_upsert(row: &AccessKeySiteRow) -> String {
    let mut out = String::from("{\"access_key\":");
    json_str(&mut out, &row.access_key);
    out.push_str(",\"secret_key\":");
    json_str(&mut out, &row.secret_key);
    out.push_str(",\"username\":");
    json_opt_str(&mut out, row.username.as_deref());
    out.push_str(",\"status\":");
    json_str(&mut out, &row.status);
    out.push_str(",\"expires_at\":");
    match row.expires_at {
        Some(v) => out.push_str(&v.to_string()),
        None => out.push_str("null"),
    }
    out.push_str(",\"created_at\":");
    out.push_str(&row.created_at.to_string());
    out.push('}');
    out
}

fn access_key_delete(row: &AccessKeySiteRow) -> String {
    let mut out = String::from("{\"access_key\":");
    json_str(&mut out, &row.access_key);
    out.push_str(",\"username\":");
    json_opt_str(&mut out, row.username.as_deref());
    out.push('}');
    out
}

fn json_opt_str(out: &mut String, value: Option<&str>) {
    match value {
        Some(v) => json_str(out, v),
        None => out.push_str("null"),
    }
}

/// SQLite `json_object` string escaping: named escapes for `\b \f \n \r \t`,
/// lowercase `\u00xx` for the other controls, raw UTF-8 otherwise.
fn json_str(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
