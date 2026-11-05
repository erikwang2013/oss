//! Backend-neutral hot-read seam for SigV4 auth, STS and ACL evaluation (C2/D7).
//!
//! The traits expose only point lookups and the exact predicates the SQLite
//! statements use today; callers never see a `SqlitePool`. Implementations are
//! supplied by the server (the object-root hot view) and must keep the documented
//! fail-closed semantics.

use xl_store::control_store::ControlError;

use crate::access::{Action, AuthedUser};
use crate::account_quota::AccountQuota;
use crate::policy::PolicyDoc;

/// Access-key lookup outcome with the three error classes SigV4 distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyLookup {
    Valid(KeyAuth),
    /// Active session row past `expires_at` → `ExpiredToken`.
    ExpiredSession,
    /// Missing/disabled key, invalid session parent chain, or disabled owner
    /// (user or current account) → `InvalidAccessKeyId`.
    Invalid,
}

/// Material returned for a valid key. `secret_key` is the stored value: sealed
/// (`enc:v1:`) when the vault is enabled, never decrypted by the seam.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyAuth {
    pub uid: i64,
    pub is_admin: bool,
    pub secret_key: String,
}

impl std::fmt::Debug for KeyAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyAuth")
            .field("uid", &self.uid)
            .field("is_admin", &self.is_admin)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

/// Reads needed by `SigV4Auth::get_secret_key` and the STS signing path.
#[async_trait::async_trait]
pub trait ControlAuth: Send + Sync {
    /// AK → owner + stored SK. Validity is exactly the SigV4 predicate:
    /// key active and unexpired, owner active, current account active, and a
    /// one-level session parent chain (`parent_ak` is long-term/active/unexpired
    /// with an active owner). `now` is Unix seconds.
    async fn key_auth(&self, access_key: &str, now: i64) -> Result<KeyLookup, ControlError>;

    /// STS caller: `kind = 'long-term'`, active, unexpired, active owner.
    /// Unlike [`Self::key_auth`] no account/parent check runs (STS `caller_of`).
    async fn sts_caller(
        &self,
        access_key: &str,
        now: i64,
    ) -> Result<Option<AuthedUser>, ControlError>;

    /// STS role resolution: active user id for `username`.
    async fn active_user_uid(&self, username: &str) -> Result<Option<i64>, ControlError>;
}

/// Reads needed by `Acl` and the account/quota call sites.
#[async_trait::async_trait]
pub trait ControlAcl: Send + Sync {
    /// Same predicate as [`ControlAuth::key_auth`], projected to identity.
    async fn user_of_key(
        &self,
        access_key: &str,
        now: i64,
    ) -> Result<Option<AuthedUser>, ControlError>;

    /// `buckets.owner_id == uid`.
    async fn is_owner(&self, bucket: &str, uid: i64) -> Result<bool, ControlError>;

    /// Admin or owner allow all; otherwise the `permissions` bit (no row = deny).
    async fn can(
        &self,
        user: &AuthedUser,
        action: Action,
        bucket: &str,
    ) -> Result<bool, ControlError>;

    /// Owned buckets ∪ buckets with any permission bit set.
    async fn visible(&self, uid: i64) -> Result<Vec<String>, ControlError>;

    /// `"Off"` when the bucket row is absent.
    async fn bucket_versioning(&self, bucket: &str) -> Result<String, ControlError>;
    /// `None` when the bucket row is absent.
    async fn bucket_replication_mode_row(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, ControlError>;
    /// `"async"` when the bucket row is absent.
    async fn bucket_replication_mode(&self, bucket: &str) -> Result<String, ControlError>;
    /// `Some((object_lock, default_mode, default_days))`; `None` = no bucket row.
    #[allow(clippy::type_complexity)]
    async fn bucket_lock(
        &self,
        bucket: &str,
    ) -> Result<Option<(bool, Option<String>, Option<i64>)>, ControlError>;
    /// Parsed policy; `Err` on malformed JSON (callers fail closed).
    async fn bucket_policy(&self, bucket: &str) -> Result<Option<PolicyDoc>, ControlError>;
    /// Raw notification config JSON.
    async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>, ControlError>;
    async fn bucket_default_encryption(&self, bucket: &str)
    -> Result<Option<String>, ControlError>;
    /// Session policy for `kind = 'session'`; empty/absent = no constraint.
    async fn session_policy_of(&self, access_key: &str) -> Result<Option<PolicyDoc>, ControlError>;

    /// Member row first, else the `printf('%012d', uid)` derived account.
    async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>, ControlError>;
    /// `(account, active)`: member row → account must exist and be active;
    /// no member row → derived account row status, or active when absent.
    async fn account_identity_of_user(
        &self,
        uid: i64,
    ) -> Result<(Option<String>, bool), ControlError>;
    async fn account_status(&self, id: &str) -> Result<Option<String>, ControlError>;
    /// Account bucket quota reached; disabled/missing rows fail closed.
    async fn bucket_quota_exceeded(&self, uid: i64) -> Result<bool, ControlError>;
    /// Hard limits for the bucket owner's current account (admin/ownerless = None).
    async fn account_quota_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Option<AccountQuota>, ControlError>;
    /// Replicated usage ledger `(used_bytes, object_count)`.
    async fn account_ledger(&self, account_id: &str) -> Result<Option<(i64, i64)>, ControlError>;
    /// `(id, username)` for the owner, else the first admin.
    async fn bucket_owner_display(
        &self,
        bucket: &str,
    ) -> Result<Option<(String, String)>, ControlError>;
    /// `sites` row count (sync bucket-creation gate).
    async fn site_peer_count(&self) -> Result<usize, ControlError>;
}
