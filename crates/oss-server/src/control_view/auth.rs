//! SigV4 / STS lookups with the exact SQLite predicates (sqlite hot path parity).

use xl_s3::access::AuthedUser;
use xl_s3::control::{ControlAuth, KeyAuth, KeyLookup};
use xl_store::control_store::ControlError;

use super::*;

impl HotView {
    fn account_active(&self, uid: i64) -> bool {
        match self.members.get(&uid) {
            None => true,
            Some(m) => self
                .accounts
                .get(&m.account_id)
                .is_some_and(|a| a.status == "active"),
        }
    }

    fn parent_valid(&self, key: &ViewKey, now: i64) -> bool {
        let Some(parent) = key.parent_ak.as_ref() else {
            return true;
        };
        let Some(parent) = self.keys.get(parent) else {
            return false;
        };
        parent.kind == "long-term"
            && parent.status == "active"
            && parent.expires_at.is_none_or(|e| e > now)
            && self
                .users
                .get(&parent.user_id)
                .is_some_and(|u| u.status == "active")
    }

    pub(super) fn key_lookup(&self, access_key: &str, now: i64) -> KeyLookup {
        let Some(key) = self.keys.get(access_key) else {
            return KeyLookup::Invalid;
        };
        let expired = key.expires_at.is_some_and(|e| e <= now);
        if !expired
            && key.status == "active"
            && let Some(user) = self.users.get(&key.user_id)
            && user.status == "active"
            && self.account_active(key.user_id)
            && self.parent_valid(key, now)
        {
            return KeyLookup::Valid(KeyAuth {
                uid: key.user_id,
                is_admin: user.is_admin,
                secret_key: key.stored_secret.clone(),
            });
        }
        if expired && key.status == "active" && key.kind == "session" {
            KeyLookup::ExpiredSession
        } else {
            KeyLookup::Invalid
        }
    }
}

#[async_trait::async_trait]
impl ControlAuth for RequestView {
    async fn key_auth(&self, access_key: &str, now: i64) -> Result<KeyLookup, ControlError> {
        Ok(self.view.key_lookup(access_key, now))
    }

    async fn sts_caller(
        &self,
        access_key: &str,
        now: i64,
    ) -> Result<Option<AuthedUser>, ControlError> {
        let Some(key) = self.view.keys.get(access_key) else {
            return Ok(None);
        };
        if key.kind != "long-term"
            || key.status != "active"
            || key.expires_at.is_some_and(|e| e <= now)
        {
            return Ok(None);
        }
        let Some(user) = self.view.users.get(&key.user_id) else {
            return Ok(None);
        };
        if user.status != "active" {
            return Ok(None);
        }
        Ok(Some(AuthedUser {
            uid: key.user_id,
            is_admin: user.is_admin,
            ak: access_key.to_string(),
        }))
    }

    async fn active_user_uid(&self, username: &str) -> Result<Option<i64>, ControlError> {
        Ok(self
            .view
            .usernames
            .get(username)
            .and_then(|uid| self.view.users.get(uid))
            .filter(|u| u.status == "active")
            .map(|u| u.uid))
    }
}
