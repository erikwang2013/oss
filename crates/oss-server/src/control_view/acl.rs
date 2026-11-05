//! ACL, bucket-config, account/quota reads over the pinned view.

use xl_s3::access::{Action, AuthedUser, account_id_of};
use xl_s3::account_quota::{AccountQuota, Limits};
use xl_s3::control::{ControlAcl, KeyLookup};
use xl_s3::policy::PolicyDoc;
use xl_store::control_store::ControlError;

use super::*;

fn parse_policy(raw: &str, what: &str) -> Result<PolicyDoc, ControlError> {
    serde_json::from_str(raw).map_err(|e| ControlError::Invalid(format!("bad {what} json: {e}")))
}

#[async_trait::async_trait]
impl ControlAcl for RequestView {
    async fn user_of_key(
        &self,
        access_key: &str,
        now: i64,
    ) -> Result<Option<AuthedUser>, ControlError> {
        Ok(match self.view.key_lookup(access_key, now) {
            KeyLookup::Valid(key) => Some(AuthedUser {
                uid: key.uid,
                is_admin: key.is_admin,
                ak: access_key.to_string(),
            }),
            _ => None,
        })
    }

    async fn is_owner(&self, bucket: &str, uid: i64) -> Result<bool, ControlError> {
        Ok(self
            .view
            .buckets
            .get(bucket)
            .is_some_and(|b| b.owner_uid == Some(uid)))
    }

    async fn can(
        &self,
        user: &AuthedUser,
        action: Action,
        bucket: &str,
    ) -> Result<bool, ControlError> {
        if user.is_admin || self.is_owner(bucket, user.uid).await? {
            return Ok(true);
        }
        Ok(self
            .view
            .permissions
            .get(&user.uid)
            .and_then(|perms| perms.get(bucket))
            .is_some_and(|p| match action {
                Action::Read => p.can_read,
                Action::Write => p.can_write,
                Action::Delete => p.can_delete,
            }))
    }

    async fn visible(&self, uid: i64) -> Result<Vec<String>, ControlError> {
        let mut out: Vec<String> = self
            .view
            .buckets
            .values()
            .filter(|b| b.owner_uid == Some(uid))
            .map(|b| b.name.clone())
            .collect();
        if let Some(perms) = self.view.permissions.get(&uid) {
            for (bucket, row) in perms {
                if row.can_read || row.can_write || row.can_delete {
                    out.push(bucket.clone());
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn bucket_versioning(&self, bucket: &str) -> Result<String, ControlError> {
        Ok(self
            .view
            .buckets
            .get(bucket)
            .map(|b| b.versioning.clone())
            .unwrap_or_else(|| "Off".to_string()))
    }

    async fn bucket_replication_mode_row(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, ControlError> {
        Ok(self
            .view
            .buckets
            .get(bucket)
            .map(|b| b.replication_mode.clone()))
    }

    async fn bucket_replication_mode(&self, bucket: &str) -> Result<String, ControlError> {
        Ok(self
            .view
            .buckets
            .get(bucket)
            .map(|b| b.replication_mode.clone())
            .unwrap_or_else(|| "async".to_string()))
    }

    async fn bucket_lock(
        &self,
        bucket: &str,
    ) -> Result<Option<(bool, Option<String>, Option<i64>)>, ControlError> {
        Ok(self.view.buckets.get(bucket).map(|b| {
            (
                b.object_lock,
                b.object_lock_default_mode.clone(),
                b.object_lock_default_days,
            )
        }))
    }

    async fn bucket_policy(&self, bucket: &str) -> Result<Option<PolicyDoc>, ControlError> {
        match self.view.policies.get(bucket) {
            Some(row) => parse_policy(&row.policy_json, "bucket policy").map(Some),
            None => Ok(None),
        }
    }

    async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>, ControlError> {
        Ok(self.view.events.get(bucket).map(|e| e.config_json.clone()))
    }

    async fn bucket_default_encryption(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, ControlError> {
        Ok(self
            .view
            .buckets
            .get(bucket)
            .and_then(|b| b.default_encryption.clone()))
    }

    async fn session_policy_of(&self, access_key: &str) -> Result<Option<PolicyDoc>, ControlError> {
        if access_key.is_empty() {
            return Ok(None);
        }
        let Some(key) = self.view.keys.get(access_key) else {
            return Ok(None);
        };
        let Some(raw) = key.session_policy.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        parse_policy(raw, "session policy").map(Some)
    }

    async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>, ControlError> {
        Ok(self
            .view
            .members
            .get(&uid)
            .map(|m| m.account_id.clone())
            .or_else(|| account_id_of(uid)))
    }

    async fn account_identity_of_user(
        &self,
        uid: i64,
    ) -> Result<(Option<String>, bool), ControlError> {
        if let Some(member) = self.view.members.get(&uid) {
            let active = self
                .view
                .accounts
                .get(&member.account_id)
                .is_some_and(|a| a.status == "active");
            return Ok((Some(member.account_id.clone()), active));
        }
        let Some(account) = account_id_of(uid) else {
            return Ok((None, false));
        };
        match self.view.accounts.get(&account) {
            Some(row) => Ok((Some(account), row.status == "active")),
            None => Ok((Some(account), true)),
        }
    }

    async fn account_status(&self, id: &str) -> Result<Option<String>, ControlError> {
        Ok(self.view.accounts.get(id).map(|a| a.status.clone()))
    }

    async fn bucket_quota_exceeded(&self, uid: i64) -> Result<bool, ControlError> {
        let account = match self.view.members.get(&uid) {
            Some(member) if self.view.accounts.contains_key(&member.account_id) => {
                member.account_id.clone()
            }
            _ => match account_id_of(uid) {
                Some(account) => account,
                None => return Ok(false),
            },
        };
        let Some(row) = self.view.accounts.get(&account) else {
            return Ok(false);
        };
        if row.status != "active" {
            return Ok(true);
        }
        let Some(quota) = row.bucket_quota.filter(|q| *q >= 0) else {
            return Ok(false);
        };
        Ok(self.view.bucket_count(&account) >= quota)
    }

    async fn account_quota_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Option<AccountQuota>, ControlError> {
        let Some(bucket) = self.view.buckets.get(bucket) else {
            return Ok(None);
        };
        let Some(uid) = bucket.owner_uid else {
            return Ok(None);
        };
        let Some(user) = self.view.users.get(&uid) else {
            return Ok(None);
        };
        if user.is_admin {
            return Ok(None);
        }
        let account = self
            .view
            .members
            .get(&uid)
            .map(|m| m.account_id.clone())
            .or_else(|| account_id_of(uid));
        let Some(account) = account else {
            return Ok(None);
        };
        let row = self.view.accounts.get(&account);
        let limits = Limits {
            hard_bytes: row.and_then(|a| a.hard_byte_quota).filter(|q| *q >= 0),
            objects: row.and_then(|a| a.object_quota).filter(|q| *q >= 0),
        };
        if !limits.any() {
            return Ok(None);
        }
        Ok(Some(AccountQuota {
            account,
            disabled: row.map(|a| a.status.as_str()) != Some("active"),
            limits,
        }))
    }

    async fn account_ledger(&self, account_id: &str) -> Result<Option<(i64, i64)>, ControlError> {
        Ok(self
            .view
            .ledger
            .get(account_id)
            .map(|l| (l.used_bytes, l.object_count)))
    }

    async fn bucket_owner_display(
        &self,
        bucket: &str,
    ) -> Result<Option<(String, String)>, ControlError> {
        if let Some(uid) = self.view.buckets.get(bucket).and_then(|b| b.owner_uid)
            && let Some(user) = self.view.users.get(&uid)
        {
            return Ok(Some((uid.to_string(), user.username.clone())));
        }
        Ok(self.view.admin_uid.and_then(|uid| {
            self.view
                .users
                .get(&uid)
                .map(|u| (uid.to_string(), u.username.clone()))
        }))
    }

    async fn site_peer_count(&self) -> Result<usize, ControlError> {
        Ok(self.view.sites.len())
    }
}
