//! 账户 / 账户成员 / 用量账本的后端中立行与读写 trait。
//!
//! 快照行身份键是 account id 与 username；本地数字 uid 只出现在运行期视图。
//! `account_usage` 是节点本地缓存（不进快照），账本才是复制数据。

use serde::{Deserialize, Serialize};

use super::ControlError;

/// 账户快照行；`admin` 是用户名（`None` = 无账户管理员）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountEntry {
    pub id: String,
    pub name: String,
    pub status: String,
    pub admin: Option<String>,
    pub bucket_quota: Option<i64>,
    pub soft_byte_quota: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 账户成员快照行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountMemberEntry {
    pub account_id: String,
    pub username: String,
    pub role: String,
    pub created_at: i64,
}

/// feature 9：账户用量账本快照行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountUsageLedgerEntry {
    pub account_id: String,
    pub used_bytes: i64,
    pub object_count: i64,
}

/// 运行期账户视图（console JSON 边界；含本地 uid 与计数投影）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountView {
    pub id: String,
    pub name: String,
    pub status: String,
    pub admin_uid: Option<i64>,
    pub bucket_quota: Option<i64>,
    pub soft_byte_quota: Option<i64>,
    pub hard_byte_quota: Option<i64>,
    pub object_quota: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub member_count: i64,
    pub bucket_count: i64,
}

/// 运行期账户成员视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountMemberView {
    pub uid: i64,
    pub username: String,
    pub role: String,
    pub is_admin: bool,
    pub status: String,
    pub created_at: i64,
}

/// 只读账户访问。
#[async_trait::async_trait]
pub trait AccountStore: Send + Sync {
    async fn account(&self, id: &str) -> Result<Option<AccountView>, ControlError>;
    /// `member_uid = Some(uid)` 只列该用户所属账户；`id` 升序。
    async fn accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountView>, ControlError>;
    async fn account_status(&self, id: &str) -> Result<Option<String>, ControlError>;
    /// `(status, bucket_quota)`；账户不存在 → None。
    async fn account_bucket_quota(
        &self,
        id: &str,
    ) -> Result<Option<(String, Option<i64>)>, ControlError>;
    /// `(hard_byte_quota, object_quota)`；账户不存在 → None。
    async fn account_hard_limits(
        &self,
        id: &str,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError>;
    /// 成员行优先；无成员行 → None（调用方回落 uid 派生账户）。
    async fn account_member_account(&self, uid: i64) -> Result<Option<String>, ControlError>;
    /// 成员行存在但账户停用/缺失 → false。
    async fn account_active(&self, uid: i64) -> Result<bool, ControlError>;
    async fn member_uid(
        &self,
        account_id: &str,
        username: &str,
    ) -> Result<Option<i64>, ControlError>;
    async fn member_role(
        &self,
        account_id: &str,
        uid: i64,
    ) -> Result<Option<String>, ControlError>;
    /// `uid` 升序。
    async fn account_members(
        &self,
        account_id: &str,
    ) -> Result<Vec<AccountMemberView>, ControlError>;
    /// `(uid, username)`，`uid` 升序。
    async fn legacy_users_missing_accounts(&self) -> Result<Vec<(i64, String)>, ControlError>;
    /// 账户名下桶名；`name` 升序。
    async fn account_bucket_names(&self, account_id: &str) -> Result<Vec<String>, ControlError>;
    /// `(accounts, account_members)` 行数。
    async fn account_counts(&self) -> Result<(i64, i64), ControlError>;
    async fn account_ledger_count(&self) -> Result<i64, ControlError>;
}

/// 调用方事务内的账户写；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait AccountTxn: Send {
    /// `INSERT OR IGNORE`（无 admin_uid）。
    async fn account_insert_if_absent(
        &mut self,
        id: &str,
        name: &str,
        bucket_quota: Option<i64>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    /// 派生账户 `INSERT OR IGNORE`（admin_uid = 用户 uid，无 bucket_quota）。
    async fn account_insert_derived_if_absent(
        &mut self,
        id: &str,
        name: &str,
        admin_uid: i64,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    async fn account_set_name(
        &mut self,
        id: &str,
        name: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    /// 按值写 status；停用保留原子「在册全局管理员」守卫。
    async fn account_set_status(
        &mut self,
        id: &str,
        status: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    async fn account_set_bucket_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    async fn account_set_soft_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    async fn account_set_hard_limits(
        &mut self,
        id: &str,
        hard_byte_quota: Option<i64>,
        object_quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    async fn account_delete(&mut self, id: &str) -> Result<bool, ControlError>;
    async fn account_member_count(&mut self, id: &str) -> Result<i64, ControlError>;
    async fn account_bucket_count(&mut self, id: &str) -> Result<i64, ControlError>;
    /// `ON CONFLICT(uid) DO UPDATE`（成员迁移/角色变更）。
    async fn account_member_upsert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError>;
    /// 普通 `INSERT`（新用户入账户，无冲突兜底）。
    async fn account_member_insert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError>;
    /// `INSERT OR IGNORE`；返回是否插入。
    async fn account_member_insert_if_absent(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<bool, ControlError>;
    /// `admin_uid` 为空时补账户管理员。
    async fn account_set_admin_if_unset(
        &mut self,
        account_id: &str,
        uid: i64,
    ) -> Result<(), ControlError>;
    /// 全局管理员移出成员表。
    async fn account_members_delete_admins(&mut self) -> Result<(), ControlError>;
    /// 删「管理员 uid 派生且已无成员」的账户。
    async fn accounts_delete_admin_derived(&mut self) -> Result<(), ControlError>;
    /// owner 的当前账户桶数 >= bucket_quota；停用账户 fail-closed。
    async fn bucket_quota_reached(
        &mut self,
        owner_id: Option<i64>,
    ) -> Result<bool, ControlError>;
    async fn account_ledger(
        &mut self,
        account_id: &str,
    ) -> Result<Option<(i64, i64)>, ControlError>;
    async fn account_ledger_upsert(
        &mut self,
        account_id: &str,
        used_bytes: i64,
        object_count: i64,
    ) -> Result<(), ControlError>;
    async fn account_ledger_clear(&mut self) -> Result<(), ControlError>;
}

/// 账户快照导出 / 安装（调用方事务内）。
#[async_trait::async_trait]
pub trait AccountSnapshotTxn: Send {
    /// `id` 升序；悬空/全局管理员 admin → Invalid（精确文本）。
    async fn snapshot_accounts(&mut self) -> Result<Vec<AccountEntry>, ControlError>;
    /// `account_id, username` 升序；全局管理员成员 → Invalid（精确文本）。
    async fn snapshot_account_members(&mut self) -> Result<Vec<AccountMemberEntry>, ControlError>;
    /// `account_id` 升序。
    async fn snapshot_account_usage_ledger(
        &mut self,
    ) -> Result<Vec<AccountUsageLedgerEntry>, ControlError>;
    /// 整表替换：先成员后账户清空，再账户→成员插入；`canonical` 时写 marker。
    async fn replace_accounts(
        &mut self,
        accounts: &[AccountEntry],
        members: &[AccountMemberEntry],
        canonical: bool,
    ) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct LegacyAccount {
        id: String,
        name: String,
        status: String,
        admin: Option<String>,
        bucket_quota: Option<i64>,
        soft_byte_quota: Option<i64>,
        created_at: i64,
        updated_at: i64,
    }

    #[derive(Serialize)]
    struct LegacyMember {
        account_id: String,
        username: String,
        role: String,
        created_at: i64,
    }

    #[derive(Serialize)]
    struct LegacyLedger {
        account_id: String,
        used_bytes: i64,
        object_count: i64,
    }

    #[test]
    fn snapshot_rows_keep_rmp_field_order() {
        let account = AccountEntry {
            id: "000000000001".into(),
            name: "acme".into(),
            status: "active".into(),
            admin: Some("alice".into()),
            bucket_quota: Some(2),
            soft_byte_quota: None,
            created_at: 1,
            updated_at: 2,
        };
        let member = AccountMemberEntry {
            account_id: account.id.clone(),
            username: "alice".into(),
            role: "owner".into(),
            created_at: 3,
        };
        let ledger = AccountUsageLedgerEntry {
            account_id: account.id.clone(),
            used_bytes: 4,
            object_count: 5,
        };
        assert_eq!(
            rmp_serde::to_vec_named(&account).unwrap(),
            rmp_serde::to_vec_named(&LegacyAccount {
                id: account.id.clone(),
                name: account.name.clone(),
                status: account.status.clone(),
                admin: account.admin.clone(),
                bucket_quota: account.bucket_quota,
                soft_byte_quota: account.soft_byte_quota,
                created_at: account.created_at,
                updated_at: account.updated_at,
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::to_vec_named(&member).unwrap(),
            rmp_serde::to_vec_named(&LegacyMember {
                account_id: member.account_id.clone(),
                username: member.username.clone(),
                role: member.role.clone(),
                created_at: member.created_at,
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::to_vec_named(&ledger).unwrap(),
            rmp_serde::to_vec_named(&LegacyLedger {
                account_id: ledger.account_id.clone(),
                used_bytes: ledger.used_bytes,
                object_count: ledger.object_count,
            })
            .unwrap()
        );
    }

    #[test]
    fn snapshot_rows_reject_unknown_fields() {
        #[derive(Serialize)]
        struct Extra {
            account_id: String,
            used_bytes: i64,
            object_count: i64,
            future: u8,
        }
        let bytes = rmp_serde::to_vec_named(&Extra {
            account_id: "a".into(),
            used_bytes: 1,
            object_count: 2,
            future: 0,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<AccountUsageLedgerEntry>(&bytes).is_err());
    }
}
