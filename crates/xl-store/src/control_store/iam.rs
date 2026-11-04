//! 用户 / 访问密钥 / 桶权限的后端中立行与读写 trait。
//!
//! 快照行（`UserEntry` / `AccessKeyEntry` / `PermissionEntry`）字段顺序即快照字节
//! 顺序，身份键是 username / access_key；本地数字 uid 只出现在运行期视图与
//! SQL 边界。`IamTxn` 的调用方持有事务边界，适配器绝不 begin/commit/rollback。

use serde::{Deserialize, Serialize};

use super::ControlError;

/// 复制用户快照行（只含 `is_admin = 0`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserEntry {
    pub username: String,
    pub password_hash: String,
    pub status: String,
    pub created_at: i64,
}

/// 访问密钥快照行（只含 `is_admin = 0 AND kind = 'long-term' AND parent_ak IS NULL`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessKeyEntry {
    pub access_key: String,
    pub secret_key: String,
    pub username: String,
    pub status: String,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub parent_ak: Option<String>,
    pub session_policy: Option<String>,
    pub kind: String,
}

/// 桶权限快照行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionEntry {
    pub username: String,
    pub bucket: String,
    pub can_read: bool,
    pub can_write: bool,
    pub can_delete: bool,
}

/// 运行期用户视图（console JSON 边界；`uid` 是本地自增 id）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserView {
    pub uid: i64,
    pub username: String,
    pub is_admin: bool,
    pub status: String,
    pub created_at: i64,
}

/// 用户身份查表结果（不做 status 过滤）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserRef {
    pub uid: i64,
    pub is_admin: bool,
}

/// 登录行：只有 active 用户且其所属账户未停用时返回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserLogin {
    pub uid: i64,
    pub password_hash: String,
}

/// 运行期权限行（三档；序列化字段名与 console 一致）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRow {
    pub bucket: String,
    pub read: bool,
    pub write: bool,
    pub delete: bool,
}

/// 只读 IAM 访问。
#[async_trait::async_trait]
pub trait IamStore: Send + Sync {
    async fn user(&self, username: &str) -> Result<Option<UserView>, ControlError>;
    async fn user_by_id(&self, uid: i64) -> Result<Option<UserView>, ControlError>;
    /// `uid` 升序。
    async fn users(&self) -> Result<Vec<UserView>, ControlError>;
    async fn user_exists(&self, username: &str) -> Result<bool, ControlError>;
    /// active 用户 + 所属账户未停用（成员行缺失 = 派生账户有效；账户行缺失 fail-closed）。
    async fn login_user(&self, username: &str) -> Result<Option<UserLogin>, ControlError>;
    /// 首个管理员 id（`id` 升序）；无管理员 → `None`。
    async fn admin_id(&self) -> Result<Option<i64>, ControlError>;
    /// `(在册管理员数, 管理员总数)`。
    async fn admin_counts(&self) -> Result<(i64, i64), ControlError>;
    /// `bucket` 升序；跳过全 0 行。
    async fn permissions_for_user(&self, uid: i64) -> Result<Vec<PermissionRow>, ControlError>;
    /// 长时 AK（`kind = 'long-term'`），`created_at, access_key` 升序。
    async fn access_keys_for_user(&self, uid: i64) -> Result<Vec<String>, ControlError>;
}

/// 调用方事务内的 IAM 写；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait IamTxn: Send {
    async fn user_by_username(&self, username: &str) -> Result<Option<UserRef>, ControlError>;
    async fn user_uid(&self, username: &str) -> Result<Option<i64>, ControlError>;
    async fn user_is_admin(&self, username: &str) -> Result<bool, ControlError>;
    /// 显式 status、`is_admin = 0`；返回新 uid。
    async fn user_create(&mut self, row: &UserEntry) -> Result<i64, ControlError>;
    /// 旧 `Db::create_user` 形状（不写 status，取表默认值）；返回新 uid。
    async fn user_create_default_status(
        &mut self,
        username: &str,
        password_hash: &str,
        created_at: i64,
    ) -> Result<i64, ControlError>;
    /// 删除守卫（取写锁 + 最后在册管理员判定）；false = 拒绝，零写。
    async fn user_delete_guard(&mut self, uid: i64) -> Result<bool, ControlError>;
    /// 守卫通过后的 IAM 级联：access_keys / permissions / users。
    async fn user_delete_rows(&mut self, uid: i64) -> Result<(), ControlError>;
    async fn user_set_password(
        &mut self,
        uid: i64,
        password_hash: &str,
    ) -> Result<bool, ControlError>;
    /// 启停用（停用走原子最后管理员守卫）。
    async fn user_set_status(&mut self, uid: i64, active: bool) -> Result<bool, ControlError>;
    /// 按值写 status（site 同步；无守卫）。
    async fn user_set_status_value(
        &mut self,
        uid: i64,
        status: &str,
    ) -> Result<bool, ControlError>;
    /// 全量替换（不跳过全 0 行；调用方按旧语义过滤）。
    async fn permissions_replace(
        &mut self,
        uid: i64,
        rows: &[PermissionRow],
    ) -> Result<(), ControlError>;
    async fn permission_upsert(
        &mut self,
        uid: i64,
        bucket: &str,
        read: bool,
        write: bool,
        delete: bool,
    ) -> Result<(), ControlError>;
    async fn permission_delete(&mut self, uid: i64, bucket: &str) -> Result<(), ControlError>;
    /// 删该桶全部权限（桶删除级联）。
    async fn permissions_delete_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn access_key_exists(&self, access_key: &str) -> Result<bool, ControlError>;
    /// apply 原语：status 固定 `active`，属主按 username 解析（缺失 → Invalid）。
    async fn access_key_create(&mut self, row: &AccessKeyEntry) -> Result<(), ControlError>;
    /// 旧 `Db::create_access_key` 形状（4 列，status/kind 取默认值）。
    async fn access_key_create_legacy(
        &mut self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        created_at: i64,
    ) -> Result<(), ControlError>;
    async fn access_key_delete(&mut self, access_key: &str) -> Result<bool, ControlError>;
    async fn access_key_set_status(
        &mut self,
        access_key: &str,
        status: &str,
    ) -> Result<bool, ControlError>;
    async fn access_key_owner_is_admin(
        &self,
        access_key: &str,
    ) -> Result<bool, ControlError>;
    /// site 删除守卫：只看长时键属主是否管理员。
    async fn access_key_long_term_owner_is_admin(
        &self,
        access_key: &str,
    ) -> Result<bool, ControlError>;
    /// `(user_id, kind)`；行不存在 → None。
    async fn access_key_user_and_kind(
        &self,
        access_key: &str,
    ) -> Result<Option<(i64, String)>, ControlError>;
    async fn access_key_delete_long_term(
        &mut self,
        access_key: &str,
    ) -> Result<bool, ControlError>;
    /// site upsert：同 AK 属主/类型冲突由调用方先判定。
    #[allow(clippy::too_many_arguments)]
    async fn access_key_site_upsert(
        &mut self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        status: &str,
        expires_at: Option<i64>,
        created_at: i64,
    ) -> Result<(), ControlError>;
}

/// 快照导出 / 安装（调用方事务内）。
#[async_trait::async_trait]
pub trait IamSnapshotTxn: Send {
    /// `username` 升序；只含非管理员。
    async fn snapshot_users(&mut self) -> Result<Vec<UserEntry>, ControlError>;
    /// `access_key` 升序；只含非管理员的长时根键。
    async fn snapshot_access_keys(&mut self) -> Result<Vec<AccessKeyEntry>, ControlError>;
    /// `username, bucket` 升序；只含非管理员。
    async fn snapshot_permissions(&mut self) -> Result<Vec<PermissionEntry>, ControlError>;
    async fn users_delete_non_admin(&mut self) -> Result<(), ControlError>;
    /// 插入非管理员行并返回新 uid。
    async fn user_install(&mut self, row: &UserEntry) -> Result<i64, ControlError>;
    /// 清非管理员属主的全部 AK（长时 + session）。
    async fn access_keys_delete_non_admin(&mut self) -> Result<(), ControlError>;
    async fn access_key_install(&mut self, row: &AccessKeyEntry) -> Result<(), ControlError>;
    async fn permissions_clear(&mut self) -> Result<(), ControlError>;
    async fn permission_install(&mut self, row: &PermissionEntry) -> Result<(), ControlError>;
    /// 清目标本地管理员名下残留权限行。
    async fn permissions_delete_admins(&mut self) -> Result<(), ControlError>;
    /// 属主已被替换而残留的 session/STS 键。
    async fn access_keys_delete_orphan_sessions(&mut self) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct LegacyUser {
        username: String,
        password_hash: String,
        status: String,
        created_at: i64,
    }

    #[derive(Serialize)]
    struct LegacyAccessKey {
        access_key: String,
        secret_key: String,
        username: String,
        status: String,
        expires_at: Option<i64>,
        created_at: i64,
        parent_ak: Option<String>,
        session_policy: Option<String>,
        kind: String,
    }

    #[derive(Serialize)]
    struct LegacyPermission {
        username: String,
        bucket: String,
        can_read: bool,
        can_write: bool,
        can_delete: bool,
    }

    #[test]
    fn snapshot_rows_keep_rmp_field_order() {
        let user = UserEntry {
            username: "alice".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        };
        let key = AccessKeyEntry {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 2,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        };
        let perm = PermissionEntry {
            username: "alice".into(),
            bucket: "b".into(),
            can_read: true,
            can_write: false,
            can_delete: true,
        };

        assert_eq!(
            rmp_serde::to_vec_named(&user).unwrap(),
            rmp_serde::to_vec_named(&LegacyUser {
                username: user.username.clone(),
                password_hash: user.password_hash.clone(),
                status: user.status.clone(),
                created_at: user.created_at,
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::to_vec_named(&key).unwrap(),
            rmp_serde::to_vec_named(&LegacyAccessKey {
                access_key: key.access_key.clone(),
                secret_key: key.secret_key.clone(),
                username: key.username.clone(),
                status: key.status.clone(),
                expires_at: key.expires_at,
                created_at: key.created_at,
                parent_ak: key.parent_ak.clone(),
                session_policy: key.session_policy.clone(),
                kind: key.kind.clone(),
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::to_vec_named(&perm).unwrap(),
            rmp_serde::to_vec_named(&LegacyPermission {
                username: perm.username.clone(),
                bucket: perm.bucket.clone(),
                can_read: perm.can_read,
                can_write: perm.can_write,
                can_delete: perm.can_delete,
            })
            .unwrap()
        );
    }

    #[test]
    fn snapshot_rows_reject_unknown_fields() {
        #[derive(Serialize)]
        struct Extra {
            username: String,
            password_hash: String,
            status: String,
            created_at: i64,
            future: u8,
        }
        let bytes = rmp_serde::to_vec_named(&Extra {
            username: "u".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
            future: 0,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<UserEntry>(&bytes).is_err());
        assert!(rmp_serde::from_slice::<PermissionEntry>(&bytes).is_err());
    }
}
