//! M8 S5：`InventoryStore` 的 SQL 实现（配置落 `inventory_configs`）。

use std::sync::Arc;

use s3s::dto::InventoryConfiguration;
use xl_s3::access::{Acl, AuthedUser};
use xl_s3::inventory::{
    InventoryError, InventoryStore, config_from_json, config_to_json, destination_bucket_name,
    validate_config,
};

use crate::db::Db;
use crate::raft::command::{ControlCommand, ControlResponse};
use crate::raft::control::ControlPlane;

pub struct SqlInventoryStore {
    db: Arc<Db>,
    acl: Acl,
    max_configs: i64,
    /// M9 S6c：仅 Raft 模式路由控制写（None/Local = M7 直写）。
    control: Option<Arc<ControlPlane>>,
}

impl SqlInventoryStore {
    pub fn new(db: Arc<Db>, acl: Acl) -> Self {
        let max_configs = std::env::var("OSS_INVENTORY_MAX_CONFIGS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        Self {
            db,
            acl,
            max_configs,
            control: None,
        }
    }

    pub fn with_control(mut self, control: Arc<ControlPlane>) -> Self {
        self.control = Some(control);
        self
    }

    fn raft_control(&self) -> Option<&Arc<ControlPlane>> {
        self.control.as_ref().filter(|c| c.is_raft())
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), InventoryError> {
        match self.db.bucket_exists(bucket).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(InventoryError::NoSuchBucket),
            Err(e) => Err(InventoryError::Internal(e.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl InventoryStore for SqlInventoryStore {
    async fn put_config(
        &self,
        bucket: &str,
        owner: &AuthedUser,
        cfg: &InventoryConfiguration,
    ) -> Result<(), InventoryError> {
        self.ensure_bucket(bucket).await?;
        validate_config(cfg)?;
        let dest = destination_bucket_name(&cfg.destination.s3_bucket_destination.bucket)
            .ok_or_else(|| {
                InventoryError::InvalidArgument(format!(
                    "destination bucket ARN 非法：{}",
                    cfg.destination.s3_bucket_destination.bucket
                ))
            })?
            .to_string();
        match self.db.bucket_exists(&dest).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(InventoryError::InvalidArgument(format!(
                    "destination bucket 不存在：{dest}"
                )));
            }
            Err(e) => return Err(InventoryError::Internal(e.to_string())),
        }
        // 3.7：记录源桶账户；配置了目标账户时必须与目标桶当前账户一致。
        // 源/目标桶账户不同 = 跨账户目标，只有管理员能创建（运行期写仍走配置所有者的
        // PutObject 检查，不绕过既有授权）。
        let source_account = self
            .db
            .bucket_account(bucket)
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        let dest_account = self
            .db
            .bucket_account(&dest)
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        if let Some(account) = &cfg.destination.s3_bucket_destination.account_id
            && account != &dest_account
        {
            return Err(InventoryError::InvalidArgument(format!(
                "destination account_id {account} 与目标桶 {dest} 的账户 {dest_account} 不一致"
            )));
        }
        if source_account != dest_account && !owner.is_admin {
            return Err(InventoryError::AccessDenied(format!(
                "跨账户目标桶 {dest}（账户 {dest_account}）仅管理员可配置"
            )));
        }
        // I4：走正典 check_op（表三档 × 桶策略，显式 Deny 优先——admin/owner 也受 Deny 约束）。
        // 配置期没有 run_key，用同形状代表键（真实 data/manifest 键在运行期复核）。
        let prefix = cfg
            .destination
            .s3_bucket_destination
            .prefix
            .as_deref()
            .unwrap_or("")
            .trim_matches('/');
        let probe = if prefix.is_empty() {
            format!("{bucket}/{}/probe/manifest.json", cfg.id)
        } else {
            format!("{prefix}/{bucket}/{}/probe/manifest.json", cfg.id)
        };
        match self
            .acl
            .check_op(owner, "", "PutObject", Some(&dest), Some(&probe), None)
            .await
        {
            Ok((true, _)) => {}
            Ok((false, detail)) => {
                let suffix = detail.map(|d| format!("（{d}）")).unwrap_or_default();
                return Err(InventoryError::AccessDenied(format!(
                    "配置所有者没有目标桶 {dest} 的 PutObject 权限{suffix}"
                )));
            }
            Err(e) => return Err(InventoryError::Internal(e.to_string())),
        }
        let existing = self
            .db
            .inventory_get_config(bucket, &cfg.id)
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        if existing.is_none() {
            let n = self
                .db
                .inventory_config_count(bucket)
                .await
                .map_err(|e| InventoryError::Internal(e.to_string()))?;
            if n >= self.max_configs {
                return Err(InventoryError::InvalidArgument(format!(
                    "每桶最多 {} 条 inventory 配置",
                    self.max_configs
                )));
            }
        }
        // ARN 形态归一为桶名存储；扩展键记录源桶账户（Raft 随 config_json 原样复制）。
        let mut stored = cfg.clone();
        stored.destination.s3_bucket_destination.bucket = dest.clone();
        let json = config_to_json(&stored, Some(&source_account))
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        if let Some(control) = self.raft_control() {
            // 属主 uid → username 在 append 前解析（apply 只做确定性映射）。
            let owner_name = if owner.is_admin {
                None
            } else {
                match self.db.get_user(owner.uid).await {
                    Ok(Some((name, _, _))) => Some(name),
                    Ok(None) => {
                        return Err(InventoryError::Internal("inventory 属主用户不存在".into()));
                    }
                    Err(e) => return Err(InventoryError::Internal(e.to_string())),
                }
            };
            return match control
                .write(ControlCommand::InventoryPut {
                    bucket: bucket.to_string(),
                    id: cfg.id.clone(),
                    owner: owner_name,
                    config_json: json,
                    updated_at: crate::db::now(),
                })
                .await
            {
                Ok(ControlResponse::Ok) => Ok(()),
                Ok(ControlResponse::Invalid) => Err(InventoryError::InvalidArgument(
                    "control write rejected".into(),
                )),
                Ok(_) => Err(InventoryError::NoSuchBucket),
                Err(e) => Err(InventoryError::Internal(e.to_string())),
            };
        }
        self.db
            .inventory_upsert_config(bucket, &cfg.id, owner.uid, &json, crate::db::now())
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))
    }

    async fn get_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfiguration>, InventoryError> {
        self.ensure_bucket(bucket).await?;
        let row = self
            .db
            .inventory_get_config(bucket, id)
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        row.map(|r| {
            config_from_json(&r.config_json)
                .map(|(cfg, _)| cfg)
                .map_err(|e| InventoryError::Internal(format!("config_json 非法：{e}")))
        })
        .transpose()
    }

    async fn list_configs(
        &self,
        bucket: &str,
    ) -> Result<Vec<InventoryConfiguration>, InventoryError> {
        self.ensure_bucket(bucket).await?;
        let rows = self
            .db
            .inventory_list_configs(bucket)
            .await
            .map_err(|e| InventoryError::Internal(e.to_string()))?;
        rows.iter()
            .map(|r| {
                config_from_json(&r.config_json)
                    .map(|(cfg, _)| cfg)
                    .map_err(|e| InventoryError::Internal(format!("config_json 非法：{e}")))
            })
            .collect()
    }

    async fn delete_config(&self, bucket: &str, id: &str) -> Result<(), InventoryError> {
        self.ensure_bucket(bucket).await?;
        if let Some(control) = self.raft_control() {
            return match control
                .write(ControlCommand::InventoryDelete {
                    bucket: bucket.to_string(),
                    id: id.to_string(),
                })
                .await
            {
                // 直写路径删除不存在的配置也返回成功（保持语义）。
                Ok(ControlResponse::Ok | ControlResponse::NotFound) => Ok(()),
                Ok(_) => Err(InventoryError::InvalidArgument(
                    "control write rejected".into(),
                )),
                Err(e) => Err(InventoryError::Internal(e.to_string())),
            };
        }
        self.db
            .inventory_delete_config(bucket, id)
            .await
            .map(|_| ())
            .map_err(|e| InventoryError::Internal(e.to_string()))
    }
}
