//! M8 S5：`InventoryStore` 的 SQL 实现（配置落 `inventory_configs`）。

use std::sync::Arc;

use s3s::dto::InventoryConfiguration;
use xl_s3::access::{Acl, Action, AuthedUser};
use xl_s3::inventory::{InventoryError, InventoryStore, validate_config};

use crate::db::Db;

pub struct SqlInventoryStore {
    db: Arc<Db>,
    acl: Acl,
    max_configs: i64,
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
        }
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
        let dest = cfg.destination.s3_bucket_destination.bucket.as_str();
        match self.db.bucket_exists(dest).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(InventoryError::InvalidArgument(format!(
                    "destination bucket 不存在：{dest}"
                )));
            }
            Err(e) => return Err(InventoryError::Internal(e.to_string())),
        }
        match self.acl.can(owner.clone(), Action::Write, dest).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(InventoryError::AccessDenied(format!(
                    "配置所有者没有目标桶 {dest} 的 PutObject 权限"
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
        let json =
            serde_json::to_string(cfg).map_err(|e| InventoryError::Internal(e.to_string()))?;
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
            serde_json::from_str(&r.config_json)
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
                serde_json::from_str(&r.config_json)
                    .map_err(|e| InventoryError::Internal(format!("config_json 非法：{e}")))
            })
            .collect()
    }

    async fn delete_config(&self, bucket: &str, id: &str) -> Result<(), InventoryError> {
        self.ensure_bucket(bucket).await?;
        self.db
            .inventory_delete_config(bucket, id)
            .await
            .map(|_| ())
            .map_err(|e| InventoryError::Internal(e.to_string()))
    }
}
